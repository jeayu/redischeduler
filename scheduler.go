package redischeduler

import (
	"context"
	"github.com/go-redis/redis/v8"
	"hash/crc32"
	"log"
	"math"
	"os"
	"time"
)

type Scheduler interface {
	ScheduleTask(task *Task, duration time.Duration)
	RemoveTask(task *Task)
	GetTaskCountdown(task *Task) time.Duration
}

type PartitionSchedulerRules struct {
	TotalShards     int
	PartitionShards int
}

func (p *PartitionSchedulerRules) Partitioning(task *Task) (int, int) {
	h := int(crc32.ChecksumIEEE([]byte(task.Serialization())))
	sharding := StartSharding + (h^(h>>16))%p.TotalShards
	partition := int(math.Ceil(float64(sharding) / float64(p.PartitionShards)))
	return partition, sharding
}

type PartitionScheduler struct {
	partitions     *Partition
	partitionRules PartitionRules
	logger         *log.Logger
}

func NewPartitionScheduler(partitions *Partition, partitionRules PartitionRules, logger *log.Logger) *PartitionScheduler {
	s := &PartitionScheduler{
		partitions:     partitions,
		partitionRules: partitionRules,
		logger:         logger,
	}
	if partitionRules == nil {
		s.partitionRules = &PartitionSchedulerRules{
			TotalShards:     partitions.Size * partitions.PartitionShards,
			PartitionShards: partitions.PartitionShards,
		}
	}
	if logger == nil {
		s.logger = log.New(os.Stdout, "PartitionScheduler", log.LstdFlags)
	}
	return s

}

func (r *PartitionScheduler) ScheduleTask(task *Task, duration time.Duration) {
	currentTime := time.Now().UnixNano() / 1e6
	triggerTime := currentTime + duration.Milliseconds()

	partition, sharding := r.partitionRules.Partitioning(task)
	partitionShardingName := PartitionShardingName(partition, sharding)
	client := r.partitions.Client(partition, sharding)

	taskKey := task.Serialization()
	var ctx = context.Background()
	var result []redis.Cmder
	err := client.Watch(ctx, func(tx *redis.Tx) error {
		scheduleKey := client.Get(ctx, taskKey).Val()
		r, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZRem(ctx, scheduleKey, taskKey)
			pipe.SetEX(ctx, taskKey, partitionShardingName, duration)
			pipe.ZAdd(ctx, partitionShardingName, &redis.Z{float64(triggerTime), taskKey})
			return nil
		})
		result = r
		return err
	}, taskKey)
	if err == redis.TxFailedErr {
		r.logger.Fatalln("ScheduleTask TxFailedErr!", err)
	} else if result == nil || len(result) < 1 {
		r.logger.Fatalln("ScheduleTask fail!")
	} else {
		for _, res := range result {
			if res.Err() != nil {
				r.logger.Fatalln("ScheduleTask fail! err:", res)
			}
		}
	}

}

func (r *PartitionScheduler) RemoveTask(task *Task) {
	partition, sharding := r.partitionRules.Partitioning(task)
	client := r.partitions.Client(partition, sharding)

	taskKey := task.Serialization()

	var ctx = context.Background()
	result, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		scheduleKey := pipe.Get(ctx, taskKey).Val()
		pipe.ZRem(ctx, scheduleKey, taskKey)
		pipe.Del(ctx, taskKey)
		return nil
	})
	if err != nil {
		r.logger.Fatalln("RemoveTask err!", err)
	} else if result == nil || len(result) < 1 {
		r.logger.Fatalln("RemoveTask fail!")
	} else {
		for _, res := range result {
			if res.Err() != nil {
				r.logger.Fatalln("RemoveTask fail! err:", res)
			}
		}
	}
}

func (r *PartitionScheduler) GetTaskCountdown(task *Task) time.Duration {
	partition, sharding := r.partitionRules.Partitioning(task)
	client := r.partitions.Client(partition, sharding)
	taskKey := task.Serialization()
	return client.TTL(client.Context(), taskKey).Val()
}
