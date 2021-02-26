package redischeduler

import (
	"context"
	"github.com/go-redis/redis/v8"
	"hash/crc32"
	"log"
	"math"
	"time"
)

type Scheduler interface {
	ScheduleTask(task *Task, duration time.Duration) (err error)
	RemoveTask(task *Task) (err error)
	GetTaskCountdown(task *Task) (duration time.Duration, err error)
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
	return s

}

func (r *PartitionScheduler) ScheduleTask(task *Task, duration time.Duration) (err error) {
	currentTime := time.Now().UnixNano() / 1e6
	triggerTime := currentTime + duration.Milliseconds()

	partition, sharding := r.partitionRules.Partitioning(task)
	partitionShardingName := PartitionShardingName(partition, sharding)
	client := r.partitions.Client(partition, sharding)

	taskKey := task.Serialization()
	ctx := context.Background()
	if r.logger != nil {
		r.logger.Println("ScheduleTask", task, "at:", partitionShardingName)
	}
	err = client.Watch(ctx, func(tx *redis.Tx) error {
		scheduleKey := client.Get(ctx, taskKey).Val()
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZRem(ctx, scheduleKey, taskKey)
			pipe.SetEX(ctx, taskKey, partitionShardingName, duration)
			pipe.ZAdd(ctx, partitionShardingName, &redis.Z{float64(triggerTime), taskKey})
			return nil
		})
		return err
	}, taskKey)
	return err
}

func (r *PartitionScheduler) RemoveTask(task *Task) (err error) {
	partition, sharding := r.partitionRules.Partitioning(task)
	client := r.partitions.Client(partition, sharding)

	taskKey := task.Serialization()

	ctx := context.Background()
	_, err = client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		scheduleKey := pipe.Get(ctx, taskKey).Val()
		pipe.ZRem(ctx, scheduleKey, taskKey)
		pipe.Del(ctx, taskKey)
		return nil
	})
	return err
}

func (r *PartitionScheduler) GetTaskCountdown(task *Task) (duration time.Duration, err error) {
	partition, sharding := r.partitionRules.Partitioning(task)
	client := r.partitions.Client(partition, sharding)
	taskKey := task.Serialization()
	ttl := client.TTL(client.Context(), taskKey)
	return ttl.Val(), ttl.Err()
}
