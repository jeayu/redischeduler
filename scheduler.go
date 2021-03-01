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
	partitions         *Partition
	partitionRules     PartitionRules
	scheduleTaskScript *redis.Script
	removeTaskScript   *redis.Script
	logger             *log.Logger
}

func NewPartitionScheduler(partitions *Partition, partitionRules PartitionRules, logger *log.Logger) *PartitionScheduler {
	s := &PartitionScheduler{
		partitions:     partitions,
		partitionRules: partitionRules,
		scheduleTaskScript: redis.NewScript(`local function scheduleTask(taskKey, partitionKey, triggerTime, duration)
	redis.replicate_commands()
	local oldPartitionKey = redis.call("GET", taskKey)
	if oldPartitionKey then 
		redis.call("ZREM",oldPartitionKey, taskKey)
	end
	redis.call("SETEX", taskKey, duration, partitionKey)
	return redis.call("ZADD", partitionKey, triggerTime, taskKey)
end
return scheduleTask(KEYS[1],KEYS[2],ARGV[1],ARGV[2])`),
		removeTaskScript: redis.NewScript(`local function removeTask(taskKey)
	redis.replicate_commands()
	local partitionKey = redis.call("GET", taskKey)
	if redis.call("ZREM",partitionKey, taskKey) == 1 then
		return redis.call("DEL", taskKey)
	end
end
return removeTask(KEYS[1])`),
		logger: logger,
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
	cmd := r.scheduleTaskScript.Run(ctx, client, []string{taskKey, partitionShardingName}, []interface{}{triggerTime, duration.Seconds()})
	if cmd.Err() != redis.Nil {
		err = cmd.Err()
	}
	return err
}

func (r *PartitionScheduler) RemoveTask(task *Task) (err error) {
	partition, sharding := r.partitionRules.Partitioning(task)
	client := r.partitions.Client(partition, sharding)

	taskKey := task.Serialization()

	ctx := context.Background()
	cmd := r.removeTaskScript.Run(ctx, client, []string{taskKey})
	if cmd.Err() != redis.Nil {
		err = cmd.Err()
	}
	return err
}

func (r *PartitionScheduler) GetTaskCountdown(task *Task) (duration time.Duration, err error) {
	partition, sharding := r.partitionRules.Partitioning(task)
	client := r.partitions.Client(partition, sharding)
	taskKey := task.Serialization()
	ttl := client.TTL(client.Context(), taskKey)
	return ttl.Val(), ttl.Err()
}
