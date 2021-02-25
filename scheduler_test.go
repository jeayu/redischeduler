package redischeduler

import (
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

func TestPartitionScheduler(t *testing.T) {
	partitionSize := 2

	clientOptions := []*redis.Options{
		// partition 1
		{
			Addr: "localhost:6379",
			DB:   1,
		},
		{
			Addr: "localhost:6379",
			DB:   2,
		},
		{
			Addr: "localhost:6379",
			DB:   3,
		},
		{
			Addr: "localhost:6379",
			DB:   4,
		},
		// partition 2
		{
			Addr: "localhost:6379",
			DB:   1,
		},
		{
			Addr: "localhost:6379",
			DB:   2,
		},
		{
			Addr: "localhost:6379",
			DB:   3,
		},
		{
			Addr: "localhost:6379",
			DB:   4,
		},
	}
	partitionRedisConfigs := NewSchedulerPartitionRedisConfigs(clientOptions, partitionSize)
	clients := NewPartitionRedisSlice(partitionRedisConfigs)
	partitions := NewPartitions(partitionSize, clients)

	scheduler := NewPartitionScheduler(partitions, nil, nil)
	task := NewTask("SayHi", "world")
	duration := 3 * time.Second
	t.Log("ScheduleTask", task, "duration:", duration)
	scheduler.ScheduleTask(task, duration)

	taskCountdown := scheduler.GetTaskCountdown(task)
	t.Log("GetTaskCountdown", task, "countdown:", taskCountdown)
	if taskCountdown != duration {
		t.Fatalf("ScheduleTask fail!")
	}

	t.Log("RemoveTask", task)
	scheduler.RemoveTask(task)

	taskCountdown = scheduler.GetTaskCountdown(task)
	t.Log("GetTaskCountdown", task, "countdown:", taskCountdown)
	if taskCountdown != -2*time.Nanosecond {
		t.Fatalf("RemoveTask fail!")
	}

	scheduler.ScheduleTask(task, duration)

	task = NewTask("SayHi", "world5")
	scheduler.ScheduleTask(task, duration)
}
