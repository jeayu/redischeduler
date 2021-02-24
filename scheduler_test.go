package redischeduler

import (
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

func TestPartitionScheduler(t *testing.T) {
	partitionSize := 2
	partitionShards := 4
	var clients []*PartitionRedis
	for partition := StartPartition; partition <= partitionSize; partition++ {
		for sharding := StartSharding; sharding <= partitionShards; sharding++ {
			t.Log("start init PartitionRedis, partition", partition, "sharding", sharding)
			client := redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
				DB:   sharding,
			})
			clients = append(clients, NewPartitionRedis(partition, sharding, partitionShards, client))
		}
	}
	partitions := NewPartitions(partitionSize, partitionShards, clients)

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
}
