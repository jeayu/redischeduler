package redischeduler

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"testing"
	"time"
)

func partitionScheduler() *PartitionScheduler {
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

	return NewPartitionScheduler(partitions, nil, nil)
}

func TestPartitionSchedulerTask(t *testing.T) {
	scheduler := partitionScheduler()
	scheduler.logger = log.New(os.Stdout, "TestPartitionSchedulerTask ", log.LstdFlags)

	task := NewTask("SayHi", "world")
	duration := 1 * time.Second
	t.Log("ScheduleTask", task, "duration:", duration)
	err := scheduler.ScheduleTask(task, duration)
	if err != nil {
		t.Fatal("ScheduleTask error!", err)
	}

	taskCountdown, err := scheduler.GetTaskCountdown(task)
	t.Log("GetTaskCountdown", task, "countdown:", taskCountdown)
	if err != nil {
		t.Fatal("GetTaskCountdown error!", err)
	} else if taskCountdown != duration {
		t.Fatal("ScheduleTask fail!")
	}

	t.Log("RemoveTask", task)
	err = scheduler.RemoveTask(task)
	if err != nil {
		t.Fatal("RemoveTask error!", err)
	}

	taskCountdown, err = scheduler.GetTaskCountdown(task)
	t.Log("GetTaskCountdown", task, "countdown:", taskCountdown)
	if err != nil {
		t.Fatal("ScheduleTask error!", err)
	} else if taskCountdown != -2*time.Nanosecond {
		t.Fatal("RemoveTask fail!")
	}

	// testing worker task

	_ = scheduler.ScheduleTask(task, duration)

	task = NewTask("SayHi", "world5")
	_ = scheduler.ScheduleTask(task, duration)

	task = NewTask("SayHi", "world7")
	_ = scheduler.ScheduleTask(task, duration)

	task = NewTask("aaa", "")
	_ = scheduler.ScheduleTask(task, duration)

}

func scheduleTask(scheduler *PartitionScheduler, t *testing.T, channel chan<- *Task) {
	for {
		task := NewTask("error", "1")
		channel <- task
		err := scheduler.ScheduleTask(task, 1*time.Hour)
		if err == redis.TxFailedErr {
			t.Log("ScheduleTask TxFailedErr!", err)
			close(channel)
			return
		}
	}
}

func TestTaskTxFailedErr(t *testing.T) {
	scheduler := partitionScheduler()
	channel := make(chan *Task)
	go scheduleTask(scheduler, t, channel)
	for {
		select {
		case task, ok := <-channel:
			if !ok {
				return
			}
			partitionId, shardingId := scheduler.partitionRules.Partitioning(task)
			client := scheduler.partitions.Client(partitionId, shardingId)
			client.Del(context.Background(), task.Serialization())
		}
	}
}
