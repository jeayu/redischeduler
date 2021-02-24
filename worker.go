package redischeduler

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"time"
)

type Worker interface {
	Run(f func())
}

type PartitionChannel struct {
	partitionRedis *PartitionRedis
	channel        chan WorkerTask
	sleep          time.Duration
	logger         *log.Logger
}

func (c *PartitionChannel) Run() {
	for {
		found := c.findTask()
		c.logger.Println("PartitionChannel find task:", found)
		if !found {
			time.Sleep(c.sleep)
		}
	}
}

func NewPartitionChannel(pr *PartitionRedis, sleep time.Duration, logger *log.Logger) *PartitionChannel {
	pc := &PartitionChannel{
		partitionRedis: pr,
		channel:        make(chan WorkerTask),
		sleep:          sleep,
		logger:         logger,
	}
	if logger == nil {
		pc.logger = log.New(os.Stdout, fmt.Sprintf("PartitionChannel-%s ", pr.Node()), log.LstdFlags)
	}
	return pc
}

func (c *PartitionChannel) findTask() bool {
	found := false
	ctx := context.Background()
	partitionKey := c.partitionRedis.Node()
	var taskId string
	err := c.partitionRedis.client.Watch(ctx, func(tx *redis.Tx) error {
		values := c.partitionRedis.client.ZRangeByScore(ctx, partitionKey, &redis.ZRangeBy{
			Min:    "0",
			Max:    fmt.Sprint(time.Now().UnixNano() / 1e6),
			Offset: 0,
			Count:  1,
		}).Val()
		if values == nil || len(values) == 0 {
			tx.Unwatch(ctx, partitionKey)
			return nil
		}
		taskId = values[0]
		cmder, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZRem(ctx, partitionKey, taskId)
			pipe.Del(ctx, taskId)
			return nil
		})
		for _, c := range cmder {
			if c.Err() != nil {
				err = c.Err()
			}
		}
		found = true
		return err
	}, partitionKey)
	if err != nil {
		return false
	}

	if found {
		c.channel <- *NewWorkerTask(taskId)
	}
	return found
}

type SinglePartitionWorker struct {
	partition         *Partition
	partitionChannels []*PartitionChannel
	taskInvoker       Invoker
	logger            *log.Logger
}

func NewSinglePartitionWorker(partitions *Partition, partitionChannels []*PartitionChannel, taskInvoker Invoker, logger *log.Logger) *SinglePartitionWorker {
	w := &SinglePartitionWorker{
		partition:         partitions,
		partitionChannels: partitionChannels,
		taskInvoker:       taskInvoker,
		logger:            logger,
	}
	if logger == nil {
		w.logger = log.New(os.Stdout, "SinglePartitionWorker ", log.LstdFlags)
	}

	return w

}

func (s *SinglePartitionWorker) Run(f func()) {
	f()
}
