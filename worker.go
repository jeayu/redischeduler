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
	Channel        chan WorkerTask
	sleep          time.Duration
	logger         *log.Logger
}

func (c *PartitionChannel) Run() {
	for {
		taskId, found, err := c.findTask()
		if err != nil {
			c.logger.Printf("PartitionChannel find task eror:%v\n", err)
		}
		if !found {
			time.Sleep(c.sleep)
		} else {
			c.logger.Println("PartitionChannel task has found:", taskId)
			c.Channel <- *NewWorkerTask(taskId)
		}
	}
}

func NewPartitionChannel(pr *PartitionRedis, sleep time.Duration, logger *log.Logger) *PartitionChannel {
	pc := &PartitionChannel{
		partitionRedis: pr,
		Channel:        make(chan WorkerTask),
		sleep:          sleep,
		logger:         logger,
	}
	if logger == nil {
		pc.logger = log.New(os.Stdout, fmt.Sprintf("PartitionChannel-%s ", pr.Node()), log.LstdFlags)
	}
	return pc
}

func (c *PartitionChannel) findTask() (taskId string, found bool, err error) {
	ctx := context.Background()
	partitionKey := c.partitionRedis.Node()
	err = c.partitionRedis.client.Watch(ctx, func(tx *redis.Tx) error {
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
		var val *redis.IntCmd
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			val = pipe.ZRem(ctx, partitionKey, taskId)
			pipe.Del(ctx, taskId)
			return nil
		})
		found = val.Val() == 1
		return err
	}, partitionKey)

	return taskId, found, err
}

type SinglePartitionWorker struct {
	Partition         *Partition
	PartitionChannels []*PartitionChannel
	TaskInvoker       Invoker
	Logger            *log.Logger
}

func NewSinglePartitionWorker(partitions *Partition, partitionChannels []*PartitionChannel, taskInvoker Invoker, logger *log.Logger) *SinglePartitionWorker {
	w := &SinglePartitionWorker{
		Partition:         partitions,
		PartitionChannels: partitionChannels,
		TaskInvoker:       taskInvoker,
		Logger:            logger,
	}
	if logger == nil {
		w.Logger = log.New(os.Stdout, "SinglePartitionWorker ", log.LstdFlags)
	}
	return w

}

func (s *SinglePartitionWorker) Run(f func()) {
	f()
}
