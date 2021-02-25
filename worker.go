package redischeduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"reflect"
	"time"
)

type Worker interface {
	Run()
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

type SinglePartitionWorkerConfig struct {
	PartitionId          int
	PartitionSize        int
	PartitionRedisConfig []*PartitionRedisConfig
	TaskInvoker          Invoker
	Logger               *log.Logger
}

func NewSinglePartitionWorker(config *SinglePartitionWorkerConfig) (w *SinglePartitionWorker, err error) {
	if config.TaskInvoker == nil {
		err = errors.New("TaskInvoker is nil!")
	}
	if config.Logger == nil {
		config.Logger = log.New(os.Stdout, "SinglePartitionWorker ", log.LstdFlags)
	}

	partitionShards := len(config.PartitionRedisConfig)
	var partitionChannels = make([]*PartitionChannel, partitionShards)
	for i, c := range config.PartitionRedisConfig {
		config.Logger.Println("Start init PartitionRedis, partitionId", c.PartitionId, "shardingId", c.ShardingId)
		pr := NewPartitionRedis(config.PartitionRedisConfig[i])
		partitionChannels[i] = NewPartitionChannel(pr, 1*time.Second, config.Logger)
	}

	singlePartition := NewSinglePartition(config.PartitionId, config.PartitionSize, partitionShards)

	w = &SinglePartitionWorker{
		Partition:         singlePartition,
		PartitionChannels: partitionChannels,
		TaskInvoker:       config.TaskInvoker,
		Logger:            config.Logger,
	}
	return w, err

}

func (w *SinglePartitionWorker) Run() {
	w.Logger.Println("Run SinglePartitionWorker! Partition id:", w.Partition.Id)
	cases := make([]reflect.SelectCase, len(w.PartitionChannels))
	for i, partitionChannel := range w.PartitionChannels {
		w.Logger.Println("Run partitionChannel", partitionChannel.partitionRedis.Node())
		go partitionChannel.Run()
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(partitionChannel.Channel)}

	}
	for len(cases) > 0 {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			w.Logger.Printf("The chosen channel %v has been closed\n", cases[chosen])
			cases[chosen].Chan = reflect.ValueOf(nil)
			continue
		}
		defer func() {
			if r := recover(); r != nil {
				w.Logger.Printf("The chosen channel %v error: %v\n", cases[chosen], r)
			}
		}()
		workerTask := value.Interface().(WorkerTask)
		err := w.TaskInvoker.Call(workerTask)
		if err != nil {
			w.Logger.Printf("The chosen channel %v invoker error: %v\n", cases[chosen], err)
		}
	}
}

func (s *SinglePartitionWorker) RunFunc(f func()) {
	f()
}
