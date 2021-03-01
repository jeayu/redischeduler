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
	script         *redis.Script
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
		script: redis.NewScript(`local function findTask(key)
	redis.replicate_commands()
	local t = redis.call('TIME')
	t = t[1] * 1000 + t[2]/100
	local tasks = redis.call("ZRANGEBYSCORE",key, 0, t, "limit", 0, 1)
	if tasks[1] == nil then
		return nil
	else
		if redis.call("ZREM",key, tasks[1]) == 1 then
			redis.call("DEL", tasks[1])
			return tasks[1]
		end
	end
	return nil
end
return findTask(KEYS[1])`),
	}
	if logger == nil {
		pc.logger = log.New(os.Stdout, fmt.Sprintf("PartitionChannel-%s ", pr.Node()), log.LstdFlags)
	}
	return pc
}

func (c *PartitionChannel) findTask() (taskId string, found bool, err error) {
	ctx := context.Background()
	partitionKey := c.partitionRedis.Node()
	cmd := c.script.Run(ctx, c.partitionRedis.client, []string{partitionKey})
	if cmd.Val() != nil {
		found = true
		taskId = cmd.Val().(string)
	}
	if cmd.Err() != redis.Nil {
		err = cmd.Err()
	}
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
		err = errors.New("TaskInvoker undefined!")
	}
	if config.Logger == nil {
		config.Logger = log.New(os.Stdout, "SinglePartitionWorker ", log.LstdFlags)
	}

	partitionShards := len(config.PartitionRedisConfig)
	var partitionChannels = make([]*PartitionChannel, partitionShards)
	for i, c := range config.PartitionRedisConfig {
		config.Logger.Println("Start init PartitionRedis, partitionId", c.PartitionId, "shardingId", c.ShardingId)
		pr := NewPartitionRedis(config.PartitionRedisConfig[i])
		partitionChannels[i] = NewPartitionChannel(pr, 1*time.Second, nil)
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
			cases = append(cases[:chosen], cases[chosen+1:]...)
			continue
		}
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
