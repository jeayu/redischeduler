package redischeduler

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"reflect"
	"testing"
	"time"
)

func sayHi(name string) {
	fmt.Println("Hi!", name)
}

func say(v ...interface{}) {
	fmt.Println("Say:", v)
}

func TestSinglePartitionWorker(t *testing.T) {
	partition := StartPartition
	partitionSize := 2
	partitionShards := 4
	var clients []*PartitionRedis
	var partitionChannels = make([]*PartitionChannel, partitionShards)

	for sharding := StartSharding; sharding <= partitionShards; sharding++ {
		t.Log("Start init PartitionRedis, partition", partition, "sharding", sharding)
		client := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   sharding,
		})
		pr := NewPartitionRedis(partition, sharding, partitionShards, client)
		clients = append(clients, pr)
		partitionChannels[sharding-StartSharding] = NewPartitionChannel(pr, 1*time.Second, nil)
	}
	singlePartition := NewSinglePartition(partition, partitionSize, partitionShards, clients)

	taskInvoker := &TaskInvoker{
		Functions: map[string]reflect.Value{
			"SayHi": reflect.ValueOf(sayHi),
			"Say":   reflect.ValueOf(say),
		},
	}

	worker := NewSinglePartitionWorker(singlePartition, partitionChannels, taskInvoker, nil)
	worker.Run(func() {
		worker.Logger.Println("Run SinglePartitionWorker", worker.Partition.Id)
		cases := make([]reflect.SelectCase, len(worker.PartitionChannels))
		for i, partitionChannel := range worker.PartitionChannels {
			worker.Logger.Println("Run partitionChannel", i+1)
			go partitionChannel.Run()
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(partitionChannel.Channel)}

		}
		for len(cases) > 0 {
			chosen, value, ok := reflect.Select(cases)
			if !ok {
				worker.Logger.Printf("The chosen channel %v has been closed\n", cases[chosen])
				cases[chosen].Chan = reflect.ValueOf(nil)
				continue
			}
			defer func() {
				if r := recover(); r != nil {
					worker.Logger.Printf("The chosen channel %v error: %v\n", cases[chosen], r)
				}
			}()
			workerTask := value.Interface().(WorkerTask)
			err := worker.TaskInvoker.Call(workerTask)
			if err != nil {
				worker.Logger.Printf("The chosen channel %v invoker error: %v\n", cases[chosen], err)
			}
			return

		}
	})
}
