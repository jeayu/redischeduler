package redischeduler

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"reflect"
	"testing"
)

func sayHi(name string) {
	fmt.Println("Hi!", name)
}

func say(v ...interface{}) {
	fmt.Println("Say:", v)
}

func TestSinglePartitionWorker(t *testing.T) {
	partitionSize := 2
	testWorker(StartPartition, partitionSize, t)
	testWorker(StartPartition+1, partitionSize, t)

}

func testWorker(partitionId, partitionSize int, t *testing.T) {
	clientOptions := []*redis.Options{
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
	worker, err := NewSinglePartitionWorker(&SinglePartitionWorkerConfig{
		PartitionId:          partitionId,
		PartitionSize:        partitionSize,
		PartitionRedisConfig: NewPartitionRedisConfigs(clientOptions, partitionId),
		TaskInvoker: &TaskInvoker{
			Functions: map[string]reflect.Value{
				"SayHi": reflect.ValueOf(sayHi),
				"Say":   reflect.ValueOf(say),
			},
		},
	})
	if err != nil {
		t.Fatal("NewWorker error:", err)
		return
	}
	worker.RunFunc(func() {
		worker.Logger.Println("Run SinglePartitionWorker! Partition id:", worker.Partition.Id)
		cases := make([]reflect.SelectCase, len(worker.PartitionChannels))
		for i, partitionChannel := range worker.PartitionChannels {
			worker.Logger.Println("Run partitionChannel", partitionChannel.partitionRedis.Node())
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
