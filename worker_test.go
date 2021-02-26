package redischeduler

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
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

func TestNewSinglePartitionWorkerError(t *testing.T) {
	_, err := NewSinglePartitionWorker(&SinglePartitionWorkerConfig{})
	if err != nil {
		t.Log("NewWorker error:", err)
	}
}
func TestSinglePartitionWorker(t *testing.T) {
	partitionSize := 2
	var logger *log.Logger
	worker := newWorker(StartPartition, partitionSize, logger, t)
	testRunWorker(worker)

	logger = log.New(os.Stdout, "SinglePartitionWorker2 ", log.LstdFlags)
	worker = newWorker(StartPartition+1, partitionSize, logger, t)

	testMultiWorkerRun(worker)

}

func newWorker(partitionId, partitionSize int, logger *log.Logger, t *testing.T) *SinglePartitionWorker {
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
		Logger: logger,
	})
	if err != nil {
		t.Fatal("NewWorker error:", err)
		return nil
	}
	return worker

}

func testRunWorker(worker *SinglePartitionWorker) {
	for _, c := range worker.PartitionChannels {
		go closePartitionChannel(c.Channel)
	}
	worker.RunFunc(func() {
		worker.Run()
	})
}

func closePartitionChannel(channel chan<- WorkerTask) {
	time.Sleep(2 * time.Second)
	close(channel)
}

func testMultiWorkerRun(worker *SinglePartitionWorker) {
	for _, c := range worker.PartitionChannels {
		go closePartitionChannel(c.Channel)
		// mock redis: transaction failed
		go c.Run()
	}
	worker.RunFunc(func() {
		worker.Run()
	})
}
