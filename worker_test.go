package redischeduler

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"reflect"
	"sync"
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
	time.Sleep(1500 * time.Millisecond)
	close(channel)
}

func TestMultiWorkerRun(t *testing.T) {
	partitionSize := 2
	var logger *log.Logger

	logger = log.New(os.Stdout, "TestMultiWorkerRun1 ", log.LstdFlags)
	worker := newWorker(StartPartition+1, partitionSize, logger, t)
	logger = log.New(os.Stdout, "TestMultiWorkerRun2 ", log.LstdFlags)
	worker2 := newWorker(StartPartition+1, partitionSize, logger, t)

	var wg sync.WaitGroup
	delta := 1000
	wg.Add(delta)
	runFunc := func(w *SinglePartitionWorker) {
		for _, c := range w.PartitionChannels {
			go closePartitionChannel(c.Channel)
		}
		w.Run()
	}

	go worker.RunFunc(func() {
		runFunc(worker)
	})
	worker2.RunFunc(func() {
		runFunc(worker2)
	})

}

func TestFindTaskError(t *testing.T) {
	partitionSize := 2
	var logger *log.Logger
	worker := newWorker(StartPartition, partitionSize, logger, t)
	for _, c := range worker.PartitionChannels {
		t.Log("Close redis", c.partitionRedis)
		_ = c.partitionRedis.client.Close()
	}
	testRunWorker(worker)
}
