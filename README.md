# Redischeduler

Redischeduler is a distributed job scheduler written in Go.

## Installation
```sh
go get github.com/jeayu/redischeduler
```
## Getting Started

### Creating scheduler
- To create partitions, you need to configure the total partition size and the sharding for each partition
```go
// the total partition size.
partitionSize := 2
// the sharding for each partition.
clientOptions := []*redis.Options{
    // partition 1
    {
        Addr: "localhost:6379",
        DB:   1,
    },
    // partition 2
    {
        Addr: "localhost:6379",
        DB:   1,
    },
}
partitionRedisConfigs := NewSchedulerPartitionRedisConfigs(clientOptions, partitionSize)
clients := NewPartitionRedisSlice(partitionRedisConfigs)
// creating partitions
partitions := NewPartitions(partitionSize, clients)
```

- To create scheduler
```go
scheduler := NewPartitionScheduler(partitions, nil, nil)
```

- Schedule task
```go
task := NewTask("SayHi", "world")
duration := 3 * time.Second
scheduler.ScheduleTask(task, duration)
```

### Creating worker

- To create a task worker, you need to define a task invoker and and create the partition in which the worker
```go
partitionId := 1
partitionSize := 2
clientOptions := []*redis.Options{
     // partition 1
    {
        Addr: "localhost:6379",
        DB:   1,
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
```

- Running worker
```go
worker.Run()
```