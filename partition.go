package redischeduler

import (
	"fmt"
	"github.com/go-redis/redis/v8"
)

const (
	StartPartition int = 1
	StartSharding  int = 1
)

func PartitionShardingName(partition, sharding int) string {
	return fmt.Sprintf("p:%d:s:%d", partition, sharding)
}

type PartitionRedis struct {
	client    *redis.Client
	partition int
	sharding  int
}

func NewPartitionRedis(partition, sharding, partitionShards int, client *redis.Client) *PartitionRedis {
	pr := &PartitionRedis{
		client:    client,
		partition: partition,
		sharding:  (partition-StartPartition)*partitionShards + sharding,
	}
	return pr
}

func (p *PartitionRedis) Node() string {
	return PartitionShardingName(p.partition, p.sharding)
}

type PartitionRules interface {
	// Partitioning task
	// Returns the partitioning and sharding of the task
	// Partition value range: [StartPartition, Size]
	// Sharding value range: [StartSharding, Size * PartitionShards]
	Partitioning(task *Task) (int, int)
}

type Partition struct {
	Id              int
	Size            int
	PartitionShards int
	clientMap       map[string]*PartitionRedis
}

// Initialize all partitions
// Size: preallocate partition sizes
// PartitionShards: number of shards per partition
// clients: redis client for each sharding
func NewPartitions(size, partitionShards int, clients []*PartitionRedis) *Partition {
	var clientMap = make(map[string]*PartitionRedis, len(clients))
	for _, c := range clients {
		clientMap[c.Node()] = c
	}
	p := &Partition{
		Size:            size,
		PartitionShards: partitionShards,
		clientMap:       clientMap,
	}
	return p
}

// Initialize single partition
// Id: partition id
// Size: preallocate partition sizes
// PartitionShards: number of shards per partition
// clients: redis client for each sharding
func NewSinglePartition(id, size, partitionShards int, clients []*PartitionRedis) *Partition {
	var clientMap = make(map[string]*PartitionRedis, len(clients))
	for _, c := range clients {
		clientMap[c.Node()] = c
	}
	p := &Partition{
		Id:              id,
		Size:            size,
		PartitionShards: partitionShards,
		clientMap:       clientMap,
	}
	return p
}

func (p *Partition) Client(partition, sharding int) *redis.Client {
	if c, ok := p.clientMap[PartitionShardingName(partition, sharding)]; ok {
		return c.client
	}
	return nil
}
