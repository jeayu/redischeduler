package redischeduler

import (
	"fmt"
	"github.com/go-redis/redis/v8"
)

const (
	StartPartition int = 1
	StartSharding  int = 1
)

func PartitionShardingName(partitionId, shardingId int) string {
	return fmt.Sprintf("p:%d:s:%d", partitionId, shardingId)
}

// Returns the shardingId
// Returns value range: [StartSharding, Size * PartitionShards]
// Note: The sharding parameter is greater than or equal to the constant StartSharding
func ShardingId(partitionId, partitionShards, sharding int) int {
	return (partitionId-StartPartition)*partitionShards + sharding
}

// -----------------------------------------
type PartitionRedisConfig struct {
	ClientOptions *redis.Options
	// PartitionId is greater than or equal to the constant StartPartition
	PartitionId int
	// ShardingId is greater than or equal to the constant StartSharding
	ShardingId int
}

func (p *PartitionRedisConfig) String() string {
	return fmt.Sprintf("%s,%s,%d", PartitionShardingName(p.PartitionId, p.ShardingId), p.ClientOptions.Addr, p.ClientOptions.DB)
}

// ClientOptions:    Redis configuration of sharding
// PartitionShards:  number of shards per partition
// PartitionId range:  [StartPartition, ...]
// Sharding range:   [StartSharding, ...]
func NewPartitionRedisConfig(clientOptions *redis.Options, partitionShards, partitionId, sharding int) *PartitionRedisConfig {
	return &PartitionRedisConfig{
		ClientOptions: clientOptions,
		PartitionId:   partitionId,
		ShardingId:    ShardingId(partitionId, partitionShards, sharding),
	}
}

func NewPartitionRedisConfigs(clientOptions []*redis.Options, partitionId int) []*PartitionRedisConfig {
	partitionShards := len(clientOptions)
	var configs = make([]*PartitionRedisConfig, partitionShards)
	for sharding := StartSharding; sharding <= partitionShards; sharding++ {
		i := sharding - StartSharding
		configs[i] = NewPartitionRedisConfig(clientOptions[i], partitionShards, partitionId, sharding)
	}
	return configs
}

func NewSchedulerPartitionRedisConfigs(clientOptions []*redis.Options, partitionSize int) []*PartitionRedisConfig {
	partitionShards := len(clientOptions) / partitionSize
	var configs = make([]*PartitionRedisConfig, partitionShards*partitionSize)
	for i, partition := 0, StartPartition; partition <= partitionSize; partition++ {
		for sharding := StartSharding; sharding <= partitionShards; sharding++ {
			configs[i] = NewPartitionRedisConfig(clientOptions[i], partitionShards, partition, sharding)
			i++
		}
	}
	return configs
}

// -----------------------------------------
type PartitionRedis struct {
	client      *redis.Client
	partitionId int
	shardingId  int
}

func NewPartitionRedis(config *PartitionRedisConfig) *PartitionRedis {
	pr := &PartitionRedis{
		client:      redis.NewClient(config.ClientOptions),
		partitionId: config.PartitionId,
		shardingId:  config.ShardingId,
	}
	return pr
}

func NewPartitionRedisSlice(configs []*PartitionRedisConfig) []*PartitionRedis {
	var clients = make([]*PartitionRedis, len(configs))
	for i, config := range configs {
		clients[i] = NewPartitionRedis(config)
	}
	return clients
}

func (p *PartitionRedis) Node() string {
	return PartitionShardingName(p.partitionId, p.shardingId)
}

// -----------------------------------------

type PartitionRules interface {
	// Partitioning task
	// Returns the partitionId and shardingId of the task.
	// PartitionId value range: [StartPartition, Size]
	// ShardingId value range: [StartSharding, Size * PartitionShards]
	Partitioning(task *Task) (int, int)
}

type Partition struct {
	Id int
	// Size: preallocate partition sizes
	Size int
	// PartitionShards: number of shards per partition
	PartitionShards int
	clientMap       map[string]*PartitionRedis
}

// Initialize all partitions. Configure for the scheduler,redis client required.
// clients: redis client for each sharding
func NewPartitions(size int, clients []*PartitionRedis) *Partition {
	var clientMap = make(map[string]*PartitionRedis, len(clients))
	for _, c := range clients {
		clientMap[c.Node()] = c
	}
	p := &Partition{
		Size:            size,
		PartitionShards: len(clients) / size,
		clientMap:       clientMap,
	}
	return p
}

// Initialize single partition. Worker configuration only,no redis client required.
// Id: partition id
// Size: preallocate partition sizes
// PartitionShards: number of shards per partition
func NewSinglePartition(id, size, partitionShards int) *Partition {
	p := &Partition{
		Id:              id,
		Size:            size,
		PartitionShards: partitionShards,
	}
	return p
}

func (p *Partition) Client(partition, sharding int) *redis.Client {
	if c, ok := p.clientMap[PartitionShardingName(partition, sharding)]; ok {
		return c.client
	}
	return nil
}
