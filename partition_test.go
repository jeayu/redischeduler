package redischeduler

import (
	"github.com/go-redis/redis/v8"
	"hash/crc32"
	"math"
	"testing"
	"time"
)

type customPartitionRules struct {
	TotalShards     int
	PartitionShards int
}

func (p *customPartitionRules) Partitioning(task *Task) (int, int) {
	h := int(crc32.ChecksumIEEE([]byte(task.Serialization())))
	sharding := StartSharding + h%p.TotalShards
	partition := int(math.Ceil(float64(sharding) / float64(p.PartitionShards)))
	return partition, sharding
}

func TestTaskPartition(t *testing.T) {
	task := NewTask("Say", "world", time.Now().UnixNano())
	partitionsSize := 2
	PartitionShards := 4
	var partitionRules PartitionRules = &customPartitionRules{
		TotalShards:     partitionsSize * PartitionShards,
		PartitionShards: PartitionShards,
	}
	partition, sharding := partitionRules.Partitioning(task)
	t.Log(partition, sharding)
}

func TestNewPartitions(t *testing.T) {
	partitionSize := 2
	partitionShards := 4
	var clients []*PartitionRedis
	for partition := StartPartition; partition <= partitionSize; partition++ {
		for sharding := StartSharding; sharding <= partitionShards; sharding++ {
			t.Log("Start init PartitionRedis, partition", partition, "sharding", sharding)
			pr := &PartitionRedis{
				client: redis.NewClient(&redis.Options{
					Addr: "localhost:6379",
					DB:   sharding,
				}),
				partitionId: partition,
				shardingId:  ShardingId(partition, partitionShards, sharding),
			}
			clients = append(clients, pr)
		}
	}
	p := NewPartitions(partitionSize, clients)
	t.Log(p)
}

func TestNewSinglePartitions(t *testing.T) {
	partition := StartPartition
	partitionSize := 2
	partitionShards := 4
	p := NewSinglePartition(partition, partitionSize, partitionShards)
	t.Log(p)
}

func TestNewPartitionRedisConfigs(t *testing.T) {
	partitionId := StartPartition
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
	partitionRedisConfigs := NewPartitionRedisConfigs(clientOptions, partitionId)
	t.Log(partitionId, "partitionRedisConfigs", partitionRedisConfigs)

	partitionId++
	clientOptions = []*redis.Options{
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
	partitionRedisConfigs = NewPartitionRedisConfigs(clientOptions, partitionId)
	t.Log(partitionId, "partitionRedisConfigs", partitionRedisConfigs)
}

func TestNewPartitionRedis(t *testing.T) {
	partitionId := StartPartition
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
	partitionRedisConfigs := NewPartitionRedisConfigs(clientOptions, partitionId)
	_ = NewPartitionRedisSlice(partitionRedisConfigs)
}
