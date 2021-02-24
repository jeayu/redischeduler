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
	sharding = sharding / p.PartitionShards
	return partition, sharding
}

func TestTaskPartition(t *testing.T) {
	task := NewTask("Say", "world", time.Now().UnixNano())
	partitions := NewPartitions(2, 4, nil)
	var partitionRules PartitionRules = &customPartitionRules{
		TotalShards:     partitions.Size * partitions.PartitionShards,
		PartitionShards: partitions.PartitionShards,
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
			t.Log("start init PartitionRedis, partition", partition, "sharding", sharding)
			pr := &PartitionRedis{
				client: redis.NewClient(&redis.Options{
					Addr: "localhost:6379",
					DB:   sharding,
				}),
				partition: partition,
				sharding:  (partition-StartPartition)*partitionShards + sharding,
			}
			clients = append(clients, pr)
		}
	}
	p := NewPartitions(partitionSize, partitionShards, clients)
	t.Log(p)
}

func TestNewSinglePartitions(t *testing.T) {
	partition := StartPartition
	partitionSize := 2
	partitionShards := 4
	var clients []*PartitionRedis
	for sharding := StartSharding; sharding <= partitionShards; sharding++ {
		t.Log("start init PartitionRedis, partition", partition, "sharding", sharding)
		pr := &PartitionRedis{
			client: redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
				DB:   sharding,
			}),
			partition: partition,
			sharding:  (partition-StartPartition)*partitionShards + sharding,
		}
		clients = append(clients, pr)
	}
	p := NewSinglePartition(partition, partitionSize, partitionShards, clients)
	t.Log(p)
}

func TestNewNewPartitionRedis(t *testing.T) {
	partition := StartPartition + 1
	partitionSize := 2
	partitionShards := 4
	var clients []*PartitionRedis
	for sharding := StartSharding; sharding <= partitionShards; sharding++ {
		t.Log("start init PartitionRedis, partition", partition, "sharding", sharding)
		client := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
			DB:   sharding,
		})
		clients = append(clients, NewPartitionRedis(partition, sharding, partitionShards, client))
	}
	p := NewSinglePartition(partition, partitionSize, partitionShards, clients)
	t.Log(p)
}
