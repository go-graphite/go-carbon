package cache

/*
Based on https://github.com/orcaman/concurrent-map
*/

import (
	"sync"
	"sync/atomic"

	"github.com/lomik/go-carbon/points"
)

type WriteStrategy int

const (
	MaximumLength WriteStrategy = iota
	TimestampOrder
	Noop
)

const shardCount = 1024

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type Cache struct {
	data []*Shard

	maxSize       int32
	writeStrategy WriteStrategy

	stat struct {
		size                int32  // changing via atomic
		queueBuildCnt       uint32 // number of times writeout queue was built
		queueBuildTimeMs    uint32 // time spent building writeout queue in milliseconds
		queueWriteoutTimeMs uint32 // in milliseconds
		overflowCnt         int32  // drop packages if cache full
		queryCnt            uint32 // number of queries
	}
}

// A "thread" safe string to anything map.
type Shard struct {
	sync.RWMutex // Read Write mutex, guards access to internal map.
	items        map[string]*points.Points
}

// Creates a new cache instance
func New() *Cache {
	c := &Cache{
		data: make([]*Shard, shardCount),
	}

	for i := 0; i < shardCount; i++ {
		c.data[i] = &Shard{items: make(map[string]*points.Points)}
	}
	return c
}

// hash function
// @TODO: try crc32 or something else?
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// Returns shard under given key
func (c *Cache) GetShard(key string) *Shard {
	// @TODO: remove type casts?
	return c.data[uint(fnv32(key))%uint(shardCount)]
}

func (c *Cache) Len() int32 {
	l := 0
	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shard.Lock()
		l += len(shard.items)
		shard.Unlock()
	}
	return int32(l)
}

func (c *Cache) Size() int32 {
	return atomic.LoadInt32(&c.stat.size)
}

// Sets the given value under the specified key.
func (c *Cache) Add(p *points.Points) {
	// Get map shard.
	count := len(p.Data)

	if c.maxSize > 0 && c.Size() > c.maxSize {
		atomic.AddInt32(&c.stat.overflowCnt, int32(count))
		return
	}

	shard := c.GetShard(p.Metric)

	shard.Lock()
	if values, exists := shard.items[p.Metric]; exists {
		values.Data = append(values.Data, p.Data...)
	} else {
		shard.items[p.Metric] = p
	}
	shard.Unlock()

	atomic.AddInt32(&c.stat.size, int32(count))
}

// Removes an element from the map and returns it
func (c *Cache) Pop(key string) (p *points.Points, exists bool) {
	// Try to get shard.
	shard := c.GetShard(key)
	shard.Lock()
	p, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()

	if exists {
		atomic.AddInt32(&c.stat.size, -int32(len(p.Data)))
	}

	return p, exists
}
