package cache

import (
	"math"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func (it *item) memoryBytes() int64 {
	return int64(unsafe.Sizeof(item{})) + int64(len(it.Metric)+cap(it.compact.buf))
}

func flightMemory(p *points.Points) int64 {
	return int64(unsafe.Sizeof(item{})) + int64(len(p.Metric)) + int64(cap(p.Data))*int64(unsafe.Sizeof(points.Point{}))
}

// memoryBytes estimates owned allocations under the shard lock. Map overhead
// includes growth slack; allocator size-class rounding and throttle-owned data
// are not known here. Names shared by an in-flight and queued batch may overlap.
func (s *Shard) memoryBytes() int64 {
	return int64(unsafe.Sizeof(Shard{})) + s.items.bytes + s.inFlightBytes +
		int64(cap(s.items.slots))*int64(unsafe.Sizeof(itemSlot{})) +
		int64(cap(s.notConfirmed))*8 + int64(cap(s.sparseLast))*4 +
		int64(cap(s.claimData))*int64(unsafe.Sizeof(points.Point{})) +
		int64(cap(s.log.entries))*int64(unsafe.Sizeof(inputlog.Entry{})) +
		int64(len(s.log.first)+len(s.log.items)+len(s.adds))*48 + int64(len(s.firstArrival))*64
}

func byteSlackFor(limit int64) int64 {
	if limit <= 0 {
		return 64 << 10
	}
	return min(limit/(shardCount*64), 64<<10)
}

// SetMaxBytes sets an optional memory budget in addition to the point limit.
// It uses the same estimate as EstimatedBytes; zero disables the byte limit.
func (c *Cache) SetMaxBytes(limit uint64) {
	s := *c.settings.Load().(*cacheSettings)
	n := int64(min(limit, math.MaxInt64))
	if s.maxBytes == n {
		return
	}
	s.maxBytes = n
	s.byteSlack = byteSlackFor(s.maxBytes)
	c.settings.Store(&s)
	for _, shard := range c.data {
		shard.mu.Lock()
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
	}
	c.writeoutQueue.notifyAt(time.Now())
}

// syncMemoryLocked publishes allocation changes in batches, keeping a warm
// point that fits its buffer off the shared counter.
func (c *Cache) syncMemoryLocked(shard *Shard) {
	s := c.settings.Load().(*cacheSettings)
	if s.maxBytes <= 0 {
		return
	}
	n := shard.memoryBytes()
	shard.byteDelta += n - shard.byteSize
	shard.byteSize = n
	if shard.byteDelta < -s.byteSlack || shard.byteDelta > s.byteSlack {
		delta := shard.byteDelta
		after := c.stat.bytes.Add(delta)
		shard.byteDelta = 0
		if s.maxBytes > 0 && after > s.maxBytes/4*3 && after-delta <= s.maxBytes/4*3 {
			c.writeoutQueue.notifyAt(time.Now())
		}
	}
}

func (c *Cache) queueMemory() int64 {
	return c.queueChannelBytes.Load() + c.queueScratchBytes.Load()
}

// EstimatedBytes accounts for queued names, items, buffer capacities and indexes,
// in-flight batches, input-log tracking/buffers, sparse tables and queue scratch.
// It visits shards, not metrics. Pending recovery's mapped files and the input
// log encoder's own buffers are outside the cache's ownership.
func (c *Cache) EstimatedBytes() int64 {
	var n int64
	for _, shard := range c.data {
		shard.mu.RLock()
		n += shard.memoryBytes()
		shard.mu.RUnlock()
	}
	return n + c.queueMemory()
}

func (c *Cache) bytePressure(s *cacheSettings) bool {
	return s.maxBytes > 0 && c.stat.bytes.Load()+c.queueMemory() > s.maxBytes/4*3
}
