package cache

import (
	"math"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/points"
)

func (it *item) memoryBytes() int64 {
	return int64(unsafe.Sizeof(item{})) + it.extraMemory() + int64(len(it.Metric)+cap(it.compact.buf))
}

func (it *item) extraMemory() int64 {
	if it.pending == nil {
		return 0
	}
	return int64(unsafe.Sizeof(points.Points{}))
}

func flightMemory(p *points.Points) int64 {
	// Staging may retain the popped item until its log batch is consumed.
	// Conservatively charge it along with the newly detached Points header.
	return int64(unsafe.Sizeof(item{})) + int64(unsafe.Sizeof(points.Points{})) + int64(len(p.Metric)) + int64(cap(p.Data))*int64(unsafe.Sizeof(points.Point{}))
}

// memoryBytes estimates owned allocations under the shard lock. Map overhead
// includes growth slack; allocator size-class rounding and throttle-owned data
// are not known here. Names shared by an in-flight and queued batch may overlap.
func (s *Shard) memoryBytes() int64 {
	return int64(unsafe.Sizeof(Shard{})) + s.items.bytes + s.inFlightBytes + s.writeout.memoryBytes() +
		int64(cap(s.items.slots))*int64(unsafe.Sizeof(itemSlot{})) + int64(cap(s.items.control)) +
		int64(cap(s.notConfirmed))*8 + int64(cap(s.sparseLast))*4 +
		int64(cap(s.claimData))*int64(unsafe.Sizeof(points.Point{})) +
		int64(cap(s.claimEncoded)) +
		s.log.stagingBytes() +
		int64(len(s.log.first)+len(s.log.items)+len(s.adds))*48 + int64(len(s.firstArrival))*64 + int64(len(s.recoverySources))*96
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

// syncMemoryLocked publishes shard metrics at allocation/ownership changes.
// A warm point that fits its buffer needs only the size store in addSizeLocked.
// The shared admission counter still publishes allocation changes in batches.
func (c *Cache) syncMemoryLocked(shard *Shard) {
	n := shard.memoryBytes()
	shard.memory.Store(n)
	shard.pointBytes.Store(shard.items.pointBytes)
	shard.metrics.Store(int32(shard.items.len()))
	shard.notConfirmedCount.Store(int32(shard.notConfirmedUsed))
	s := c.settings.Load().(*cacheSettings)
	if s.maxBytes <= 0 && c.recoveryAdmission.Load() == nil {
		return
	}
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
// log encoder's own buffers are outside the cache's ownership. Shard totals are
// published at allocation changes; sampling never waits for a shard lock.
func (c *Cache) EstimatedBytes() int64 {
	var n int64
	if r := c.inputReplay.Load(); r != nil {
		n = r.history.EstimatedBytes()
	}
	for _, shard := range c.data {
		n += shard.memory.Load()
	}
	return n + c.queueMemory()
}

// PointBytes sums the capacities of queued compact point buffers. Capacity
// includes spare allocated space, matching EstimatedBytes. Inline codec state
// (including the partial word), names, item headers and in-flight batches are
// excluded. It samples atomic shard totals without taking shard locks.
func (c *Cache) PointBytes() int64 {
	var n int64
	for _, shard := range c.data {
		n += shard.pointBytes.Load()
	}
	return n
}

func (c *Cache) bytePressure(s *cacheSettings) bool {
	return s.maxBytes > 0 && c.stat.bytes.Load()+c.queueMemory() > s.maxBytes/4*3
}
