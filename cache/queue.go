package cache

import (
	"slices"
	"sort"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"
)

const queueRegionBits = 8

// Each region summarizes the earliest deadline of 256 hash-table slots. A
// zero deadline needs a scan. The shard lock protects summaries and items.
type queueDeadlines struct {
	settings *cacheSettings
	released *time.Time
	next     []time.Time
}

func (m *itemMap) invalidateDeadline(slot int) {
	if m.deadlines != nil {
		m.deadlines.next[slot>>queueRegionBits] = time.Time{}
	}
}

// Invalidate a changed item at its probed slot, which may lie in the next
// region. Inserts, replacements and table rebuilds invalidate in set.
func (m *itemMap) invalidateItemDeadline(it *item) {
	if m.deadlines == nil {
		return
	}
	hash := xxh3.HashString(it.Metric)
	mask := len(m.slots) - 1
	for i, n := int(hash>>shardBits)&mask, 0; n < len(m.slots); i, n = (i+1)&mask, n+1 {
		if slot := &m.slots[i]; slot.item == it {
			m.invalidateDeadline(i)
			return
		} else if slot.item == nil && slot.hash == 0 {
			break
		}
	}
	// Not found under the shard lock: drop every summary rather than risk a
	// stale deadline hiding this item.
	clear(m.deadlines.next)
}

func (m *itemMap) readyAt(now time.Time, s *cacheSettings, yield func(*item)) time.Time {
	if m.deadlines == nil {
		m.deadlines = &queueDeadlines{next: make([]time.Time, (len(m.slots)+(1<<queueRegionBits)-1)>>queueRegionBits)}
	}
	d := m.deadlines
	var released *time.Time
	if s.startupWriteout != nil {
		released = s.startupWriteout.released.Load()
	}
	if d.settings != s || d.released != released {
		clear(d.next)
		d.settings, d.released = s, released
	}
	var next time.Time
	for region, deadline := range d.next {
		if !now.Before(deadline) {
			deadline = time.Time{}
			ready := false
			start := region << queueRegionBits
			for _, slot := range m.slots[start:min(start+(1<<queueRegionBits), len(m.slots))] {
				if slot.item == nil {
					continue
				}
				at := writeoutDeadline(slot.item, s)
				if !now.Before(at) {
					ready = true
					yield(slot.item)
				} else if deadline.IsZero() || at.Before(deadline) {
					deadline = at
				}
			}
			d.next[region] = deadline
			if ready {
				d.next[region] = time.Time{}
			}
		}
		if !deadline.IsZero() && (next.IsZero() || deadline.Before(next)) {
			next = deadline
		}
	}
	return next
}

type queueItem struct {
	metric   string
	orderKey int64
}

type queue []queueItem

type byOrderKey queue

func (v byOrderKey) Len() int           { return len(v) }
func (v byOrderKey) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v byOrderKey) Less(i, j int) bool { return v[i].orderKey < v[j].orderKey }

func (c *Cache) makeQueue() chan string {
	q, _ := c.makeQueueAt(time.Now())
	return q
}

// The next deadline lets an empty queue sleep without rescanning deferred data.
func (c *Cache) makeQueueAt(now time.Time) (chan string, time.Time) {
	c.mu.Lock()
	writeStrategy := c.writeStrategy
	prevBuild := c.queueLastBuild
	c.mu.Unlock()

	if !prevBuild.IsZero() {
		// @TODO: may be max (with atomic cas)
		atomic.StoreUint32(&c.stat.queueWriteoutTime, uint32(time.Since(prevBuild)/time.Second))
	}

	start := time.Now()

	defer func() {
		atomic.AddUint32(&c.stat.queueBuildTimeMs, uint32(time.Since(start)/time.Millisecond))
		atomic.AddUint32(&c.stat.queueBuildCnt, 1)

		c.mu.Lock()
		c.queueLastBuild = time.Now()
		c.mu.Unlock()
	}()

	orderKey := func(_ *item) int64 {
		return 0
	}

	switch writeStrategy {
	case MaximumLength:
		orderKey = func(values *item) int64 {
			return int64(values.count())
		}
	case TimestampOrder:
		orderKey = func(values *item) int64 {
			return firstTimestamp(values.compact.buf)
		}
	}

	s := c.settings.Load().(*cacheSettings)
	batch := c.batchWrites(s)
	q, scratch := c.takeQueue(int(c.Len()), batch)
	index := int32(0)
	var next time.Time
	var shardRanges [shardCount]struct{ next, end int32 }

	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shardRanges[i].next = index
		shard.mu.Lock()

		add := func(p *item) {
			q = append(q, queueItem{p.Metric, orderKey(p)})
			index++
		}
		if batch {
			if at := shard.items.readyAt(now, s, add); !at.IsZero() && (next.IsZero() || at.Before(next)) {
				next = at
			}
		} else {
			for _, p := range shard.items.all {
				add(p)
			}
		}

		shard.mu.Unlock()
		shardRanges[i].end = index
	}

	// A pressure wave may have made the whole cache eligible last time.
	// Once it passes, keep scratch proportional to the ready population.
	if batch && cap(q) > max(1024, 2*len(q)) {
		smaller := slices.Clone(q)
		clear(q)
		q = smaller
	}

	switch writeStrategy {
	case MaximumLength:
		sort.Sort(sort.Reverse(byOrderKey(q)))
	case TimestampOrder:
		sort.Sort(byOrderKey(q))
	case MetricNameOrder:
		// Shared catalog and block keys benefit from nearby metric names.
		// All shard locks are released before this optional ordering work.
		sort.Slice(q, func(i, j int) bool { return q[i].metric < q[j].metric })
	}

	l := len(q)
	if l == 0 {
		c.releaseQueue(scratch, q)
		return nil, next
	}

	ch := make(chan string, l)
	if writeStrategy == Noop {
		// Adjacent entries used to share one cache lock, concentrating the
		// entire persister pool on it. Noop has no cross-metric ordering
		// contract: alternate nonempty shards without another per-entry copy.
		var active [shardCount]int
		n := 0
		for shard, bounds := range shardRanges {
			if bounds.next < bounds.end {
				active[n] = shard
				n++
			}
		}
		for n > 0 {
			for i := 0; i < n; {
				bounds := &shardRanges[active[i]]
				ch <- q[bounds.next].metric
				bounds.next++
				if bounds.next == bounds.end {
					n--
					active[i] = active[n]
				} else {
					i++
				}
			}
		}
		c.releaseQueue(scratch, q)
		return ch, next
	}
	for _, i := range q {
		ch <- i.metric
	}

	c.releaseQueue(scratch, q)
	return ch, next
}

// takeQueue reuses the previous rebuild's buffer. Batched queues reserve only
// a small starting capacity: most metrics may be waiting for their deadline.
// Only the channel outlives a rebuild. Concurrent rebuilds take separate buffers.
func (c *Cache) takeQueue(n int, batch bool) (queue, *queue) {
	if n == 0 {
		// Leave the buffer for the next nonempty cache.
		return nil, nil
	}
	capacity := n
	if batch {
		capacity = min(n, 1024)
	}
	if scratch := c.queueScratch.Swap(nil); scratch != nil {
		c.queueScratchBytes.Store(0)
		if q := *scratch; (batch || cap(q) >= n) && cap(q) <= 2*n {
			return q[:0], scratch
		}
		return make(queue, 0, capacity), scratch
	}
	return make(queue, 0, capacity), nil
}

// releaseQueue keeps q's buffer for the next rebuild. Clearing it releases
// metric names that may since have left the cache. Entries beyond len(q) are
// zero: they were cleared by earlier releases or never written.
func (c *Cache) releaseQueue(scratch *queue, q queue) {
	if cap(q) == 0 {
		return
	}
	clear(q)
	if scratch == nil {
		scratch = new(queue)
	}
	*scratch = q[:0]
	c.queueScratch.Store(scratch)
	c.queueScratchBytes.Store(int64(cap(q)) * 24)
}
