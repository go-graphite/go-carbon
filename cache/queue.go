package cache

import (
	"runtime"
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

// matches is checked under the shard lock before using cached summaries.
func (d *queueDeadlines) matches(s *cacheSettings) bool {
	if d == nil || d.settings != s {
		return false
	}
	if s.startupWriteout == nil {
		return d.released == nil
	}
	return d.released == s.startupWriteout.released.Load()
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
		} else if slot.item == nil && m.control[i] == 0 {
			break
		}
	}
	// Not found under the shard lock: drop every summary rather than risk a
	// stale deadline hiding this item.
	clear(m.deadlines.next)
}

// readyAtRegion visits at most 256 slots. Callers hold the shard lock and may
// release it between regions; each visit rechecks settings and startup release.
func (m *itemMap) readyAtRegion(region int, now time.Time, s *cacheSettings, yield func(*item)) time.Time {
	if m.deadlines == nil {
		m.deadlines = &queueDeadlines{next: make([]time.Time, (len(m.slots)+(1<<queueRegionBits)-1)>>queueRegionBits)}
	}
	d := m.deadlines
	if !d.matches(s) {
		clear(d.next)
		d.settings = s
		d.released = nil
		if s.startupWriteout != nil {
			d.released = s.startupWriteout.released.Load()
		}
	}

	deadline := d.next[region]
	if now.Before(deadline) {
		return deadline
	}
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
	return deadline
}

// SetWriteoutScanYield limits queue scans to one deadline region per lock visit
// and yields CPU between visits during restart ordering. Enabled by default;
// disabling restores whole-shard visits for workloads where the extra
// scheduling is undesirable. Buffer
// growth remains outside the lock in either mode. Reloads affect the next build.
func (c *Cache) SetWriteoutScanYield(on bool) {
	s := *c.settings.Load().(*cacheSettings)
	s.writeoutScanYield = on
	c.settings.Store(&s)
}

// queueScan retains the starting table across region visits. Slots never move
// within a table. After a rehash, resolve its names in the current table before
// inspecting items: Pop may already be decoding detached items without mu.
// Arrivals behind the cursor notify the next build, as with a visited shard.
// WriteoutReady and the persister's metric lock still govern actual writes.
type queueScan struct {
	slots []itemSlot
	start int
}

// appendRegion visits at most 256 slots, and grows the candidate buffer outside
// mu. A catalog-sized queue allocation/copy must never block live ingestion.
func (scan *queueScan) appendRegion(shard *Shard, now time.Time, s *cacheSettings, batch bool, strategy WriteStrategy, q queue) (queue, time.Time, bool) {
	shard.mu.Lock()
	if scan.start == 0 {
		scan.slots = shard.items.slots
	}
	var next time.Time
	current := len(scan.slots) > 0 && len(shard.items.slots) > 0 && &scan.slots[0] == &shard.items.slots[0]
	if s.writeoutScanYield && batch && current && shard.items.deadlines.matches(s) {
		// Coalesce deferred regions without visiting their table slots. Scanning
		// this small summary array needs no extra lock or scheduler turn per region.
		for scan.start < len(scan.slots) {
			at := shard.items.deadlines.next[scan.start>>queueRegionBits]
			if !now.Before(at) {
				break
			}
			if next.IsZero() || at.Before(next) {
				next = at
			}
			scan.start = min(scan.start+(1<<queueRegionBits), len(scan.slots))
		}
		if scan.start == len(scan.slots) {
			shard.mu.Unlock()
			return q, next, false
		}
	}
	end := len(scan.slots)
	if s.writeoutScanYield {
		end = min(scan.start+(1<<queueRegionBits), end)
	}
	if cap(q)-len(q) < end-scan.start {
		shard.mu.Unlock()
		q = slices.Grow(q, end-scan.start)
		shard.mu.Lock()
		current = len(scan.slots) > 0 && len(shard.items.slots) > 0 && &scan.slots[0] == &shard.items.slots[0]
	}
	add := func(p *item) {
		var key int64
		switch strategy {
		case MaximumLength:
			key = int64(p.count())
		case TimestampOrder:
			key = p.compact.firstTimestamp()
		}
		q = append(q, queueItem{p.Metric, key})
	}
	if batch && current {
		for region := scan.start >> queueRegionBits; region<<queueRegionBits < end; region++ {
			if at := shard.items.readyAtRegion(region, now, s, add); !at.IsZero() && (next.IsZero() || at.Before(next)) {
				next = at
			}
		}
	} else {
		for _, slot := range scan.slots[scan.start:end] {
			p := slot.item
			if p == nil {
				continue
			}
			if !current {
				p = shard.items.getHashed(p.hash, p.Metric)
				if p == nil {
					continue
				}
			}
			if batch {
				if at := writeoutDeadline(p, s); now.Before(at) {
					if next.IsZero() || at.Before(next) {
						next = at
					}
					continue
				}
			}
			add(p)
		}
	}
	scan.start = end
	more := end < len(scan.slots)
	shard.mu.Unlock()
	return q, next, more
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
	queue, next, _ := c.makeQueuePlan(now)
	return queue, next
}

func (c *Cache) makeQueuePlan(now time.Time) (chan string, time.Time, bool) {
	c.mu.Lock()
	writeStrategy := c.writeStrategy
	prevBuild := c.queueLastBuild
	restartOrder := c.restartOrder
	warmReady := c.restartWarm
	c.mu.Unlock()
	oldest := restartOrder != nil && restartOrder()
	if oldest {
		writeStrategy = TimestampOrder
	}
	if c.recoveryDrain.Load() {
		// A finite restored prefix has no cross-metric age requirement. Visit
		// nearby catalog keys together; per-metric order remains unchanged.
		writeStrategy = MetricNameOrder
		oldest = false
	}

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

	s := c.settings.Load().(*cacheSettings)
	batch := c.batchWrites(s)
	n := int(c.Len())
	if n == 0 {
		// Keep the previous scratch and avoid allocating from empty tombstone
		// tables. An arrival during this check wakes the next rebuild.
		return nil, time.Time{}, false
	}
	q, scratch := c.takeQueue(n, batch)
	index := int32(0)
	var next time.Time
	yieldAt := time.Now().Add(2 * time.Millisecond)
	var shardRanges [shardCount]struct{ next, end int32 }

	for i := 0; i < shardCount; i++ {
		shard := c.data[i]
		shardRanges[i].next = index

		var scan queueScan
		for {
			var at time.Time
			var more bool
			q, at, more = scan.appendRegion(shard, now, s, batch, writeStrategy, q)
			if !at.IsZero() && (next.IsZero() || at.Before(next)) {
				next = at
			}
			if !more {
				break
			}
			// Keep shard lock visits short, but bound explicit scheduler yields
			// by time rather than table size. Yielding every 256 slots can
			// strand the entire persister pool behind millions of runnable receivers.
			if oldest && time.Now().After(yieldAt) {
				runtime.Gosched()
				yieldAt = time.Now().Add(2 * time.Millisecond)
			}
		}
		index = int32(len(q))

		shardRanges[i].end = index
	}

	// A pressure wave may have made the whole cache eligible last time.
	// Once it passes, keep scratch proportional to the ready population.
	if batch && cap(q) > max(1024, 2*len(q)) {
		smaller := slices.Clone(q)
		clear(q)
		q = smaller
	}

	hasCold := false
	switch writeStrategy {
	case MaximumLength:
		sort.Sort(sort.Reverse(byOrderKey(q)))
	case TimestampOrder:
		if oldest && warmReady != nil {
			// Partition in place: do not retain another metric list or add per-item
			// flags. Never call the store hint under a cache shard lock.
			cut := 0
			for i := range q {
				if warmReady(q[i].metric) {
					q[cut], q[i] = q[i], q[cut]
					cut++
				}
			}
			hasCold = cut < len(q)
			sort.Sort(byOrderKey(q[:cut]))
			sort.Sort(byOrderKey(q[cut:]))
		} else {
			sort.Sort(byOrderKey(q))
		}
	case MetricNameOrder:
		// Shared catalog and block keys benefit from nearby metric names.
		// All shard locks are released before this optional ordering work.
		sort.Slice(q, func(i, j int) bool { return q[i].metric < q[j].metric })
	}

	l := len(q)
	if l == 0 {
		c.releaseQueue(scratch, q)
		return nil, next, false
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
		return ch, next, hasCold
	}
	for _, i := range q {
		ch <- i.metric
	}

	c.releaseQueue(scratch, q)
	return ch, next, hasCold
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
