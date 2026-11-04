package cache

import (
	"math/bits"
	"time"
	"unsafe"
)

// timeCounts indexes seconds in 256-second pages under a compressed radix tree.
// Prefix counts and minima visit at most 56 nodes and 256 counters, independent
// of metric count or residence time. Empty pages recycle their nodes and storage.
type timeTree struct {
	root            *timeCount
	free            *timeCount
	capacity, pages int
}

type timeCount struct {
	key   uint64
	n     int64
	child [2]*timeCount
	bit   uint8 // branching bit plus one; zero identifies a leaf
	page  *[256]int64
}

func timeKey(second int64) uint64 { return uint64(second) ^ (1 << 63) }

func (t *timeTree) add(second, delta int64) {
	key := timeKey(second) >> 8
	offset := timeKey(second) & 255
	link := &t.root
	var parent *timeCount
	var parentLink **timeCount
	var side uint64
	for {
		n := *link
		if n == nil {
			*link = t.leaf(key, offset, delta)
			return
		}
		if bit := uint8(bits.Len64(key ^ n.key)); bit > n.bit {
			branch := t.node(key, n.n+delta, bit)
			child := (key >> (bit - 1)) & 1
			branch.child[child] = t.leaf(key, offset, delta)
			branch.child[1-child] = n
			*link = branch
			return
		}
		n.n += delta
		if n.bit == 0 {
			n.page[offset] += delta
			if n.n == 0 {
				if parent == nil {
					*link = nil
				} else {
					*parentLink = parent.child[1-side]
					t.release(parent)
				}
				t.release(n)
			}
			return
		}
		parent, parentLink = n, link
		side = (key >> (n.bit - 1)) & 1
		link = &n.child[side]
	}
}

func (t *timeTree) node(key uint64, count int64, bit uint8) *timeCount {
	n := t.free
	if n == nil {
		n = new(timeCount)
		t.capacity++
	} else {
		t.free = n.child[0]
	}
	*n = timeCount{key: key, n: count, bit: bit, page: n.page}
	return n
}

func (t *timeTree) release(n *timeCount) {
	*n = timeCount{child: [2]*timeCount{t.free, nil}, page: n.page}
	t.free = n
}

func (t *timeTree) leaf(key, offset uint64, count int64) *timeCount {
	n := t.node(key, count, 0)
	if n.page == nil {
		n.page = new([256]int64)
		t.pages++
	}
	n.page[offset] = count
	return n
}

func (t *timeTree) through(second int64) int64 {
	key := timeKey(second) >> 8
	var count int64
	for n := t.root; n != nil; {
		if n.bit == 0 {
			if n.key < key {
				count += n.n
			} else if n.key == key {
				for _, v := range n.page[:(timeKey(second)&255)+1] {
					count += v
				}
			}
			break
		}
		if bits.Len64(key^n.key) > int(n.bit) {
			if n.key < key {
				count += n.n
			}
			break
		}
		if key>>(n.bit-1)&1 != 0 {
			count += n.child[0].n
			n = n.child[1]
		} else {
			n = n.child[0]
		}
	}
	return count
}

func (t *timeTree) oldest() (int64, bool) {
	n := t.root
	if n == nil {
		return 0, false
	}
	for n.bit != 0 {
		n = n.child[0]
	}
	for i, count := range n.page {
		if count > 0 {
			return int64((n.key<<8 | uint64(i)) ^ (1 << 63)), true
		}
	}
	panic("empty time-count page")
}

// Eight directly addressed pages cover the ordinary residence window without
// touching tree branches on insert/pop. Colliding pages use the radix fallback;
// overdue work and arbitrarily large configured delays retain the same bounds.
type timeCounts struct {
	pages [8]*timePage
	tree  timeTree
}

type timePage struct {
	key    uint64
	n      int64
	counts [256]int64
}

func (t *timeCounts) add(second, delta int64) {
	key, offset := timeKey(second)>>8, timeKey(second)&255
	p := t.pages[key&7]
	if delta > 0 && (p == nil || p.n == 0) {
		if p == nil {
			p = new(timePage)
			t.pages[key&7] = p
		}
		p.key = key
	}
	if p != nil && p.key == key && (delta > 0 || p.counts[offset] > 0) {
		change := delta
		if delta < 0 {
			change = max(delta, -p.counts[offset])
		}
		p.counts[offset] += change
		p.n += change
		delta -= change
	}
	if delta != 0 {
		t.tree.add(second, delta)
	}
}

func (t *timeCounts) through(second int64) int64 {
	key, offset := timeKey(second)>>8, timeKey(second)&255
	n := t.tree.through(second)
	for _, p := range t.pages {
		if p == nil {
			continue
		}
		if p.key < key {
			n += p.n
		} else if p.key == key {
			for _, v := range p.counts[:offset+1] {
				n += v
			}
		}
	}
	return n
}

func (t *timeCounts) oldest() (int64, bool) {
	first, ok := t.tree.oldest()
	var oldest *timePage
	for _, p := range t.pages {
		if p != nil && p.n > 0 && (oldest == nil || p.key < oldest.key) {
			oldest = p
		}
	}
	if oldest != nil {
		for i, n := range oldest.counts {
			if n > 0 {
				at := int64((oldest.key<<8 | uint64(i)) ^ (1 << 63))
				if !ok || at < first {
					return at, true
				}
				break
			}
		}
	}
	return first, ok
}

func (t *timeCounts) memoryBytes() int64 {
	n := int64(t.tree.capacity)*int64(unsafe.Sizeof(timeCount{})) + int64(t.tree.pages)*256*8
	for _, p := range t.pages {
		if p != nil {
			n += int64(unsafe.Sizeof(timePage{}))
		}
	}
	return n
}

// writeoutSummary uses the batching clock, not wall time: releasing a paused
// startup only changes the query clock and never reschedules cached items.
// All fields are guarded by the owning shard. No per-item metadata is needed.
type writeoutIndex struct {
	epoch               time.Time
	settings            *cacheSettings
	deadlines, arrivals timeCounts
	immediate           int64
}

func (w *writeoutIndex) memoryBytes() int64 {
	return w.deadlines.memoryBytes() + w.arrivals.memoryBytes()
}

func sameWriteoutSettings(a, b *cacheSettings) bool {
	return a.writeoutMinPoints == b.writeoutMinPoints && a.writeoutMaxDelay == b.writeoutMaxDelay &&
		a.writeoutSpread == b.writeoutSpread && a.writeoutPhase == b.writeoutPhase &&
		a.writeoutSparseSpread == b.writeoutSparseSpread && a.writeoutStartupSpread == b.writeoutStartupSpread &&
		a.startupWriteout == b.startupWriteout
}

func floorSecond(elapsed time.Duration) int64 {
	seconds := int64(elapsed / time.Second)
	if elapsed < 0 && elapsed%time.Second != 0 {
		seconds--
	}
	return seconds
}

func ceilSecond(elapsed time.Duration) int64 {
	seconds := int64(elapsed / time.Second)
	if elapsed > 0 && elapsed%time.Second != 0 {
		seconds++
	}
	return seconds
}

// Multiplication must saturate for the bucket containing MinInt64 nanoseconds.
func secondDuration(second int64) time.Duration {
	if second < (-1<<63)/int64(time.Second) {
		return -1 << 63
	}
	if second > (1<<63-1)/int64(time.Second) {
		return 1<<63 - 1
	}
	return time.Duration(second) * time.Second
}

func (w *writeoutIndex) deadlineAt(at time.Time, delta int64) {
	if at.IsZero() {
		w.immediate += delta
	} else {
		w.deadlines.add(ceilSecond(at.Sub(w.epoch)), delta)
	}
}

func (w *writeoutIndex) deadline(it *item, delta int64) {
	w.deadlineAt(writeoutDeadlineClock(it, w.settings), delta)
}

func (w *writeoutIndex) add(it *item, delta int64) {
	at := writeoutDeadlineClock(it, w.settings)
	w.deadlineAt(at, delta)
	if !it.firstArrival.IsZero() {
		w.arrivals.add(floorSecond(it.firstArrival.Sub(w.epoch)), delta)
	}
}

// appended is called after encoding and before an immediate restore clears the
// old arrival. Ordinary appends only test threshold transitions; they do not
// visit either tree or compute a deadline.
func (w *writeoutIndex) appended(it *item, count int, immediate bool) {
	s := w.settings
	n := it.count()
	threshold := s.minPointsFor(it)
	if !immediate && !(n >= threshold && n-count < threshold) &&
		!(it.has(itemSparse) && s.writeoutSparseSpread && n > sparseMaxPoints && n-count <= sparseMaxPoints) {
		return
	}
	// Only deadline inputs belong in the temporary item; its log reference
	// contains atomics and must never be copied.
	before := item{firstArrival: it.firstArrival, spread: it.spread, slot: it.slot,
		flags: it.flags, compact: compactPoints{n: n - count}}
	w.deadline(&before, -1)
	if immediate && !it.firstArrival.IsZero() {
		w.arrivals.add(floorSecond(it.firstArrival.Sub(w.epoch)), -1)
	}
	before.compact.n = n
	if immediate {
		before.firstArrival = time.Time{}
	}
	w.deadline(&before, 1)
}

// During a settings rebuild, a temporary set identifies copied items. It exists
// for only one shard at a time; ordinary items carry no sampler metadata.
type writeoutSummary struct {
	writeoutIndex
	stableSettings *cacheSettings
	building       *writeoutBuild
}

type writeoutBuild struct {
	writeoutIndex
	seen   map[*item]struct{}
	cursor int
	slots  *itemSlot
}

func (w *writeoutSummary) add(it *item, delta int64) {
	w.writeoutIndex.add(it, delta)
	if b := w.building; b != nil {
		_, seen := b.seen[it]
		if delta > 0 {
			b.add(it, delta)
			b.seen[it] = struct{}{}
		} else if seen {
			b.add(it, delta)
			delete(b.seen, it)
		}
	}
}

// Keep sampler bookkeeping off the ordinary append's stack and register set.
func (c *Cache) appendWriteout(shard *Shard, it *item, count int, exists, immediate bool) {
	w := &shard.writeout
	if !exists {
		if shard.items.len() == 1 && w.building == nil {
			w.settings = c.settings.Load().(*cacheSettings)
			w.stableSettings = w.settings
		}
		if immediate {
			it.firstArrival = time.Time{}
		}
		w.add(it, 1)
	} else {
		w.appended(it, count, immediate)
	}
	c.syncMemoryLocked(shard)
}

func (w *writeoutSummary) appended(it *item, count int, immediate bool) {
	w.writeoutIndex.appended(it, count, immediate)
	if b := w.building; b != nil {
		if _, seen := b.seen[it]; seen {
			b.appended(it, count, immediate)
		}
	}
	if immediate {
		it.firstArrival = time.Time{}
	}
}

func (w *writeoutSummary) memoryBytes() int64 {
	n := w.writeoutIndex.memoryBytes()
	if b := w.building; b != nil {
		n += b.writeoutIndex.memoryBytes() + int64(unsafe.Sizeof(writeoutBuild{})) + int64(len(b.seen))*32
	}
	return n
}

// A reload visits at most 32 slots per lock acquisition. Ingest and pops keep
// running between batches and update the replacement alongside the old index.
// Rehashing restarts the slot cursor; the copied set prevents double counting.
func (c *Cache) rebuildWriteout(shard *Shard, s *cacheSettings) {
	for {
		shard.mu.Lock()
		w := &shard.writeout
		if sameWriteoutSettings(w.settings, s) {
			w.settings, w.stableSettings = s, s
			shard.mu.Unlock()
			return
		}
		if w.building == nil {
			w.building = &writeoutBuild{writeoutIndex: writeoutIndex{settings: s, epoch: w.epoch}, seen: make(map[*item]struct{})}
			w.stableSettings = nil
		}
		b := w.building
		var slots *itemSlot
		if len(shard.items.slots) > 0 {
			slots = &shard.items.slots[0]
		}
		if b.slots != slots {
			b.slots, b.cursor = slots, 0
		}
		end := min(b.cursor+32, len(shard.items.slots))
		for _, slot := range shard.items.slots[b.cursor:end] {
			if it := slot.item; it != nil {
				if _, seen := b.seen[it]; !seen {
					b.add(it, 1)
					b.seen[it] = struct{}{}
				}
			}
		}
		b.cursor = end
		done := end == len(shard.items.slots)
		if done {
			w.writeoutIndex, w.stableSettings, w.building = b.writeoutIndex, s, nil
		}
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
		if done {
			return
		}
	}
}
