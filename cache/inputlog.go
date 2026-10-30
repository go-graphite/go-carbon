package cache

import (
	"math"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// maxShardLogEntries bounds a shard's unwritten input log records: a stalled
// log must not exhaust memory. Further points are still cached, but not
// logged. At 4.5M points/s a one-second cycle records about 4400 per shard,
// but cycles stretch to seconds while shard locks are contended, and relays
// then flush their backlog at several times the usual rate: 1<<15 dropped
// up to 27M points per burst on converting hosts. Entries are 32 bytes, so
// a full buffer across all shards holds 4 GiB.
const maxShardLogEntries = 1 << 17

// Restore can run much faster than the log writer. Leave half the bounded
// buffer for live input, and wait for a drain before admitting more old data.
const maxShardRestoredLogEntries = maxShardLogEntries / 2

// shardLog is a shard's part of the input log, guarded by the shard lock.
// Points are recorded at generation gen. Every cached or in-flight item is
// tracked with the generation of its oldest point not yet persisted, so the
// log knows which segments it still needs. first is nil while logging is off.
type shardLog struct {
	entries     []inputlog.Entry
	gen         uint64
	first       map[*points.Points]uint64
	items       map[uint64]int32 // tracked items per first generation
	oldest      uint64           // no tracked item is older
	dropped     uint64
	waitDrained chan struct{}
}

// restoreSpace is called under the shard lock, before adding restored data.
// Live input keeps its non-blocking admission policy.
func (l *shardLog) restoreSpace(n int) <-chan struct{} {
	if l.first == nil || len(l.entries)+n <= maxShardRestoredLogEntries {
		return nil
	}
	if l.waitDrained == nil {
		l.waitDrained = make(chan struct{})
	}
	return l.waitDrained
}

func (l *shardLog) wakeRestore() {
	if l.waitDrained != nil {
		close(l.waitDrained)
		l.waitDrained = nil
	}
}

func (l *shardLog) record(ref *inputlog.StreamRef, value float64, timestamp int64) {
	if len(l.entries) >= maxShardLogEntries {
		l.dropped++
		return
	}
	l.entries = append(l.entries, ref.Entry(value, timestamp))
}

func (l *shardLog) recordPoints(ref *inputlog.StreamRef, data []points.Point) {
	for _, point := range data {
		l.record(ref, point.Value, point.Timestamp)
	}
}

// track starts tracking a new item at the current generation.
func (l *shardLog) track(p *points.Points) {
	if l.first == nil {
		return
	}
	if _, tracked := l.first[p]; !tracked {
		l.first[p] = l.gen
		l.items[l.gen]++
	}
}

// untrack is called once p's points are persisted, or dropped for good.
func (l *shardLog) untrack(p *points.Points) {
	if gen, tracked := l.first[p]; tracked {
		delete(l.first, p)
		l.release(gen)
	}
}

func (l *shardLog) release(gen uint64) {
	if l.items[gen]--; l.items[gen] <= 0 {
		delete(l.items, gen)
	}
}

// merge moves from's points into into, which keeps the older generation.
func (l *shardLog) merge(into, from *points.Points) {
	gen, tracked := l.first[from]
	if !tracked {
		return
	}
	l.untrack(from)
	if current, ok := l.first[into]; !ok || gen < current {
		if ok {
			l.release(current)
		}
		l.first[into] = gen
		l.items[gen]++
	}
}

func (l *shardLog) lowWater() uint64 {
	if len(l.items) == 0 {
		l.oldest = l.gen
	}
	// Every tracked generation is at least oldest and at most gen.
	for l.oldest < l.gen && l.items[l.oldest] == 0 {
		l.oldest++
	}
	return l.oldest
}

// relog records again, at the current generation, the points of items first
// recorded before gen, so their old segments can be reused. pointsOf returns
// a tracked item's points.
func (l *shardLog) relog(gen uint64, budget int, pointsOf func(*points.Points) ([]points.Point, *inputlog.StreamRef)) int {
	if l.first == nil || l.lowWater() >= gen {
		return 0
	}
	n := 0
	for p, first := range l.first {
		if first >= gen {
			continue
		}
		data, ref := pointsOf(p)
		if n > 0 && n+len(data) > budget {
			break
		}
		if ref == nil {
			ref = inputlog.NewStreamRef(p.Metric)
		}
		l.recordPoints(ref, data)
		n += len(data)
		l.release(first)
		l.first[p] = l.gen
		l.items[l.gen]++
	}
	return n
}

// EnableInputLog starts recording accepted points for an inputlog.Log, which
// takes them through the inputlog.Source methods. Items already cached count
// as recorded at generation zero. Call it before input starts.
func (c *Cache) EnableInputLog() {
	for _, shard := range c.data {
		shard.mu.Lock()
		if shard.log.first == nil {
			shard.log = shardLog{first: make(map[*points.Points]uint64), items: make(map[uint64]int32)}
			for _, values := range shard.items.all {
				shard.log.track(&values.Points)
			}
			for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
				if p != nil {
					shard.log.track(p)
				}
			}
		}
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
	}
}

// DisableInputLog stops recording once the log is closed.
func (c *Cache) DisableInputLog() {
	for _, shard := range c.data {
		shard.mu.Lock()
		shard.log.wakeRestore()
		shard.log = shardLog{}
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
	}
}

// TakeLogEntries implements inputlog.Source.
func (c *Cache) TakeLogEntries(shard int, gen uint64, spare []inputlog.Entry) []inputlog.Entry {
	s := c.data[shard]
	s.mu.Lock()
	taken := s.log.entries
	s.log.entries, s.log.gen = spare[:0], gen
	s.log.wakeRestore()
	c.syncMemoryLocked(s)
	s.mu.Unlock()
	return taken
}

// LogLowWater implements inputlog.Source.
func (c *Cache) LogLowWater() uint64 {
	low := uint64(math.MaxUint64)
	for _, shard := range c.data {
		shard.mu.Lock()
		low = min(low, shard.log.lowWater())
		shard.mu.Unlock()
	}
	return low
}

// loggedPoints returns the points of an item the input log tracks, cached or
// in flight. Called under the shard lock: a cached item holds its points
// compact, an in-flight batch is only read by the persister, and requeue
// changes it under this lock.
func (shard *Shard) loggedPoints(p *points.Points) ([]points.Point, *inputlog.StreamRef) {
	if values := shard.items.get(p.Metric); values != nil && &values.Points == p {
		return appendDecoded(nil, values.compact.buf, values.compact.n), &values.log
	}
	return p.Data, nil
}

// RelogBefore implements inputlog.Source.
func (c *Cache) RelogBefore(gen uint64, budget int) int {
	n := 0
	for _, shard := range c.data {
		if n >= budget {
			break
		}
		shard.mu.Lock()
		n += shard.log.relog(gen, budget-n, shard.loggedPoints)
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
	}
	return n
}

// LogDropped implements inputlog.Source.
func (c *Cache) LogDropped() uint64 {
	var n uint64
	for _, shard := range c.data {
		shard.mu.RLock()
		n += shard.log.dropped
		shard.mu.RUnlock()
	}
	return n
}
