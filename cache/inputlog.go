package cache

import (
	"math"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// maxShardLogEntries bounds a shard's unwritten input log records: a stalled
// log must not exhaust memory. Live intake waits outside the shard lock when
// staging is full, requesting an early cycle instead of accepting unlogged input.
// At 4.5M points/s a one-second cycle records about 4400 per shard,
// but cycles stretch to seconds while shard locks are contended, and relays
// then flush their backlog at several times the usual rate: 1<<15 dropped
// up to 27M points per burst. Staging retains raw points in separate reference/value columns;
// the admission limit remains a point count.
const maxShardLogEntries = 1 << 17

// Restore can run much faster than the log writer. Leave half the bounded
// buffer for live input, and wait for a drain before admitting more old data.
const maxShardRestoredLogEntries = maxShardLogEntries / 2

// LogPressure wakes group commit when restored data exhausts its bounded
// staging allowance, or live input fills its staging buffer.
func (c *Cache) LogPressure() <-chan struct{} { return c.logPressure }

// shardLog is a shard's part of the input log, guarded by the shard lock.
// Points are recorded at generation gen. Every cached or in-flight item is
// tracked with the generation of its oldest point not yet persisted, so the
// log knows which segments it still needs. Queued items hold their first
// generation inline; first contains only in-flight batches and is nil while
// logging is off.
type shardLog struct {
	entries        inputlog.Batch
	gen            uint64
	first          map[*points.Points]uint64
	items          map[uint64]int32 // tracked items per first generation
	generationPeak int              // high-water number of simultaneously retained generations
	oldest         uint64           // no tracked item is older
	dropped        uint64
	waitDrained    chan struct{}
}

// stagingBytes includes capacity slack in every staging column.
func (l *shardLog) stagingBytes() int64 {
	return l.entries.Bytes()
}

// restoreSpace is called under the shard lock, before adding restored data.
func (l *shardLog) restoreSpace(n int) <-chan struct{} {
	return l.space(n, maxShardRestoredLogEntries)
}

func (l *shardLog) space(n, limit int) <-chan struct{} {
	if l.first == nil || l.entries.Len()+n <= limit {
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

func (l *shardLog) record(ref *inputlog.StreamRef, value float64, timestamp int64) bool {
	if l.entries.Len() >= maxShardLogEntries {
		l.dropped++
		return false
	}
	return l.entries.Append(ref, value, timestamp)
}

func (l *shardLog) recordPoints(ref *inputlog.StreamRef, data []points.Point) {
	n := min(len(data), maxShardLogEntries-l.entries.Len())
	l.entries.AppendPoints(ref, data[:n])
	l.dropped += uint64(len(data) - n)
}

// track starts tracking a new item at the current generation.
func (l *shardLog) track(p *points.Points) {
	if l.first == nil {
		return
	}
	if _, tracked := l.first[p]; !tracked {
		l.first[p] = l.gen
		l.items[l.gen]++
		l.generationPeak = max(l.generationPeak, len(l.items))
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
		if len(l.items) == 0 && l.generationPeak > 64 {
			l.items = make(map[uint64]int32)
			l.generationPeak = 0
		}
	}
}

// trackItem keeps queued tracking on the item instead of one pointer-map
// entry per metric. Every generation includes both queued and in-flight items.
func (l *shardLog) trackItem(it *item) {
	if l.first != nil && !it.has(itemLogTracked) {
		it.logFirst = l.gen
		it.setFlag(itemLogTracked, true)
		l.items[l.gen]++
		l.generationPeak = max(l.generationPeak, len(l.items))
	}
}

func (l *shardLog) untrackItem(it *item) {
	if l.first != nil && it.has(itemLogTracked) {
		l.release(it.logFirst)
		it.setFlag(itemLogTracked, false)
	}
}

// pop transfers ownership to the in-flight map without changing counts. A
// successor with the same name has a different Points identity/generation.
func (l *shardLog) pop(it *item, p *points.Points) {
	if l.first != nil && it.has(itemLogTracked) {
		l.first[p] = it.logFirst
		it.setFlag(itemLogTracked, false)
	}
}

func (l *shardLog) mergeGeneration(into *item, gen uint64) {
	if !into.has(itemLogTracked) || gen < into.logFirst {
		if into.has(itemLogTracked) {
			l.release(into.logFirst)
		}
		into.logFirst = gen
		into.setFlag(itemLogTracked, true)
		l.items[gen]++
		l.generationPeak = max(l.generationPeak, len(l.items))
	}
}

// mergeItem and mergeFlight transfer the oldest generation to a replacement
// queued item, removing each old identity exactly once.
func (l *shardLog) mergeItem(into, from *item) {
	if l.first != nil && from.has(itemLogTracked) {
		gen := from.logFirst
		l.untrackItem(from)
		l.mergeGeneration(into, gen)
	}
}

func (l *shardLog) mergeFlight(into *item, from *points.Points) {
	if gen, tracked := l.first[from]; tracked {
		l.untrack(from)
		l.mergeGeneration(into, gen)
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

// relog records old queued and in-flight batches again at the current
// generation, so the log can recycle their old segments. The first eligible
// batch may exceed budget, preserving the previous progress guarantee.
func (l *shardLog) relog(gen uint64, budget int, queued *itemMap) int {
	if l.first == nil || l.lowWater() >= gen {
		return 0
	}
	n := 0
	for _, it := range queued.all {
		if !it.has(itemLogTracked) || it.logFirst >= gen {
			continue
		}
		if n > 0 && n+it.count() > budget {
			return n
		}
		// Only a complete replacement may release the old journal generation.
		// Oversized items pin their original segments until persistence; do not
		// allocate/decode them just to clip and retire their sole durable copy.
		if it.count() > maxShardLogEntries-l.entries.Len() {
			continue
		}
		l.recordPoints(&it.StreamRef, it.compact.appendDecoded(nil))
		n += it.count()
		l.release(it.logFirst)
		it.logFirst = l.gen
		l.items[l.gen]++
		l.generationPeak = max(l.generationPeak, len(l.items))
	}
	for p, first := range l.first {
		if first >= gen {
			continue
		}
		if n > 0 && n+len(p.Data) > budget {
			break
		}
		if len(p.Data) > maxShardLogEntries-l.entries.Len() {
			continue
		}
		l.recordPoints(inputlog.NewStreamRef(p.Metric), p.Data)
		n += len(p.Data)
		l.release(first)
		l.first[p] = l.gen
		l.items[l.gen]++
		l.generationPeak = max(l.generationPeak, len(l.items))
	}
	return n
}

// EnableInputLog starts recording accepted points for an inputlog.Log, which
// takes them through the inputlog.Source methods. Items already cached count
// as recorded at generation zero. Call it before input starts.
func (c *Cache) EnableInputLog() {
	c.enableInputLog(true)
}

// EnableInputLogFromNow starts a fresh journal after all earlier journalled
// points have persisted. Existing cache points retain independent spool sources;
// they must not be adopted/relogged into this new journal. Input is fenced.
func (c *Cache) EnableInputLogFromNow() {
	c.enableInputLog(false)
}

func (c *Cache) enableInputLog(adopt bool) {
	for _, shard := range c.data {
		shard.mu.Lock()
		if shard.log.first == nil {
			shard.log = shardLog{first: make(map[*points.Points]uint64), items: make(map[uint64]int32)}
			if adopt {
				for _, values := range shard.items.all {
					values.setFlag(itemLogTracked, false)
					shard.log.trackItem(values)
				}
				for _, p := range shard.notConfirmed[:shard.notConfirmedUsed] {
					if p != nil {
						shard.log.track(p)
					}
				}
			}
		}
		c.syncMemoryLocked(shard)
		shard.mu.Unlock()
	}
}

// InputLogIdle is used with live input fenced and history replay joined. No
// producer can add journal entries then, and persistence only releases items.
func (c *Cache) InputLogIdle() bool {
	for _, shard := range c.data {
		shard.mu.Lock()
		busy := shard.log.entries.Len() != 0 || len(shard.log.items) != 0
		shard.mu.Unlock()
		if busy {
			return false
		}
	}
	return true
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

// TakeLogBatch implements inputlog.BatchSource. Swapping staging buffers under
// the shard lock gives the log immutable points while ingestion continues.
func (c *Cache) TakeLogBatch(shard int, gen uint64, spare inputlog.Batch) inputlog.Batch {
	s := c.data[shard]
	s.mu.Lock()
	taken := s.log.entries
	spare.Reset()
	s.log.entries, s.log.gen = spare, gen
	s.log.wakeRestore()
	// Empty maps can retain millions of slots after a complete drain. Releasing
	// at a log cycle avoids reallocating the table on each pop/reinsert lifetime.
	if s.items.n == 0 && len(s.items.slots) > 0 {
		s.items = itemMap{}
	}
	s.releaseDrainedTracking()
	c.syncMemoryLocked(s)
	s.mu.Unlock()
	return taken
}

// TakeLogEntries implements the original Source interface for callers that
// still consume rows. The log uses TakeLogBatch, without this expansion.
func (c *Cache) TakeLogEntries(shard int, gen uint64, spare []inputlog.Entry) []inputlog.Entry {
	batch := c.TakeLogBatch(shard, gen, inputlog.Batch{})
	return batch.Entries(spare[:0])
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

// RelogBefore implements inputlog.Source.
func (c *Cache) RelogBefore(gen uint64, budget int) int {
	n := 0
	for _, shard := range c.data {
		if n >= budget {
			break
		}
		shard.mu.Lock()
		n += shard.log.relog(gen, budget-n, &shard.items)
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
