package cache

import (
	"fmt"
	"math"
	"time"
)

func (s *cacheSettings) batching() bool {
	return s.writeoutMinPoints > 0 && s.writeoutMaxDelay > 0
}

// SetWriteoutBatching delays small writes until minPoints or maxDelay since
// their first arrival. Both zero disables batching. Existing untracked arrivals
// remain immediately eligible when batching is enabled at runtime.
func (c *Cache) SetWriteoutBatching(minPoints int, maxDelay time.Duration) error {
	if minPoints < 0 || maxDelay < 0 || (minPoints == 0) != (maxDelay == 0) {
		return fmt.Errorf("writeout-min-points and writeout-max-delay must both be positive or both zero")
	}
	s := *c.settings.Load().(*cacheSettings)
	s.writeoutMinPoints, s.writeoutMaxDelay = minPoints, maxDelay
	c.settings.Store(&s)
	c.writeoutQueue.notifyAt(time.Now())
	return nil
}

// SetWriteoutSpread shortens each metric's writeout-min-points and max delay
// by a stable fraction of up to spread. Metrics that start batching together,
// as after a restart, otherwise reach the threshold or the deadline together
// and stay in one synchronized write wave per cycle; with a spread their
// cycles differ and drift apart. A metric's threshold and delay stay within
// [1-spread, 1] times the configured ones. Zero disables it. Reloadable, for
// cached items too.
func (c *Cache) SetWriteoutSpread(spread float64) error {
	if !(spread >= 0 && spread <= maxWriteoutSpread) {
		return fmt.Errorf("writeout-max-delay-spread must be within [0, %g]", maxWriteoutSpread)
	}
	s := *c.settings.Load().(*cacheSettings)
	s.writeoutSpread = spread
	c.settings.Store(&s)
	c.writeoutQueue.notifyAt(time.Now())
	return nil
}

const maxWriteoutSpread = 0.5

// SetWriteoutStartupSpread gives metrics starting together independent first
// deadlines across their usual spread periods. Unlike the ordinary point
// threshold, a startup window always waits for its phase, including when
// restored points already fill a batch. Pressure still bypasses batching.
// Only arrivals before a metric's first slot are shortened; later batches
// retain their usual thresholds and delays, without a metric history table.
// Reloadable; enabling starts with the next eligible arrival, once per cache.
func (c *Cache) SetWriteoutStartupSpread(on bool) {
	s := *c.settings.Load().(*cacheSettings)
	s.writeoutStartupSpread = on
	c.settings.Store(&s)
	c.writeoutQueue.notifyAt(time.Now())
}

// spreadStartup changes only a new, batchable item's first window. A shared
// epoch prevents every later batch getting another short window. Start it
// lazily: urgent log replay or handoff may run long before live input opens.
// The mixed sparse share is independent of the ordinary period's spread;
// using that spread for both phase and period makes the waves converge again.
func (c *Cache) spreadStartup(values *item, s *cacheSettings, now time.Time) {
	if !s.writeoutStartupSpread || values.sparse {
		return
	}
	epoch := c.startupAt.Load()
	if epoch == nil {
		epoch = new(time.Time)
		*epoch = now
		if !c.startupAt.CompareAndSwap(nil, epoch) {
			epoch = c.startupAt.Load()
		}
	}
	// A concurrent first arrival may have captured now before the winner.
	// Keep its ordinary window rather than extending its maximum delay.
	if now.Before(*epoch) {
		return
	}
	delay := s.delayFor(values)
	deadline := epoch.Add(time.Duration(float64(delay) * float64(values.sparseShare()) / (1 << 16)))
	if now.Before(deadline) {
		values.firstArrival = deadline.Add(-delay)
		values.startup = true
	}
}

// SetWriteoutRestoreSpread makes points restored after a restart wait for a
// writeout deadline spread over the first max delay, by metric, instead of
// being written at once, when they stay durable meanwhile: dump points the
// input log records, and saved checkpoint metrics the read index already held
// (their source is retired only once they are written). Each such metric is
// then written once, with its first live points, rather than at restore and
// again a cycle later. Reloadable; applies to later restores.
func (c *Cache) SetWriteoutRestoreSpread(on bool) {
	s := *c.settings.Load().(*cacheSettings)
	s.writeoutRestoreSpread = on
	c.settings.Store(&s)
}

// restoreArrival is the first arrival that gives a restored item of values a
// writeout deadline in [now, now+writeoutMaxDelay), placed by the metric's
// spread fraction.
func (s *cacheSettings) restoreArrival(values *item, now time.Time) time.Time {
	at := time.Duration(float64(s.writeoutMaxDelay) * float64(values.spread) / (1 << 16))
	return now.Add(at - s.delayFor(values))
}

// writeoutSpreadFrac is a metric's fixed share of the writeout spread, in
// units of 1/65536, from an FNV-1a hash of its name.
func writeoutSpreadFrac(metric string) uint16 {
	return uint16(writeoutHash(metric) >> 48)
}

// writeoutHash is the FNV-1a hash of metric. Its top 16 bits are the
// metric's spread share; shards are picked by an unrelated hash.
func writeoutHash(metric string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(metric); i++ {
		h ^= uint64(metric[i])
		h *= 1099511628211
	}
	return h
}

// sparseSlot derives a metric's sparseLast slot and sparse share from its
// writeoutHash h. FNV-1a's top bits hardly depend on the last byte, so
// names differing only there, such as numbered series, would share a sparse
// share; a final mix spreads them.
func sparseSlot(h uint64) uint32 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return uint32(h)
}

// sparseShare is the item's fixed place, in units of 1/65536 of
// writeout-max-delay, of its sparse deadline. It reuses the slot's top bits,
// which only tables of more than 65536 slots also index by.
func (values *item) sparseShare() uint16 {
	return uint16(values.slot >> 16)
}

// SetWriteoutSparseSpread spreads sparse metrics' writes over the whole
// writeout-max-delay instead of its last spread share. A metric is sparse
// when the first point of a new cache item is at least writeout-max-delay
// newer than the last point written from it before: it then holds that one
// point for the whole window, so one write costs the same anywhere in it.
// Its deadline is placed at a stable fraction, by name hash, of [0,
// writeout-max-delay) after its arrival. Metrics that report hourly at the
// same minute are otherwise all written within the window's last spread
// share, an hourly burst of updates. A sparse item that gains more than
// sparseMaxPoints points returns to the dense deadline. Dense metrics, whose
// cost is the number of writes, keep their placement. Each shard remembers
// the last written timestamp per hash slot, 16 bytes per cached item, so a
// metric is recognized from its second write after enabling (or a
// restart); slots shared with a denser metric keep it dense. Reloadable;
// disabling releases the tables and returns sparse items to the dense
// deadline.
func (c *Cache) SetWriteoutSparseSpread(on bool) {
	s := *c.settings.Load().(*cacheSettings)
	if s.writeoutSparseSpread == on {
		return
	}
	s.writeoutSparseSpread = on
	c.settings.Store(&s)
	if !on {
		for _, shard := range c.data {
			shard.mu.Lock()
			shard.sparseLast = nil
			shard.mu.Unlock()
		}
	}
	c.writeoutQueue.notifyAt(time.Now())
}

// WriteoutSparseSpread reports whether sparse metrics' writes are spread over
// the whole writeout window (see SetWriteoutSparseSpread).
func (c *Cache) WriteoutSparseSpread() bool {
	return c.settings.Load().(*cacheSettings).writeoutSparseSpread
}

// sparseMaxPoints is the most points a sparse item holds before it is
// treated as dense again. A second point can be a resent duplicate.
const sparseMaxPoints = 2

// sparseSlotsPerItem sizes a shard's sparseLast table against its items. A
// sparse metric whose slot a dense one shares is not recognized, so the
// table must be several times larger than the cached items, which are
// mostly dense.
const sparseSlotsPerItem = 4

// sparseSlots returns the shard's sparseLast table, grown for items cached
// items. Called with the shard locked. Growing copies each slot to every slot
// it splits into, so metrics keep their history; the copies only make other
// metrics look denser.
func (shard *Shard) sparseSlots(items int) []uint32 {
	want := max(64, items*sparseSlotsPerItem)
	if len(shard.sparseLast) >= want {
		return shard.sparseLast
	}
	n := 64
	for n < want {
		n <<= 1
	}
	grown := make([]uint32, n)
	if old := shard.sparseLast; len(old) > 0 {
		for i := range grown {
			grown[i] = old[i&(len(old)-1)]
		}
	}
	shard.sparseLast = grown
	return grown
}

// sparseSince reports whether values, a new item whose first point is at
// first, is sparse: the last point written from its slot is at least maxDelay
// older. Called with the shard locked, which holds items cached items.
func (shard *Shard) sparseSince(values *item, first int64, maxDelay time.Duration, items int) bool {
	slots := shard.sparseSlots(items)
	last := slots[values.slot&uint32(len(slots)-1)]
	window := int64((maxDelay + time.Second - 1) / time.Second)
	return last != 0 && first-int64(last) >= window
}

// noteWritten records the last point of values, which is being popped for
// writing, in its slot. Called with the shard locked.
func (shard *Shard) noteWritten(values *item, s *cacheSettings) {
	if !s.writeoutSparseSpread || values.count() == 0 || values.compact.ts <= 0 {
		return
	}
	slots := shard.sparseSlots(shard.items.len() + 1)
	ts := uint32(min(values.compact.ts, math.MaxUint32))
	if i := values.slot & uint32(len(slots)-1); ts > slots[i] {
		slots[i] = ts
	}
}

// spreadShare is the fraction values' threshold and delay are shortened by.
func (s *cacheSettings) spreadShare(values *item) float64 {
	return s.writeoutSpread * float64(values.spread) / (1 << 16)
}

// minPointsFor returns values' writeout threshold.
func (s *cacheSettings) minPointsFor(values *item) int {
	if s.writeoutSpread == 0 {
		return s.writeoutMinPoints
	}
	return s.writeoutMinPoints - int(float64(s.writeoutMinPoints)*s.spreadShare(values))
}

// delayFor returns values' max delay since its first arrival.
func (s *cacheSettings) delayFor(values *item) time.Duration {
	if values.sparse && s.writeoutSparseSpread && values.count() <= sparseMaxPoints {
		return time.Duration(float64(s.writeoutMaxDelay) * float64(values.sparseShare()) / (1 << 16))
	}
	if s.writeoutSpread == 0 {
		return s.writeoutMaxDelay
	}
	return s.writeoutMaxDelay - time.Duration(float64(s.writeoutMaxDelay)*s.spreadShare(values))
}

// Backlogged reports whether writes should not be rate-limited: restored
// points are still pending, or the cache holds over three quarters of
// max-size, where writeout batching is released too.
func (c *Cache) Backlogged() bool {
	return c.pending.Load() != nil || c.settings.Load().(*cacheSettings).underPressure(c.approxSize())
}

func (c *Cache) batchWrites(s *cacheSettings) bool {
	return s.batching() && !s.underPressure(c.approxSize())
}

func (s *cacheSettings) underPressure(size int64) bool {
	// Divide first to avoid overflowing a large configured capacity.
	return s.maxSize > 0 && size > s.maxSize/4*3+s.maxSize%4*3/4
}

// Called with the shard locked. Zero means immediately eligible.
func writeoutDeadline(values *item, s *cacheSettings) time.Time {
	first := values.firstArrival
	if values.startup && s.writeoutStartupSpread && !first.IsZero() {
		return first.Add(s.delayFor(values))
	}
	if values.count() >= s.minPointsFor(values) || first.IsZero() {
		return time.Time{}
	}
	return first.Add(s.delayFor(values))
}

// WriteoutReady must also be checked by the persister under its per-metric
// store lock: a queue entry can outlive the batch that originally made it ready.
func (c *Cache) WriteoutReady(metric string) bool {
	s := c.settings.Load().(*cacheSettings)
	if !c.batchWrites(s) {
		return true
	}
	shard := c.GetShard(metric)
	shard.mu.RLock()
	p := shard.items.get(metric)
	exists := p != nil
	ready := exists && !time.Now().Before(writeoutDeadline(p, s))
	shard.mu.RUnlock()
	return ready
}
