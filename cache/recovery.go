package cache

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// pendingRecovery is installed before any input receivers start. Each metric's
// handoff from the immutable source into cache is protected by its shard lock,
// the same lock used by Get and PopNotConfirmed.
//
// Live input may arrive before every saved metric is claimed. The first live
// write for a metric claims its saved history into the same cache item under
// that shard lock, so saved points never reach disk after newer live points of
// the same metric. unpersisted tracks claimed items, with their failed writes,
// until the persister confirms them; the sources are retired only when none
// remain.
type pendingRecovery struct {
	bundle    *recovery.Bundle
	claimed   []atomic.Uint64
	remaining atomic.Uint64
	// The owner matched the saved read-index generation and prepared every
	// pending name with the ordinary storable-name policy before input opens.
	namesPrepared bool
	// newSlots marks slots whose metric was absent from the saved read index;
	// set only with namesPrepared. Other claimed metrics may wait for a spread
	// deadline (see SetWriteoutRestoreSpread): the store already has them,
	// so the first scan need not wait for their writes.
	newSlots []uint64
	// Retries and confirmations of one metric stay in the same tracking
	// shard, without serializing every recovery writer on a global lock.
	writes [64]pendingWrites
}

type pendingWrites struct {
	mu          sync.Mutex
	unpersisted map[*points.Points]int
	// deferred holds the unpersisted items claimed with a spread deadline.
	deferred map[*points.Points]struct{}
}

func (r *pendingRecovery) writesFor(p *points.Points) *pendingWrites {
	return &r.writes[helper.HashString(p.Metric)&(uint64(len(r.writes))-1)]
}

// pendingWriteAttempts bounds how often a saved metric may fail to write
// before recovery stops waiting for it. Its points stay in the cache and keep
// being retried like live points; a file the persister cannot update must not
// hold back source retirement and the first scan forever.
const pendingWriteAttempts = 3

func (r *pendingRecovery) track(p *points.Points) {
	r.trackAs(p, false)
}

// trackAs tracks p, deferred if it waits for a spread writeout deadline.
func (r *pendingRecovery) trackAs(p *points.Points, deferred bool) {
	w := r.writesFor(p)
	w.mu.Lock()
	if w.unpersisted == nil {
		w.unpersisted = make(map[*points.Points]int)
	}
	w.unpersisted[p] = 0
	if deferred {
		if w.deferred == nil {
			w.deferred = make(map[*points.Points]struct{})
		}
		w.deferred[p] = struct{}{}
	} else {
		delete(w.deferred, p)
	}
	w.mu.Unlock()
}

// isNewSlot reports whether slot's metric was absent from the saved read
// index; without prepared names every slot counts as new.
func (r *pendingRecovery) isNewSlot(slot uint64) bool {
	if !r.namesPrepared || r.newSlots == nil {
		return true
	}
	return r.newSlots[slot/64]&(uint64(1)<<(slot%64)) != 0
}

// failed counts a write the persister handed back and settles the metric once
// it has failed pendingWriteAttempts times.
func (r *pendingRecovery) failed(p *points.Points) {
	w := r.writesFor(p)
	w.mu.Lock()
	if n, ok := w.unpersisted[p]; ok {
		if n+1 >= pendingWriteAttempts {
			delete(w.unpersisted, p)
			delete(w.deferred, p)
		} else {
			w.unpersisted[p] = n + 1
		}
	}
	w.mu.Unlock()
}

// persisted reports a confirmed write. Requeue keeps tracking by moving it.
func (r *pendingRecovery) persisted(p *points.Points) {
	w := r.writesFor(p)
	w.mu.Lock()
	delete(w.unpersisted, p)
	delete(w.deferred, p)
	w.mu.Unlock()
}

func (r *pendingRecovery) moved(from, to *points.Points) {
	w := r.writesFor(from)
	w.mu.Lock()
	if n, ok := w.unpersisted[from]; ok {
		_, fromDeferred := w.deferred[from]
		delete(w.unpersisted, from)
		delete(w.deferred, from)
		_, toTracked := w.unpersisted[to]
		_, toDeferred := w.deferred[to]
		w.unpersisted[to] = max(n, w.unpersisted[to])
		// The merged item may defer only if both sides could.
		if !fromDeferred || toTracked && !toDeferred {
			delete(w.deferred, to)
		} else {
			w.deferred[to] = struct{}{}
		}
	}
	w.mu.Unlock()
}

// Outstanding counts saved metrics not yet claimed plus claimed cache items
// whose saved points have not been confirmed on disk.
func (r *pendingRecovery) outstanding() uint64 {
	return r.count(false)
}

// count is outstanding, without the deferred items when urgent is set.
func (r *pendingRecovery) count(urgent bool) uint64 {
	n := r.remaining.Load()
	for i := range r.writes {
		w := &r.writes[i]
		w.mu.Lock()
		n += uint64(len(w.unpersisted))
		if urgent {
			n -= uint64(len(w.deferred))
		}
		w.mu.Unlock()
	}
	return n
}

func (r *pendingRecovery) isClaimed(slot uint64) bool {
	return r.claimed[slot/64].Load()&(uint64(1)<<(slot%64)) != 0
}
func (r *pendingRecovery) claim(slot uint64) {
	r.claimed[slot/64].Or(uint64(1) << (slot % 64))
	r.remaining.Add(^uint64(0))
}

// AttachPendingRecovery requires an empty cache and closed input receivers.
// Receivers may open once reads serve the bundle: see pendingRecovery.
func (c *Cache) AttachPendingRecovery(bundle *recovery.Bundle) error {
	return c.attachPendingRecovery(bundle, false)
}

// AttachPendingRecoveryWithPreparedNames also attests that the owner matched
// the saved read-index generation and prepared every pending name, including
// rejecting unstorable names. First-write claims then need no repeated index
// lookup or realtime notification for names already handled by that pass.
func (c *Cache) AttachPendingRecoveryWithPreparedNames(bundle *recovery.Bundle) error {
	return c.attachPendingRecovery(bundle, true)
}

func (c *Cache) attachPendingRecovery(bundle *recovery.Bundle, namesPrepared bool) error {
	if !c.IsEmpty() {
		return errors.New("cannot attach recovery to a nonempty cache")
	}
	r := &pendingRecovery{bundle: bundle, claimed: make([]atomic.Uint64, (bundle.Slots()+63)/64), namesPrepared: namesPrepared}
	if namesPrepared {
		r.newSlots = make([]uint64, (bundle.Slots()+63)/64)
		bundle.VisitNewSlots(func(slot uint64) {
			if slot < bundle.Slots() {
				r.newSlots[slot/64] |= uint64(1) << (slot % 64)
			}
		})
	}
	r.remaining.Store(bundle.Metrics())
	if !c.pending.CompareAndSwap(nil, r) {
		return errors.New("pending recovery already attached")
	}
	return nil
}

// HasRecoveryCapacity reserves half the cache for live and in-flight work.
// Below that boundary, the published size plus its bounded unpublished slack
// avoids locking every shard for each input record. Only the pressure boundary
// needs an exact count. An empty cache can admit one oversized record so that
// recovery always has a way to make progress.
func (c *Cache) HasRecoveryCapacity(count int) bool {
	s := c.settings.Load().(*cacheSettings)
	if s.maxBytes > 0 && c.stat.bytes.Load()+shardCount*byteSlackFor(0)+c.queueMemory() > s.maxBytes/2 {
		if (c.Size() > 0 || c.NotConfirmedLength() > 0) && c.EstimatedBytes() > s.maxBytes/2 {
			return false
		}
	}
	if s.maxSize <= 0 {
		return true
	}
	// A shard may retain a delta accumulated under a larger or unlimited
	// cache setting. The maximum slack stays valid across limit changes.
	upper := c.approxSize() + shardCount*sizeSlackFor(0)
	if upper+int64(count) <= s.maxSize/2 {
		return true
	}
	size := c.Size()
	return size == 0 || size+int64(count) <= s.maxSize/2
}

// pendingPoints is called with the metric's cache shard locked. Returned points
// own their data, so the caller never retains a slice into the mapped source.
func (c *Cache) pendingPoints(key string) ([]points.Point, error) {
	r := c.pending.Load()
	if r == nil {
		return nil, nil
	}
	slot, found, err := r.bundle.Find(key)
	if err != nil || !found {
		return nil, err
	}
	if r.isClaimed(slot) {
		return nil, nil
	}
	p, err := r.bundle.Read(slot)
	if err != nil {
		return nil, err
	}
	return p.Data, nil
}

// RecoverPending loads one complete metric at a time. It bypasses live quotas
// because these points were already accepted before the restart. Receivers stay
// closed until the returned work and all in-flight persistence have drained.
func (c *Cache) RecoverPending(stop <-chan struct{}, pointsPerSecond int) error {
	phasecpu.Set(phasecpu.Restore)
	defer phasecpu.Set(phasecpu.Other)
	r := c.pending.Load()
	if r == nil {
		return nil
	}
	started := time.Now()
	var restored int64
	// Metrics come in the order the bundle reads fastest.
	return r.bundle.VisitMetrics(func(slot uint64, name string) error {
		count := r.bundle.Count(slot)
		for {
			select {
			case <-stop:
				return errors.New("pending recovery stopped")
			default:
			}
			// Reserve half the configured cache for in-flight work. One large metric
			// can always progress once the cache has drained.
			if c.HasRecoveryCapacity(int(count)) {
				break
			}
			select {
			case <-stop:
				return errors.New("pending recovery stopped")
			case <-time.After(10 * time.Millisecond):
			}
		}
		if pointsPerSecond > 0 {
			restored += int64(count)
			delay := time.Until(started.Add(time.Duration(float64(restored) / float64(pointsPerSecond) * float64(time.Second))))
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-stop:
					timer.Stop()
					return errors.New("pending recovery stopped")
				case <-timer.C:
				}
			}
		}
		return c.claimPendingMetric(r, name, slot)
	})
}

func (c *Cache) claimPendingMetric(r *pendingRecovery, name string, slot uint64) error {
	shard := c.GetShard(name)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return c.claimLocked(r, shard, name, slot, 0, true)
}

// claimForWrite runs under the shard lock before a live write to name. It moves
// the metric's saved history into cache first, if it is still unclaimed.
func (c *Cache) claimForWrite(r *pendingRecovery, shard *Shard, name string, count int, startup bool) error {
	slot, found, err := r.bundle.Find(name)
	if err != nil || !found || r.isClaimed(slot) {
		return err
	}
	return c.claimLocked(r, shard, name, slot, count, startup)
}

func (c *Cache) claimLocked(r *pendingRecovery, shard *Shard, name string, slot uint64, extra int, startup bool) error {
	if r.isClaimed(slot) {
		return nil
	}
	// Every live write claims first, so an item cannot precede its history.
	// Refuse an invalid orchestration instead of silently changing precedence.
	if shard.items.get(name) != nil {
		return fmt.Errorf("metric %q was written before pending recovery", name)
	}
	// A compact dump's first record is the item's encoding already: copy
	// it instead of decoding and encoding again. Other records' points are
	// decoded into the shard's scratch and appended. The slot was found by
	// name, or name was read from it, so it need not be compared again;
	// the bundle still checks that every record of the slot is named alike.
	var head recovery.CompactRecord
	tail, err := r.bundle.ReadCompactAs(slot, name, shard.claimData[:0], func(rec recovery.CompactRecord) { head = rec })
	shard.claimData = tail[:0]
	if err != nil {
		return err
	}
	values := newItem(name, 0)
	if head.N > 0 {
		values.compact = compactPoints{buf: bytes.Clone(head.Data), n: head.N,
			ts: head.State.Timestamp, delta: head.State.Delta, bits: head.State.Bits}
	}
	values.compact.appendPoints(tail)
	if cap(shard.claimData) > maxClaimScratch {
		shard.claimData = nil
	}
	shard.items.set(name, values)
	s := c.settings.Load().(*cacheSettings)
	c.addSizeLocked(shard, int64(values.count()), s.sizeSlack)
	// A metric the store already has keeps its saved points durable here
	// until written (the source retires only then), so it may wait for a
	// spread deadline and be written once with its first live points. A new
	// one is written at once: the first scan waits for it.
	deferred := s.writeoutRestoreSpread && s.batching() && !r.isNewSlot(slot)
	r.trackAs(&values.Points, deferred)
	shard.log.track(&values.Points)
	r.claim(slot)
	c.syncMemoryLocked(shard)
	if filter := c.newMetricCf.Load(); extra > 0 && r.namesPrepared && filter != nil {
		filter.Add(helper.HashString(name))
	}
	if deferred {
		now := time.Now()
		values.firstArrival = s.restoreArrival(values, now)
		if startup {
			c.spreadStartup(values, s, now)
		}
		c.writeoutQueue.notifyAt(values.firstArrival.Add(s.delayFor(values)))
		return nil
	}
	c.writeoutQueue.notifyAt(time.Now())
	return nil
}

// maxClaimScratch bounds the decode buffer a shard keeps between claims.
const maxClaimScratch = 4096

// PendingOutstanding reports saved metrics not yet confirmed on disk.
func (c *Cache) PendingOutstanding() uint64 {
	if r := c.pending.Load(); r != nil {
		return r.outstanding()
	}
	return 0
}

// PendingUrgentOutstanding is PendingOutstanding without claimed metrics that
// wait for a spread deadline: those the saved read index already held, which
// the first scan need not wait for.
func (c *Cache) PendingUrgentOutstanding() uint64 {
	if r := c.pending.Load(); r != nil {
		return r.count(true)
	}
	return 0
}

// dumpPending writes saved metrics that were never claimed. Callers must have
// diverted input and stopped RecoverPending, so no claim can race the dump.
func (c *Cache) dumpPending(write func(*points.Points) error) error {
	r := c.pending.Load()
	if r == nil {
		return nil
	}
	for slot := uint64(0); slot < r.bundle.Slots(); slot++ {
		name, found, err := r.bundle.Name(slot)
		if err != nil {
			return err
		}
		if !found || r.isClaimed(slot) {
			continue
		}
		p, err := r.bundle.Read(slot)
		if err != nil {
			return err
		}
		if p == nil || p.Metric != name {
			return errors.New("pending recovery metric identity differs")
		}
		if err = write(p); err != nil {
			return err
		}
	}
	return nil
}

// DumpPendingRange copies the unclaimed saved metrics of part seg of parts.
// Into a legacy dump of a legacy checkpoint they go as their original
// encoded records, without decoding them; otherwise each metric's saved
// points are read and written as one record of out's format. Parts are
// disjoint slot ranges, and unclaimed metrics have no cache data, so parts
// may be written concurrently with each other and with DumpShards.
func (c *Cache) DumpPendingRange(seg, parts int, out *recovery.Segment) error {
	r := c.pending.Load()
	if r == nil {
		return nil
	}
	raw := out.Format() == recovery.FormatLegacy && r.bundle.CacheFormat() == recovery.FormatLegacy
	slots := r.bundle.Slots()
	for slot := slots * uint64(seg) / uint64(parts); slot < slots*uint64(seg+1)/uint64(parts); slot++ {
		if r.isClaimed(slot) {
			continue
		}
		name, found, err := r.bundle.Name(slot)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if raw {
			err = r.bundle.RawRecords(slot, func(raw []byte, count int) error { return out.WriteRaw(name, raw, count) })
		} else {
			var p *points.Points
			if p, err = r.bundle.Read(slot); err == nil {
				err = out.WritePoints(p)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// RetirePendingSources removes the old sources after a newer dump that also
// holds their unpersisted points is durable. Reader mappings stay valid.
func (c *Cache) RetirePendingSources() error {
	r := c.pending.Load()
	if r == nil {
		return nil
	}
	return r.bundle.Retire()
}

// FinishPendingRecovery is called after every saved metric has been written (or
// handled by the existing invalid-metric policy). A crash before retirement can
// safely replay the unchanged legacy sources: a metric's live values only reach
// disk in the same write as its saved history, so replay merely repeats points.
func (c *Cache) FinishPendingRecovery() error {
	r := c.pending.Load()
	if r == nil {
		return nil
	}
	if r.outstanding() != 0 {
		return errors.New("pending recovery has not drained")
	}
	if err := r.bundle.Retire(); err != nil {
		return err
	}
	c.pending.CompareAndSwap(r, nil)
	return nil
}
