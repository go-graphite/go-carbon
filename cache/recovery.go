package cache

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

// pendingRecovery is installed before any input receivers start. Each metric's
// handoff from the immutable source into cache is protected by its shard lock,
// the same lock used by Get and PopNotConfirmed.
type pendingRecovery struct {
	bundle    *recovery.Bundle
	claimed   []atomic.Uint64
	remaining atomic.Uint64
}

func (r *pendingRecovery) isClaimed(slot uint64) bool {
	return r.claimed[slot/64].Load()&(uint64(1)<<(slot%64)) != 0
}
func (r *pendingRecovery) claim(slot uint64) {
	r.claimed[slot/64].Or(uint64(1) << (slot % 64))
	r.remaining.Add(^uint64(0))
}

// AttachPendingRecovery requires an empty cache and closed input receivers.
// Until recovery completes, live input must retain its existing startup gate.
func (c *Cache) AttachPendingRecovery(bundle *recovery.Bundle) error {
	if !c.IsEmpty() {
		return errors.New("cannot attach recovery to a nonempty cache")
	}
	r := &pendingRecovery{bundle: bundle, claimed: make([]atomic.Uint64, (bundle.Slots()+63)/64)}
	r.remaining.Store(bundle.Metrics())
	if !c.pending.CompareAndSwap(nil, r) {
		return errors.New("pending recovery already attached")
	}
	return nil
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
	r := c.pending.Load()
	if r == nil {
		return nil
	}
	started := time.Now()
	var restored int64
	for slot := uint64(0); slot < r.bundle.Slots(); slot++ {
		name, found, err := r.bundle.Name(slot)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		count := r.bundle.Count(slot)
		for {
			select {
			case <-stop:
				return errors.New("pending recovery stopped")
			default:
			}
			settings := c.settings.Load().(*cacheSettings)
			// Reserve half the configured cache for in-flight work. One large metric
			// can always progress once the cache has drained.
			if settings.maxSize <= 0 || c.Size() == 0 || c.Size()+int64(count) <= settings.maxSize/2 {
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
		if err = c.claimPendingMetric(r, name, slot); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) claimPendingMetric(r *pendingRecovery, name string, slot uint64) error {
	shard := c.GetShard(name)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if r.isClaimed(slot) {
		return nil
	}
	// No caller may append live points until all saved history is persisted.
	// Refuse an invalid orchestration instead of silently changing precedence.
	if _, exists := shard.items[name]; exists {
		return fmt.Errorf("metric %q was written before pending recovery", name)
	}
	p, err := r.bundle.Read(slot)
	if err != nil {
		return err
	}
	if p == nil || p.Metric != name {
		return errors.New("pending recovery metric identity differs")
	}
	shard.items[name] = p
	atomic.AddInt64(&c.stat.size, int64(len(p.Data)))
	r.claim(slot)
	c.writeoutQueue.notifyAt(time.Now())
	return nil
}

// FinishPendingRecovery is called after every saved metric has been written (or
// handled by the existing invalid-metric policy) and before opening receivers.
// A crash before retirement can safely replay the unchanged legacy sources:
// no newer live values have been allowed to reach disk.
func (c *Cache) FinishPendingRecovery() error {
	r := c.pending.Load()
	if r == nil {
		return nil
	}
	if r.remaining.Load() != 0 || !c.IsEmpty() {
		return errors.New("pending recovery has not drained")
	}
	if err := r.bundle.Retire(); err != nil {
		return err
	}
	c.pending.CompareAndSwap(r, nil)
	return nil
}
