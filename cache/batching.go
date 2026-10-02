package cache

import (
	"fmt"
	"time"

	"github.com/go-graphite/go-carbon/points"
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

func (c *Cache) batchWrites(s *cacheSettings) bool {
	return s.batching() && !s.underPressure(c.Size())
}

func (s *cacheSettings) underPressure(size int64) bool {
	// Divide first to avoid overflowing a large configured capacity.
	return s.maxSize > 0 && size > s.maxSize/4*3+s.maxSize%4*3/4
}

// Called with the shard locked. Zero means immediately eligible.
func writeoutDeadline(shard *Shard, p *points.Points, s *cacheSettings) time.Time {
	first := shard.firstArrival[p]
	if len(p.Data) >= s.writeoutMinPoints || first.IsZero() {
		return time.Time{}
	}
	return first.Add(s.writeoutMaxDelay)
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
	p, exists := shard.items[metric]
	ready := exists && !time.Now().Before(writeoutDeadline(shard, p, s))
	shard.mu.RUnlock()
	return ready
}
