package cache

import "time"

// WriteoutState is a sampled view of existing batching and capacity state.
// It needs no per-metric history and remains useful with the input log disabled.
type WriteoutState struct {
	Metrics, Eligible              int64
	Points, MaxPoints              int64
	MaxDelay, MeanDelay, OldestAge time.Duration
	Pressure, Recovering           bool
}

// WriteoutState queries per-shard counts without visiting cached metrics.
// Eligibility is delayed by less than a second; oldest residence age is
// overestimated by less than a second. Point and metric totals are exact under
// each shard lock. Ages refer to first arrival, never sample timestamps.
// A batching-settings reload rebuilds each shard's indexes once, on its next
// sample. Unrelated settings and startup-clock release need no rebuild.
func (c *Cache) WriteoutState() WriteoutState {
	return c.writeoutStateAt(time.Now())
}

func (c *Cache) writeoutStateAt(now time.Time) WriteoutState {
	c.writeoutStateMu.Lock()
	defer c.writeoutStateMu.Unlock()
	s := c.settings.Load().(*cacheSettings)
	state := WriteoutState{
		MaxPoints: s.maxSize, MaxDelay: s.writeoutMaxDelay,
		MeanDelay: time.Duration(float64(s.writeoutMaxDelay) * (1 - s.writeoutSpread/2)),
		Pressure:  c.bytePressure(s), Recovering: c.recovering(s),
	}
	if !s.writeoutPhase.IsZero() {
		state.MeanDelay = s.writeoutMaxDelay
	}
	clockNow, held := s.writeoutTime(now)
	for _, shard := range c.data {
		for {
			shard.mu.RLock()
			if shard.writeout.settings == s {
				break
			}
			shard.mu.RUnlock()
			c.rebuildWriteout(shard, s)
		}
		state.Metrics += int64(shard.items.len())
		state.Points += shard.sizePublished + shard.sizeDelta
		w := &shard.writeout
		state.Eligible += w.immediate
		if !held {
			state.Eligible += w.deadlines.through(floorSecond(clockNow.Sub(c.writeoutEpoch)))
			if first, ok := w.arrivals.oldest(); ok {
				state.OldestAge = max(state.OldestAge, clockNow.Sub(c.writeoutEpoch.Add(secondDuration(first))))
			}
		}
		shard.mu.RUnlock()
	}
	return state
}

// WriteoutMaxDelay returns the live batching deadline without scanning metrics.
func (c *Cache) WriteoutMaxDelay() time.Duration {
	return c.settings.Load().(*cacheSettings).writeoutMaxDelay
}
