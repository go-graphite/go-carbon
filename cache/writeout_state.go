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

// WriteoutState samples arrivals under one shard lock at a time. Call it at
// controller frequency, never per update. Ages measure residence, not sample
// timestamps, which may legitimately contain historical or future data.
func (c *Cache) WriteoutState() WriteoutState {
	now := time.Now()
	s := c.settings.Load().(*cacheSettings)
	state := WriteoutState{
		MaxPoints: s.maxSize, MaxDelay: s.writeoutMaxDelay,
		MeanDelay: time.Duration(float64(s.writeoutMaxDelay) * (1 - s.writeoutSpread/2)),
		Pressure:  c.bytePressure(s), Recovering: c.recovering(s),
	}
	for _, shard := range c.data {
		shard.mu.RLock()
		state.Metrics += int64(shard.items.len())
		for _, it := range shard.items.all {
			state.Points += int64(it.count())
			if !now.Before(writeoutDeadline(it, s)) {
				state.Eligible++
			}
			if !it.firstArrival.IsZero() {
				state.OldestAge = max(state.OldestAge, now.Sub(s.writeoutWall(it.firstArrival)))
			}
		}
		shard.mu.RUnlock()
	}
	return state
}
