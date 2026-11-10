package chunkstore

import (
	"sync"
	"time"
)

type rangeCacheSnapshot struct{ bytes, hits, misses uint64 }

func (s *Store) rangeCacheSnapshot() (out rangeCacheSnapshot) {
	if s.rangeDelCache != nil {
		m := s.rangeDelCache.Metrics()
		out = rangeCacheSnapshot{uint64(m.Size), uint64(m.Hits), uint64(m.Misses)}
	}
	return
}

// TelemetryStats never enters Pebble or waits on an application lock. Backend
// gauges come from the existing one-second background sample; application
// counters are live. Skip an observation if a control/statistics lock is busy.
// No mutation stripe, filesystem operation or unbounded callback occurs here.
func (s *Store) TelemetryStats() (Stats, SidecarInventory, bool) {
	m := s.telemetryPebble.Load()
	if m == nil {
		return Stats{}, SidecarInventory{}, false
	}
	c := s.compactionControl
	locks := []*sync.Mutex{&c.mu, &c.pacer.mu, &c.flushPacer.mu, &s.stallMu,
		&s.coldPack.mu, &s.relayout.guard.mu, &s.relayout.reclaim.mu, &s.sidecars.mu}
	for i, mu := range locks {
		if !mu.TryLock() {
			for j := i - 1; j >= 0; j-- {
				locks[j].Unlock()
			}
			return Stats{}, SidecarInventory{}, false
		}
	}
	pacing := c.statsLocked(c.pacer.rate, c.flushPacer.rate)
	cold, relayout := s.coldPack.stats, s.relayoutAtomicStats()
	s.relayout.guard.statsLocked(&relayout)
	s.relayout.reclaim.statsLocked(&relayout)
	stalls, duration, reason := s.stallCount.Load(), s.stallDuration, s.stallReason
	active := !s.stallSince.IsZero()
	if active {
		duration += time.Since(s.stallSince)
	}
	inv := SidecarInventory{Count: uint64(len(s.sidecars.entries)), Points: s.sidecars.points, Ready: s.sidecars.ready}
	if len(s.sidecars.oldest) > 0 {
		inv.Oldest = s.sidecars.oldest[0].Oldest
	}
	for i := len(locks) - 1; i >= 0; i-- {
		locks[i].Unlock()
	}
	var r rangeCacheSnapshot
	if snapshot := s.telemetryRange.Load(); snapshot != nil {
		r = *snapshot
	}
	return s.statsFrom(m, pacing, cold, relayout, stalls, duration, reason, active, r), inv, true
}

// WriteStalls reports the live cumulative stall count without waiting for
// optional telemetry/control locks or entering Pebble.
func (s *Store) WriteStalls() uint64 { return s.stallCount.Load() }

// TelemetryAge makes a stalled background observation visible without making
// collectors wait for it. Counters read from atomics remain current.
func (s *Store) TelemetryAge() time.Duration {
	return max(0, time.Since(time.Unix(0, s.telemetryAt.Load())))
}
