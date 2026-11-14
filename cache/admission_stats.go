package cache

import "github.com/go-graphite/go-carbon/points"

// AdmissionStats counts points rejected before entering the cache. These totals
// survive Stat calls; quota telemetry can describe attempted enforcement and
// batch errors, whereas these counters count actual rejected points once.
// Name-limit refusals are available separately through NameTooLongRejectedPoints.
type AdmissionStats struct {
	Throttled, Overflow, InvalidTags, XlogWriteFailed uint64
}

func (c *Cache) AdmissionStats() AdmissionStats {
	return AdmissionStats{
		Throttled: c.stat.throttledPoints.Load(), Overflow: c.stat.overflowPoints.Load(),
		InvalidTags: c.stat.invalidTagPoints.Load(), XlogWriteFailed: c.stat.xlogFailedPoints.Load(),
	}
}

// divert keeps a failed diversion distinct from successful handoff buffering.
// A failed write may have written a prefix; count all affected points rather
// than claiming that none of the batch reached the diversion destination.
func (c *Cache) divert(write func(*points.Points) error, p *points.Points) {
	if err := write(p); err != nil {
		c.stat.xlogFailedPoints.Add(uint64(len(p.Data)))
	}
}
