package cache

import (
	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
)

// AdmissionStats counts input sources, admission outcomes and unfinished writes.
// Totals survive Stat calls. Quota telemetry may describe attempted enforcement;
// Throttled counts actual rejected points once.
// Name-limit refusals are available separately through NameTooLongRejectedPoints.
type AdmissionStats struct {
	Throttled, Overflow, InvalidTags, XlogWriteFailed uint64
	Received, Recovered, XlogDiverted                 uint64
	InFlight                                          int64
}

func (c *Cache) AdmissionStats() AdmissionStats {
	s := AdmissionStats{
		Received: c.stat.receivedPoints.Load(), Recovered: c.stat.recoveredPoints.Load(),
		XlogDiverted: c.stat.xlogDivertedPoints.Load(),
		InvalidTags:  c.stat.invalidTagPoints.Load(), XlogWriteFailed: c.stat.xlogFailedPoints.Load(),
	}
	for _, shard := range c.data {
		// Flush partial publication when the shard is available. A busy shard
		// contributes its last published total, at most 63 accepted points old.
		// Publish while still read-locked so concurrent scrapes cannot regress it.
		if shard.mu.TryRLock() {
			shard.livePointsPublished.Store(shard.livePoints)
			shard.mu.RUnlock()
		}
		s.Received += shard.livePointsPublished.Load() + shard.receivedPoints.Load()
		s.Recovered += shard.recoveredPoints.Load()
		s.InFlight += shard.inFlightPoints.Load()
		s.Throttled += shard.throttledPoints.Load()
		s.Overflow += shard.overflowPoints.Load()
	}
	return s
}

// divert keeps a failed diversion distinct from successful handoff buffering.
// A failed write may have written a prefix; count all affected points rather
// than claiming that none of the batch reached the diversion destination.
func (c *Cache) divert(write func(*points.Points) error, p *points.Points) {
	if err := write(p); err != nil {
		c.stat.xlogFailedPoints.Add(uint64(len(p.Data)))
	} else {
		c.stat.xlogDivertedPoints.Add(uint64(len(p.Data)))
	}
}

// admissionStat publishes the same cumulative point totals as Prometheus.
func (c *Cache) admissionStat(send helper.StatCallback) {
	s := c.AdmissionStats()
	send("received_points_total", float64(s.Received))
	send("recovered_points_total", float64(s.Recovered))
	send("in_flight_points", float64(s.InFlight))
	send("throttled_points_total", float64(s.Throttled))
	send("overflow_dropped_points_total", float64(s.Overflow))
	send("invalid_tag_rejected_points_total", float64(s.InvalidTags))
	send("xlog_write_failed_points_total", float64(s.XlogWriteFailed))
	send("xlog_diverted_points_total", float64(s.XlogDiverted))
	send("name_too_long_rejected_points_total", float64(c.NameTooLongRejectedPoints()))
}

// recordUnshardedInput covers refusals/diversions before shard resolution.
func (c *Cache) recordUnshardedInput(p *points.Points, recovered bool) {
	if recovered {
		c.stat.recoveredPoints.Add(uint64(len(p.Data)))
	} else {
		c.stat.receivedPoints.Add(uint64(len(p.Data)))
	}
}
