package v2core

import "time"

// Observation contains process-lifetime counters and current LSM gauges. It
// reads only atomics and Pebble's metrics; it never walks names or data parts.
// Parts are physical SST files, not logical rollup archives.
type Observation struct {
	CommittedPoints, Seals, SealNanoseconds, CompactionDebtBytes uint64
	CompactedBytes, ObsoleteBytes, LiveBytes, DispatchBytes      uint64
	// DurableSeals, SealNames and SealPoints give names and points per seal.
	DurableSeals, SealNames, SealPoints uint64
	BufferedPoints                      int64
	SealLagSeconds                      float64
	Parts                               [7]int64
}

func (e *coreEngine) Observe() Observation {
	var s Observation
	now := time.Now()
	for i := range e.routers {
		s.DispatchBytes += e.routers[i].bytes.Load()
	}
	for _, o := range e.owners {
		s.CommittedPoints += o.committed.Load()
		s.Seals += o.sealCalls.Load()
		s.DurableSeals += o.durableSeals.Load()
		s.SealNames += o.sealNames.Load()
		s.SealPoints += o.sealPoints.Load()
		s.SealNanoseconds += o.sealNS.Load()
		s.BufferedPoints += o.bufferedPoints.Load()
		if since := o.bufferedSince.Load(); since != 0 {
			s.SealLagSeconds = max(s.SealLagSeconds, now.Sub(time.Unix(0, since)).Seconds())
		}
		m := o.db.Metrics()
		s.CompactionDebtBytes += m.Compact.EstimatedDebt
		s.ObsoleteBytes += m.Table.ObsoleteSize
		for level, l := range m.Levels {
			s.Parts[level] += l.NumFiles
			s.LiveBytes += uint64(l.Size)
			s.CompactedBytes += l.BytesCompacted
		}
	}
	return s
}
