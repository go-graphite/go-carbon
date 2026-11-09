package chunkstore

import (
	"sync/atomic"
	"time"
)

// Timings are cumulative worker time, not elapsed wall time. They isolate the
// owner-side stage from donor export and controller/RPC scheduling.
type transferApplyCounters struct {
	reserve, build, ingest, wait, hold, maxHold, prepare, retire, sync atomic.Uint64
}

type TransferApplyStats struct {
	ReserveSeconds, BuildSeconds, IngestSeconds, LockWaitSeconds, LockHoldSeconds, MaxLockHoldSeconds, PrepareSeconds, RetireSeconds, SyncSeconds float64
}

func (s *Store) TransferApplyStats() TransferApplyStats {
	c := &s.transferApply
	sec := func(v *atomic.Uint64) float64 { return time.Duration(v.Load()).Seconds() }
	return TransferApplyStats{sec(&c.reserve), sec(&c.build), sec(&c.ingest), sec(&c.wait), sec(&c.hold), sec(&c.maxHold), sec(&c.prepare), sec(&c.retire), sec(&c.sync)}
}

func (c *transferApplyCounters) recordHold(d time.Duration) {
	n := uint64(d)
	c.hold.Add(n)
	for old := c.maxHold.Load(); n > old; old = c.maxHold.Load() {
		if c.maxHold.CompareAndSwap(old, n) {
			break
		}
	}
}
