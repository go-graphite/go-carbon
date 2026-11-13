package chunkstore

import (
	"sync/atomic"
	"time"
)

// Timings are cumulative worker time, not elapsed wall time. They isolate the
// owner-side stage from donor export and controller/RPC scheduling.
type transferApplyCounters struct {
	reserve, build, ingest, wait, hold, maxHold, prepare, retire, sync atomic.Uint64
	splits, epochValidated                                             atomic.Uint64
	stripeHolds                                                        [17]atomic.Uint64
}

type TransferApplyStats struct {
	SplitGroups uint64
	// EpochValidatedNames counts commits that skipped the catalog re-read.
	EpochValidatedNames                                                                                                                           uint64
	StripeHolds                                                                                                                                   [17]uint64
	ReserveSeconds, BuildSeconds, IngestSeconds, LockWaitSeconds, LockHoldSeconds, MaxLockHoldSeconds, PrepareSeconds, RetireSeconds, SyncSeconds float64
}

func (s *Store) TransferApplyStats() TransferApplyStats {
	c := &s.transferApply
	sec := func(v *atomic.Uint64) float64 { return time.Duration(v.Load()).Seconds() }
	out := TransferApplyStats{ReserveSeconds: sec(&c.reserve), BuildSeconds: sec(&c.build), IngestSeconds: sec(&c.ingest), LockWaitSeconds: sec(&c.wait), LockHoldSeconds: sec(&c.hold), MaxLockHoldSeconds: sec(&c.maxHold), PrepareSeconds: sec(&c.prepare), RetireSeconds: sec(&c.retire), SyncSeconds: sec(&c.sync), SplitGroups: c.splits.Load(), EpochValidatedNames: c.epochValidated.Load()}
	for i := range out.StripeHolds {
		out.StripeHolds[i] = c.stripeHolds[i].Load()
	}
	return out
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
