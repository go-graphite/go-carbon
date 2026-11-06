package chunkstore

import (
	"math/bits"
	"sync/atomic"
	"time"
)

type nativeCounters struct {
	publishSync                                                      atomic.Uint64
	ranges, copied, deferred, wire, sst, errors                      atomic.Uint64
	reserve, build, ingest, publishWait, publishHold, maxPublishHold atomic.Uint64
	combineRetries, combineWait, combineHold, combineMaxHold         atomic.Uint64
	combineHolds                                                     [64]atomic.Uint64
	combineCopiedBlocks, combinePointBlocks, combineMaterialized     atomic.Uint64
}
type NativeStats struct {
	PublishSyncSeconds                                                                                         float64
	Ranges, CopiedNames, DeferredNames, WireBytes, SSTBytes, Errors                                            uint64
	ReserveSeconds, BuildSeconds, IngestSeconds, PublishWaitSeconds, PublishHoldSeconds, MaxPublishHoldSeconds float64
	CombineRetries                                                                                             uint64
	CombineWaitSeconds, CombineHoldSeconds, CombineMaxHoldSeconds, CombineP99HoldSeconds                       float64
	CombineCopiedBlocks, CombinePointBlocks, CombineMaterializedArchives                                       uint64
}

func (c *nativeCounters) recordCombineHold(d time.Duration) {
	n := uint64(d)
	c.combineHold.Add(n)
	c.combineHolds[min(bits.Len64(n), 63)].Add(1)
	for old := c.combineMaxHold.Load(); n > old; old = c.combineMaxHold.Load() {
		if c.combineMaxHold.CompareAndSwap(old, n) {
			break
		}
	}
}

func (s *Store) NativeStats() NativeStats {
	c := &s.nativeCounters
	seconds := func(n uint64) float64 { return float64(n) / float64(time.Second) }
	var total uint64
	for i := range c.combineHolds {
		total += c.combineHolds[i].Load()
	}
	var cumulative, p99 uint64
	for i := range c.combineHolds {
		cumulative += c.combineHolds[i].Load()
		if total > 0 && cumulative >= total-total/100 {
			p99 = uint64(1) << uint(i)
			break
		}
	}
	return NativeStats{PublishSyncSeconds: seconds(c.publishSync.Load()), Ranges: c.ranges.Load(), CopiedNames: c.copied.Load(), DeferredNames: c.deferred.Load(), WireBytes: c.wire.Load(), SSTBytes: c.sst.Load(), Errors: c.errors.Load(), ReserveSeconds: seconds(c.reserve.Load()), BuildSeconds: seconds(c.build.Load()), IngestSeconds: seconds(c.ingest.Load()), PublishWaitSeconds: seconds(c.publishWait.Load()), PublishHoldSeconds: seconds(c.publishHold.Load()), MaxPublishHoldSeconds: seconds(c.maxPublishHold.Load()), CombineRetries: c.combineRetries.Load(), CombineWaitSeconds: seconds(c.combineWait.Load()), CombineHoldSeconds: seconds(c.combineHold.Load()), CombineMaxHoldSeconds: seconds(c.combineMaxHold.Load()), CombineP99HoldSeconds: seconds(p99), CombineCopiedBlocks: c.combineCopiedBlocks.Load(), CombinePointBlocks: c.combinePointBlocks.Load(), CombineMaterializedArchives: c.combineMaterialized.Load()}
}
