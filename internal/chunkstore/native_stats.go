package chunkstore

import (
	"sync/atomic"
	"time"
)

type nativeCounters struct {
	ranges, copied, deferred, wire, sst, errors                      atomic.Uint64
	reserve, build, ingest, publishWait, publishHold, maxPublishHold atomic.Uint64
	publishSync                                                      atomic.Uint64
}
type NativeStats struct {
	Ranges, CopiedNames, DeferredNames, WireBytes, SSTBytes, Errors                                            uint64
	ReserveSeconds, BuildSeconds, IngestSeconds, PublishWaitSeconds, PublishHoldSeconds, MaxPublishHoldSeconds float64
	PublishSyncSeconds                                                                                         float64
}

func (s *Store) NativeStats() NativeStats {
	c := &s.nativeCounters
	seconds := func(n uint64) float64 { return float64(n) / float64(time.Second) }
	return NativeStats{c.ranges.Load(), c.copied.Load(), c.deferred.Load(), c.wire.Load(), c.sst.Load(), c.errors.Load(), seconds(c.reserve.Load()), seconds(c.build.Load()), seconds(c.ingest.Load()), seconds(c.publishWait.Load()), seconds(c.publishHold.Load()), seconds(c.maxPublishHold.Load()), seconds(c.publishSync.Load())}
}
