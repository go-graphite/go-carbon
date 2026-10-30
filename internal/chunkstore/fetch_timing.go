package chunkstore

import (
	"sync/atomic"
	"time"
)

// FetchTiming is cumulative wall time for one stage of storage fetches.
type FetchTiming struct {
	Calls       uint64
	Nanoseconds uint64
}

// FetchTimings separates the fetch body from its final read-view release.
// IteratorClose covers only the final shared continuation iterator, when used,
// and is a subset of Body. Other short-lived iterator closes remain in Body.
// Fetches rejected before opening a read view are not counted.
type FetchTimings struct {
	Body, ReadViewClose, IteratorClose FetchTiming
}

type fetchTimingCounter struct {
	calls, nanos atomic.Uint64
}

func (c *fetchTimingCounter) add(elapsed time.Duration) {
	c.nanos.Add(uint64(elapsed))
	c.calls.Add(1)
}

func (c *fetchTimingCounter) snapshot() FetchTiming {
	return FetchTiming{Calls: c.calls.Load(), Nanoseconds: c.nanos.Load()}
}

type fetchTimingCounters struct {
	body, readViewClose, iteratorClose fetchTimingCounter
}

// FetchTimings reads fetch timing counters without gathering database metrics.
func (s *Store) FetchTimings() FetchTimings {
	return FetchTimings{
		Body: s.fetchTimings.body.snapshot(), ReadViewClose: s.fetchTimings.readViewClose.snapshot(),
		IteratorClose: s.fetchTimings.iteratorClose.snapshot(),
	}
}
