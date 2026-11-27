package v2read

import (
	"sync/atomic"
	"time"
)

// ReadBounds are seconds per FetchMany call, including failed/cancelled calls
// and the consumer callback. They are fixed to keep telemetry memory bounded.
func ReadBounds() []float64 { return []float64{.001, .005, .01, .05, .1, .5, 1, 5, 30} }

type readCounters struct {
	count, nanoseconds atomic.Uint64
	buckets            [9]atomic.Uint64
}

func (s *readCounters) observe(d time.Duration) {
	for i, bound := range ReadBounds() {
		if d.Seconds() <= bound {
			s.buckets[i].Add(1)
		}
	}
	s.nanoseconds.Add(uint64(d))
	s.count.Add(1)
}

// ReadObservation reports cumulative histogram buckets. Collection is an
// approximate concurrent view, never a read or durability frontier.
func (r *Reader) ReadObservation() (uint64, float64, map[float64]uint64) {
	count := r.reads.count.Load()
	buckets := make(map[float64]uint64, 9)
	var previous uint64
	for i, bound := range ReadBounds() {
		previous = max(previous, min(count, r.reads.buckets[i].Load()))
		buckets[bound] = previous
	}
	return count, float64(r.reads.nanoseconds.Load()) / 1e9, buckets
}
