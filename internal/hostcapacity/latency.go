package hostcapacity

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// Latency records bounded-memory read latency independently of Prometheus.
// Buckets are noncumulative upper bounds, 1us doubling; the last is overflow.
type Latency struct{ buckets [32]atomic.Uint64 }
type LatencySnapshot [32]uint64

func (h *Latency) Observe(d time.Duration) {
	us := uint64(max(time.Duration(1), d/time.Microsecond))
	index := min(31, bits.Len64(us-1))
	h.buckets[index].Add(1)
}

func (h *Latency) Snapshot() (s LatencySnapshot) {
	for i := range s {
		s[i] = h.buckets[i].Load()
	}
	return
}

// P99Since takes an interval, never a process-lifetime percentile. Counter
// resets discard the interval rather than underflowing into false pressure.
func (s LatencySnapshot) P99Since(previous LatencySnapshot) (p99 time.Duration, count uint64) {
	for i := range s {
		if s[i] < previous[i] {
			return 0, 0
		}
		count += s[i] - previous[i]
	}
	if count == 0 {
		return 0, 0
	}
	target := (count*99 + 99) / 100
	var cumulative uint64
	for i := range s {
		cumulative += s[i] - previous[i]
		if cumulative >= target {
			return time.Microsecond * time.Duration(uint64(1)<<i), count
		}
	}
	return
}
