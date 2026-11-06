package hostcapacity

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// Latency records bounded-memory read latency independently of Prometheus.
// Eight buckets per power of two bound quantization error to 12.5%, rather
// than rounding a normal 66ms read into a 131ms observation. The last bucket
// contains values above 16 seconds. No request/metric labels are retained.
const latencyBuckets = 24*8 + 1

type Latency struct{ buckets [latencyBuckets]atomic.Uint64 }
type LatencySnapshot [latencyBuckets]uint64

func (h *Latency) Observe(d time.Duration) {
	d = max(time.Microsecond, d)
	exponent := bits.Len64(uint64(d/time.Microsecond)) - 1
	index := latencyBuckets - 1
	if exponent < 24 {
		base := time.Microsecond << exponent
		index = exponent*8 + int((d-base)*8/base)
	}
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
			if i == latencyBuckets-1 {
				return time.Microsecond << 24, count
			}
			base := time.Microsecond << (i / 8)
			return base + base*time.Duration(i%8+1)/8, count
		}
	}
	return
}
