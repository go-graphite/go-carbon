package persister

import (
	"slices"
	"time"
)

const capacityReadProbeWindow = 30 * time.Second

// One quiet-write interval per minute, expiring after an hour. Taking a median
// avoids an unusually cheap interval ratcheting the reference down forever.
// Read latency itself must NOT exclude a quiet sample: the reference has to
// follow unrelated workload/device changes upward as well as downward.
type capacityReadReference struct {
	samples [60]struct {
		at    time.Time
		value time.Duration
	}
	next int
	last time.Time
}

func (r *capacityReadReference) observe(now time.Time, p99 time.Duration) {
	if p99 <= 0 || !r.last.IsZero() && now.Sub(r.last) < time.Minute {
		return
	}
	r.samples[r.next].at, r.samples[r.next].value = now, max(time.Millisecond, p99)
	r.next = (r.next + 1) % len(r.samples)
	r.last = now
}

func (r *capacityReadReference) value(now time.Time) time.Duration {
	var values [60]time.Duration
	n := 0
	for _, s := range r.samples {
		if !s.at.IsZero() && now.Sub(s.at) >= 0 && now.Sub(s.at) < time.Hour {
			values[n] = s.value
			n++
		}
	}
	if n == 0 {
		return 0
	}
	slices.Sort(values[:n])
	return (values[(n-1)/2] + values[n/2]) / 2
}
