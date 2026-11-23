package downsize

import (
	"context"
	"sync"
	"time"
)

// syncRateFloor is the slowest cache-throttled copy rate.
const syncRateFloor = 1 << 20

// Shared across all metric/native copy phases and streams. Completed batches
// may burst; pacing bounds the aggregate long-run copy rate without holding
// remote transfer slots idle. No limit is imposed by default.
//
// The cache governor (sync_cache_governor.go) lowers the rate below the
// operator ceiling, or pauses it, while a store's write cache is filling.
type syncRateLimit struct {
	mu             sync.Mutex
	bytesPerSecond int64 // operator ceiling; 0 unlimited
	next           time.Time

	throttled   int64         // cache-governed rate; 0 means the ceiling applies
	unthrottled float64       // observed rate when throttling began
	paused      chan struct{} // non-nil while paused; closed on resume
	copied      int64         // bytes since the last observation
	since       time.Time
	reason      string // why the governor is limiting; empty when it is not
}

func (r *syncRateLimit) wait(ctx context.Context, bytes int64) error {
	if r == nil || bytes <= 0 {
		return nil
	}
	r.mu.Lock()
	for r.paused != nil {
		paused := r.paused
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-paused:
		}
		r.mu.Lock()
	}
	r.copied += bytes
	rate := r.bytesPerSecond
	if r.throttled > 0 && (rate == 0 || r.throttled < rate) {
		rate = r.throttled
	}
	if rate == 0 {
		r.mu.Unlock()
		return nil
	}
	now := time.Now()
	if r.next.Before(now) {
		r.next = now
	}
	r.next = r.next.Add(time.Duration(float64(bytes) / float64(rate) * float64(time.Second)))
	delay := r.next.Sub(now)
	r.mu.Unlock()
	return sleepContext(ctx, delay)
}

// limiter reports the cache guard's reason and whether it has paused the copy.
func (r *syncRateLimit) limiter() (reason string, paused bool) {
	if r == nil {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reason, r.paused != nil
}

// observed returns the copy rate since the previous call. Caller holds r.mu.
func (r *syncRateLimit) observed() float64 {
	now := time.Now()
	rate := 0.0
	if seconds := now.Sub(r.since).Seconds(); !r.since.IsZero() && seconds > 0 {
		rate = float64(r.copied) / seconds
	}
	r.copied, r.since = 0, now
	return rate
}

// throttle halves the rate, and with pause also blocks further batches. A
// copy that is already paused is not slowed further: it measures nothing.
func (r *syncRateLimit) throttle(pause bool, reason string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	observed := r.observed()
	if r.paused == nil {
		base := float64(r.throttled)
		if base == 0 {
			base, r.unthrottled = observed, observed
			if base == 0 {
				base = float64(r.bytesPerSecond)
			}
		}
		r.throttled = max(syncRateFloor, int64(base/2))
	}
	if pause && r.paused == nil {
		r.paused = make(chan struct{})
	}
	r.reason = reason
	return r.throttled
}

// relax resumes a paused copy and raises a throttled rate by a quarter, back
// to the ceiling once the limit no longer binds. It returns the governed rate.
func (r *syncRateLimit) relax() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observed()
	r.unpauseLocked()
	if r.throttled > 0 {
		r.throttled += r.throttled/4 + 1
		if r.bytesPerSecond > 0 && r.throttled >= r.bytesPerSecond || r.bytesPerSecond == 0 && float64(r.throttled) >= r.unthrottled {
			r.throttled = 0
		}
	}
	r.reason = ""
	if r.throttled > 0 {
		r.reason = "cache pressure recovery"
	}
	return r.throttled
}

// unpause resumes a paused copy at its current rate.
func (r *syncRateLimit) unpause() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unpauseLocked()
}

func (r *syncRateLimit) unpauseLocked() {
	if r.paused != nil {
		close(r.paused)
		r.paused = nil
	}
}
