package downsize

import (
	"context"
	"sync"
	"time"
)

// Shared across all metric/native copy phases and streams. Completed batches
// may burst; pacing bounds the aggregate long-run copy rate without holding
// remote transfer slots idle. No limit is imposed by default.
type syncRateLimit struct {
	mu             sync.Mutex
	bytesPerSecond int64
	next           time.Time
}

func (r *syncRateLimit) wait(ctx context.Context, bytes int64) error {
	if r == nil || r.bytesPerSecond == 0 || bytes <= 0 {
		return nil
	}
	r.mu.Lock()
	now := time.Now()
	if r.next.Before(now) {
		r.next = now
	}
	r.next = r.next.Add(time.Duration(float64(bytes) / float64(r.bytesPerSecond) * float64(time.Second)))
	delay := r.next.Sub(now)
	r.mu.Unlock()
	return sleepContext(ctx, delay)
}
