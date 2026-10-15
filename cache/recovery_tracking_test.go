package cache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// Confirmation tracking controls when durable recovery sources may disappear.
// Concurrent retries must neither lose that ownership nor count it twice.
func TestPendingTrackingConcurrentRetries(t *testing.T) {
	var r pendingRecovery
	const metrics = 4096
	items := make([]*points.Points, metrics)
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Go(func() {
			for i := worker; i < metrics; i += 16 {
				items[i] = points.OnePoint(fmt.Sprintf("recover.metric.%d", i), 1, 1)
				r.track(items[i])
			}
		})
	}
	wg.Wait()
	if got := r.outstanding(); got != metrics {
		t.Fatalf("tracked %d metrics, want %d", got, metrics)
	}
	for worker := range 16 {
		wg.Go(func() {
			for i := worker; i < metrics; i += 16 {
				p := points.OnePoint(items[i].Metric, 2, 1)
				r.moved(items[i], p)
				items[i] = p
				for range pendingWriteAttempts - 1 {
					r.failed(p)
				}
			}
		})
	}
	wg.Wait()
	if got := r.outstanding(); got != metrics {
		t.Fatalf("retries released recovery ownership: %d", got)
	}
	for worker := range 16 {
		wg.Go(func() {
			for i := worker; i < metrics; i += 16 {
				r.persisted(items[i])
			}
		})
	}
	wg.Wait()
	if got := r.outstanding(); got != 0 {
		t.Fatalf("confirmed metrics retained: %d", got)
	}
}
