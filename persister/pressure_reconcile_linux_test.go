package persister

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type pressureReconciliation struct {
	metrics, points, failures atomic.Uint64
	group                     sync.WaitGroup
}

// The separate reconciler only captures/reads originals. Its destination is the
// private database, while the foreground writer's root is unused-files. It
// shares the foreground metric exclusion so verification cannot race updates.
// Replacement may supersede replayed private values with current production
// values: this is a pressure diagnosis, not the full-history write oracle.
func startPressureReconciliation(t *testing.T, ctx context.Context, p *Whisper, db *store.Store, output string) *pressureReconciliation {
	t.Helper()
	r := new(pressureReconciliation)
	source := os.Getenv("GRAPHITE_PRESSURE_RECONCILE_SOURCE")
	if source == "" {
		return r
	}
	if filepath.Clean(source) != "/var/lib/carbon/whisper" {
		t.Fatal("reconciliation diagnostic requires the existing original tree")
	}
	limit, err := db.CatalogHighWatermark(ctx)
	if err != nil {
		t.Fatal(err)
	}
	background := NewWhisper(source, p.schemas, p.aggregation, nil, nil, nil, nil)
	background.SetMetricStore(db)
	background.SetMirrorFiles(true)
	background.SetMirrorState(&MirrorState{ScratchDir: output, BaselineCopy: filepath.Join(filepath.Dir(source), "storage-exact-canary-20261019/full-baseline/cwhisper-copy"), BaselineIDLimit: limit})
	jobs := make(chan string, 32)
	for range 16 {
		r.group.Go(func() {
			for name := range jobs {
				if ctx.Err() != nil {
					continue
				}
				mutex := &p.storeMutex[storeMutexIndex(name)]
				locked := false
				for ctx.Err() == nil {
					if mutex.TryLock() {
						locked = true
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if !locked {
					continue
				}
				_, count, e := background.ReconcileMirrorMetric(ctx, name, 0)
				mutex.Unlock()
				if e != nil {
					if !errors.Is(e, context.Canceled) {
						r.failures.Add(1)
					}
					continue
				}
				r.metrics.Add(1)
				r.points.Add(uint64(count))
			}
		})
	}
	r.group.Go(func() {
		defer close(jobs)
		err := store.EachPage(ctx, db.ListPage, "", 1000, func(page []store.Metadata) error {
			for _, m := range page {
				if m.ID > limit {
					continue
				}
				select {
				case jobs <- m.Name:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			r.failures.Add(1)
		}
	})
	return r
}
