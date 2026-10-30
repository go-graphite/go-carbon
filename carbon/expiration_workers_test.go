package carbon

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

type parallelExpirationStore struct {
	expirationStore
	entered      chan []store.Metadata
	release      <-chan struct{}
	stalled      atomic.Bool
	active, peak atomic.Int64
}

func (s *parallelExpirationStore) ExpirationPressure() (int64, uint64, bool) {
	return 0, 0, s.stalled.Load()
}

func (s *parallelExpirationStore) DeleteBatchIfUnchanged(ctx context.Context, batch []store.Metadata) ([]error, error) {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for peak := s.peak.Load(); active > peak && !s.peak.CompareAndSwap(peak, active); peak = s.peak.Load() {
	}
	select {
	case s.entered <- batch:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.expirationStore.(interface {
		DeleteBatchIfUnchanged(context.Context, []store.Metadata) ([]error, error)
	}).DeleteBatchIfUnchanged(ctx, batch)
}

func TestExpirationParallelPressureStopsAdmission(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	const count = 7 * expirationDeleteBatchSize
	for i := range count {
		createExpirationMetric(t, db, fmt.Sprintf("jobs.%04d", i))
	}
	now = now.Add(2 * time.Hour)
	release := make(chan struct{})
	hooks := &parallelExpirationStore{expirationStore: db, entered: make(chan []store.Metadata, 4), release: release}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour, rate: 1_000_000_000, deleteWorkers: 4, stats: &expirationStats{}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.sweep(ctx) }()
	for range 4 {
		select {
		case <-hooks.entered:
		case <-ctx.Done():
			t.Fatal("delete workers did not overlap")
		}
	}
	hooks.stalled.Store(true)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := r.stats.deleted.Load(); got != 4*expirationDeleteBatchSize {
		t.Fatalf("deleted=%d", got)
	}
	if got := r.stats.guarded.Load(); got != 3*expirationDeleteBatchSize {
		t.Fatalf("guarded=%d", got)
	}
	if hooks.peak.Load() != 4 || hooks.active.Load() != 0 {
		t.Fatal("unbounded or unjoined workers")
	}
	if len(hooks.entered) != 0 {
		t.Fatal("pressure admitted another delete batch")
	}
}

func TestExpirationParallelPendingConflictsAndLimit(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	const count = 7 * expirationDeleteBatchSize
	for i := range count {
		createExpirationMetric(t, db, fmt.Sprintf("jobs.%04d", i))
	}
	now = now.Add(2 * time.Hour)
	core := cache.New()
	release := make(chan struct{})
	hooks := &parallelExpirationStore{expirationStore: db, entered: make(chan []store.Metadata, 8), release: release}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour, rate: 1_000_000_000,
		deleteWorkers: 4, maxDeletes: 4*expirationDeleteBatchSize + 88, pending: core.Has, stats: &expirationStats{},
		rules: expirationTestRules(t, "[jobs]\npattern = ^jobs\\.\nexpiration = 1h\n"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.sweep(ctx) }()
	var written string
	for i := range 4 {
		select {
		case batch := <-hooks.entered:
			if i == 0 {
				written = batch[0].Name
			}
		case <-ctx.Done():
			t.Fatal("workers did not overlap")
		}
	}
	// The next batch is waiting for a worker. Revalidate both an already
	// scanned cache arrival and a committed write before deleting its snapshot.
	pending := fmt.Sprintf("jobs.%04d", 4*expirationDeleteBatchSize)
	core.Add(points.OnePoint(pending, 42, now.Unix()))
	if err := db.UpdateMany(ctx, written, []store.Point{{Timestamp: now.Unix(), Value: 7}}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if r.stats.deleted.Load() != 4*expirationDeleteBatchSize+88 || r.stats.conflicts.Load() != 1 || r.stats.pending.Load() != 1 || r.stats.limited.Load() != count-(4*expirationDeleteBatchSize+90) {
		t.Fatalf("deleted=%d conflicts=%d pending=%d limited=%d", r.stats.deleted.Load(), r.stats.conflicts.Load(), r.stats.pending.Load(), r.stats.limited.Load())
	}
	rule := r.ruleStats[1]
	if rule.scanned.Load() != count || rule.expired.Load() != 4*expirationDeleteBatchSize+88 || rule.kept.Load() != 2 {
		t.Fatal("inexact rule counters")
	}
	for _, name := range []string{written, pending} {
		if _, err := db.Metadata(ctx, name); err != nil {
			t.Fatalf("deleted active metric %s: %v", name, err)
		}
	}
	remaining, err := db.List(ctx, "")
	if err != nil || len(remaining) != count-(4*expirationDeleteBatchSize+88) {
		t.Fatalf("remaining=%d: %v", len(remaining), err)
	}
}

func TestExpirationParallelCancellationJoinsWorkers(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	for i := range 2 * expirationDeleteBatchSize {
		createExpirationMetric(t, db, fmt.Sprintf("jobs.%04d", i))
	}
	now = now.Add(2 * time.Hour)
	hooks := &parallelExpirationStore{expirationStore: db, entered: make(chan []store.Metadata, 2), release: make(chan struct{})}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour, rate: 1_000_000_000, deleteWorkers: 2, stats: &expirationStats{}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.sweep(ctx) }()
	for range 2 {
		select {
		case <-hooks.entered:
		case <-ctx.Done():
			t.Fatal("workers did not start")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if hooks.active.Load() != 0 || r.stats.deleted.Load() != 0 || r.stats.lastSuccess.Load() != 0 {
		t.Fatal("cancel did not join workers")
	}
}

func TestExpirationParallelSharesBudgetAndChecksPressureAfterPacing(t *testing.T) {
	for _, paused := range []bool{false, true} {
		t.Run(fmt.Sprintf("paused=%v", paused), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Now().Add(-2 * time.Hour)
				db := expirationTestDB(t, &now)
				for i := range 4 * expirationDeleteBatchSize {
					createExpirationMetric(t, db, fmt.Sprintf("jobs.%04d", i))
				}
				now = now.Add(2 * time.Hour)
				release := make(chan struct{})
				close(release)
				hooks := &parallelExpirationStore{expirationStore: db, entered: make(chan []store.Metadata, 4), release: release}
				r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour, rate: 1_000_000_000, deleteRate: expirationDeleteBatchSize, deleteWorkers: 4, stats: &expirationStats{}}
				started := time.Now()
				done := make(chan error, 1)
				go func() { done <- r.sweep(context.Background()) }()
				synctest.Wait()
				hooks.stalled.Store(paused)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if paused {
					if r.stats.deleted.Load() != 0 || r.stats.guarded.Load() != 4*expirationDeleteBatchSize {
						t.Fatal("deleted despite pressure after pacing")
					}
				} else if elapsed := time.Since(started); elapsed < 4*time.Second || elapsed > 4*time.Second+time.Millisecond {
					t.Fatalf("workers multiplied the global budget: %s", elapsed)
				}
			})
		})
	}
}
