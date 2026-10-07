package carbon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

func expirationTestDB(t *testing.T, now *time.Time) *store.Store {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "store"), store.Options{
		Now: func() time.Time { return *now }, SyncInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return db
}

func createExpirationMetric(t *testing.T, db *store.Store, name string) store.Metadata {
	t.Helper()
	m, err := db.Create(context.Background(), store.MetricConfig{
		Name: name, Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Average,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func expirationTestRules(t *testing.T, text string) persister.WhisperExpirationRules {
	t.Helper()
	path := filepath.Join(t.TempDir(), "expiration.conf")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	rules, err := persister.ReadWhisperExpiration(path)
	if err != nil {
		t.Fatal(err)
	}
	return rules
}

func TestExpirationSweepRulesAndPages(t *testing.T) {
	now := time.Unix(100_000, 0)
	db := expirationTestDB(t, &now)
	const count = 2*expirationPageSize + 7
	for i := 0; i < count; i++ {
		createExpirationMetric(t, db, fmt.Sprintf("jobs.%04d", i))
	}
	createExpirationMetric(t, db, "jobs.keep.forever")
	createExpirationMetric(t, db, "unmatched")
	now = now.Add(time.Hour)
	createExpirationMetric(t, db, "jobs.recent")
	hooks := &expirationHooks{expirationStore: db}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, rate: 1_000_000, stats: &expirationStats{},
		rules: expirationTestRules(t, "[keep]\npattern = ^jobs\\.keep\\.\nexpiration = 0s\n[jobs]\npattern = ^jobs\\.\nexpiration = 1h\n"),
	}
	notifications := 0
	r.notify = func() { notifications++ }
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	remaining, err := db.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 3 || r.stats.deleted.Load() != count || r.stats.examined.Load() != count+3 {
		t.Fatalf("remaining=%v deleted=%d examined=%d", remaining, r.stats.deleted.Load(), r.stats.examined.Load())
	}
	if hooks.flushes != 1 || notifications == 0 || r.stats.lastSuccess.Load() != now.Unix() {
		t.Fatalf("flushes=%d notifications=%d last success=%d", hooks.flushes, notifications, r.stats.lastSuccess.Load())
	}
	// A second pass must not flush or notify when there are no deletions.
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hooks.flushes != 1 || r.stats.deleted.Load() != count {
		t.Fatalf("idle pass mutated store: flushes=%d deleted=%d", hooks.flushes, r.stats.deleted.Load())
	}
}

func TestExpirationBoundaryAndPendingWrites(t *testing.T) {
	now := time.Unix(100_000, 0)
	db := expirationTestDB(t, &now)
	m := createExpirationMetric(t, db, "metric")
	core := cache.New()
	r := &metricExpirer{db: db, now: func() time.Time { return now }, expiration: time.Hour,
		pending: core.Has, stats: &expirationStats{},
	}
	for _, delta := range []time.Duration{-time.Hour, time.Hour - time.Nanosecond} {
		now = m.LastUpdate.Add(delta)
		if deleted, err := r.expire(context.Background(), m); err != nil || deleted {
			t.Fatalf("expired before threshold (%s): deleted=%v error=%v", delta, deleted, err)
		}
	}
	now = m.LastUpdate.Add(time.Hour)
	core.Add(points.OnePoint(m.Name, 1, now.Unix()))
	if deleted, err := r.expire(context.Background(), m); err != nil || deleted {
		t.Fatalf("expired pending metric: deleted=%v error=%v", deleted, err)
	}
	inflight, ok := core.PopNotConfirmed(m.Name)
	if !ok {
		t.Fatal("pending metric missing")
	}
	if deleted, err := r.expire(context.Background(), m); err != nil || deleted {
		t.Fatalf("expired in-flight metric: deleted=%v error=%v", deleted, err)
	}
	core.Confirm(inflight)
	if deleted, err := r.expire(context.Background(), m); err != nil || !deleted {
		t.Fatalf("did not expire at threshold: deleted=%v error=%v", deleted, err)
	}
	if r.stats.pending.Load() != 2 {
		t.Fatalf("pending skips=%d", r.stats.pending.Load())
	}
}

func TestExpirationStaleCandidatePreservesNewWrite(t *testing.T) {
	now := time.Unix(100_000, 0)
	db := expirationTestDB(t, &now)
	m := createExpirationMetric(t, db, "metric")
	now = now.Add(time.Hour)
	hooks := &expirationHooks{expirationStore: db, beforeDelete: func() {
		if err := db.UpdateMany(context.Background(), m.Name, []store.Point{{Timestamp: now.Unix(), Value: 42}}); err != nil {
			t.Fatal(err)
		}
	}}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour, stats: &expirationStats{}}
	if deleted, err := r.expire(context.Background(), m); err != nil || deleted {
		t.Fatalf("stale candidate deleted: deleted=%v error=%v", deleted, err)
	}
	current, err := db.Metadata(context.Background(), m.Name)
	if err != nil || !current.LastUpdate.Equal(now) || r.stats.conflicts.Load() != 1 {
		t.Fatalf("new write missing: metadata=%+v error=%v conflicts=%d", current, err, r.stats.conflicts.Load())
	}
}

func TestExpirationCancelledSweepFlushesPartialDeletes(t *testing.T) {
	now := time.Unix(100_000, 0)
	db := expirationTestDB(t, &now)
	createExpirationMetric(t, db, "a")
	createExpirationMetric(t, db, "b")
	now = now.Add(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hooks := &expirationHooks{expirationStore: db, afterDelete: cancel}
	notified := false
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour,
		rate: 1_000_000, stats: &expirationStats{}, notify: func() { notified = true },
	}
	if err := r.sweep(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("sweep error=%v", err)
	}
	if hooks.flushes != 1 || r.stats.deleted.Load() != 1 || !notified || r.stats.lastSuccess.Load() != 0 || r.stats.errors.Load() != 0 {
		t.Fatalf("partial sweep: flushes=%d deleted=%d notified=%v", hooks.flushes, r.stats.deleted.Load(), notified)
	}
}

func TestExpirationSweepErrorIsReportedAndRetried(t *testing.T) {
	now := time.Unix(100_000, 0)
	db := expirationTestDB(t, &now)
	createExpirationMetric(t, db, "metric")
	now = now.Add(time.Hour)
	failure := errors.New("injected deletion failure")
	hooks := &expirationHooks{expirationStore: db, deleteErr: failure}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour, rate: 1_000_000, stats: &expirationStats{}}
	if err := r.sweep(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("sweep error=%v", err)
	}
	if r.stats.errors.Load() != 1 || r.stats.lastSuccess.Load() != 0 || hooks.flushes != 0 {
		t.Fatal("failed sweep reported success or flushed")
	}
	hooks.deleteErr = nil
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.stats.deleted.Load() != 1 || r.stats.lastSuccess.Load() == 0 {
		t.Fatal("retry did not complete")
	}
}

func TestExpirationStopInterruptsRateLimit(t *testing.T) {
	now := time.Unix(100_000, 0)
	db := expirationTestDB(t, &now)
	createExpirationMetric(t, db, "metric")
	listed := make(chan struct{})
	hooks := &expirationHooks{expirationStore: db, afterList: func() { close(listed) }}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour,
		rate: 1, interval: time.Hour, stats: &expirationStats{},
	}
	r.start()
	<-listed
	r.close()
	if r.stats.examined.Load() != 0 || r.stats.lastSuccess.Load() != 0 {
		t.Fatal("cancelled sweep kept scanning")
	}
}

func TestExpirationWaitsForRestoreAndCanStopWhileWaiting(t *testing.T) {
	for _, stopEarly := range []bool{false, true} {
		t.Run(fmt.Sprintf("stopEarly=%v", stopEarly), func(t *testing.T) {
			now := time.Unix(100_000, 0)
			db := expirationTestDB(t, &now)
			createExpirationMetric(t, db, "restored")
			now = now.Add(time.Hour)
			core := cache.New()
			synctest.Test(t, func(t *testing.T) {
				restored := make(chan struct{})
				r := &metricExpirer{db: db, now: func() time.Time { return now }, expiration: time.Hour,
					rate: 1000, interval: time.Hour, stats: &expirationStats{}, restored: restored,
					pending: core.Has,
				}
				r.start()
				synctest.Wait()
				if r.stats.examined.Load() != 0 {
					t.Fatal("cleanup ran before restore completed")
				}
				if stopEarly {
					r.close()
					return
				}
				core.AddRestored(points.OnePoint("restored", 42, now.Unix()))
				close(restored)
				time.Sleep(time.Second)
				synctest.Wait()
				r.close()
				if r.stats.examined.Load() != 1 || r.stats.pending.Load() != 1 || r.stats.deleted.Load() != 0 {
					t.Fatalf("restored metric was not protected: examined=%d pending=%d deleted=%d", r.stats.examined.Load(), r.stats.pending.Load(), r.stats.deleted.Load())
				}
			})
		})
	}
}

func TestDumpStopJoinsExpirationBeforeDumpSetup(t *testing.T) {
	app := New("")
	app.Cache = cache.New()
	app.Config.Dump.Enabled = true
	app.Config.Dump.Path = filepath.Join(t.TempDir(), "missing-directory")
	r := &metricExpirer{restored: make(chan struct{})}
	r.start()
	defer r.cancel()
	app.expirer = r
	if err := app.DumpStop(); err == nil {
		t.Fatal("expected missing dump directory error")
	}
	if app.expirer != nil {
		t.Fatal("expiration survived entry into dump phase")
	}
	select {
	case <-r.done:
	default:
		t.Fatal("DumpStop did not join expiration")
	}
}

func TestExpirationNewArrivalAfterCandidateCheckStaysQueued(t *testing.T) {
	now := time.Unix(100_000, 0)
	db := expirationTestDB(t, &now)
	m := createExpirationMetric(t, db, "metric")
	now = now.Add(time.Hour)
	core := cache.New()
	hooks := &expirationHooks{expirationStore: db, beforeDelete: func() {
		core.Add(points.OnePoint(m.Name, 42, now.Unix()))
	}}
	r := &metricExpirer{db: hooks, now: func() time.Time { return now }, expiration: time.Hour,
		pending: core.Has, stats: &expirationStats{},
	}
	if deleted, err := r.expire(context.Background(), m); err != nil || !deleted {
		t.Fatalf("deletion did not win interleaving: deleted=%v error=%v", deleted, err)
	}
	queued := core.Get(m.Name)
	if len(queued) != 1 || queued[0].Value != 42 {
		t.Fatalf("deletion lost new arrival: %v", queued)
	}
	created := createExpirationMetric(t, db, m.Name)
	if err := db.UpdateMany(context.Background(), m.Name, queued); err != nil {
		t.Fatal(err)
	}
	if created.ID == m.ID || !created.LastUpdate.Equal(now) {
		t.Fatalf("recreation reused expired identity or activity: %+v", created)
	}
}

type expirationHooks struct {
	expirationStore
	beforeDelete, afterDelete, afterList func()
	deleteErr                            error
	flushes                              int
}

func (s *expirationHooks) DeleteIfUnchanged(ctx context.Context, name string, m store.Metadata) error {
	if s.beforeDelete != nil {
		s.beforeDelete()
	}
	if s.deleteErr != nil {
		return s.deleteErr
	}
	err := s.expirationStore.DeleteIfUnchanged(ctx, name, m)
	if err == nil && s.afterDelete != nil {
		s.afterDelete()
	}
	return err
}

func (s *expirationHooks) ListPage(ctx context.Context, prefix, after string, limit int) ([]store.Metadata, error) {
	page, err := s.expirationStore.ListPage(ctx, prefix, after, limit)
	if s.afterList != nil {
		s.afterList()
	}
	return page, err
}

func (s *expirationHooks) AsyncFlush() (<-chan struct{}, error) {
	s.flushes++
	return s.expirationStore.AsyncFlush()
}
