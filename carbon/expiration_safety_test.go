package carbon

import (
	"context"
	"testing"
	"time"
)

type pressureExpirationStore struct {
	expirationStore
	calls   int
	stalled bool
	l0      int64
	debt    uint64
}

func (s *pressureExpirationStore) ExpirationPressure() (int64, uint64, bool) {
	s.calls++
	return s.l0, s.debt, s.stalled
}

func TestExpirationDryRunLimitsAndPressure(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	old := createExpirationMetric(t, db, "jobs.old")
	createExpirationMetric(t, db, "keep.metric")
	now = now.Add(2 * time.Hour)
	rules := expirationTestRules(t, "[keep]\npattern = ^keep\\.\nexpiration = 0s\n[jobs]\npattern = ^jobs\\.\nexpiration = 1h\n")
	r := &metricExpirer{db: db, now: func() time.Time { return now }, rate: 1_000_000, expiration: time.Hour, stats: &expirationStats{}, rules: rules, dryRun: true}
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := db.Metadata(context.Background(), old.Name)
	if err != nil || got.Revision != old.Revision || !got.LastUpdate.Equal(old.LastUpdate) {
		t.Fatalf("dry-run mutated %+v %v", got, err)
	}
	if r.stats.wouldExpire.Load() != 1 || r.stats.wouldBytes.Load() != 43228 || r.ruleStats[2].would.Load() != 1 || r.ruleStats[1].kept.Load() != 1 {
		t.Fatalf("dry-run counters: would %d bytes %d", r.stats.wouldExpire.Load(), r.stats.wouldBytes.Load())
	}
	fields := map[string]float64{}
	r.stats.Stat(func(name string, value float64) { fields[name] = value })
	if fields["rules.2_jobs.wouldExpire"] != 1 || fields["rules.1_keep.kept"] != 1 {
		t.Fatal(fields)
	}
	for i := 0; i < 3; i++ {
		createExpirationMetric(t, db, []string{"jobs.a", "jobs.b", "jobs.c"}[i])
	}
	now = now.Add(2 * time.Hour)
	r.dryRun = false
	r.maxDeletes = 1
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.stats.deleted.Load() != 1 || r.stats.limited.Load() != 3 {
		t.Fatalf("cap deleted=%d limited=%d", r.stats.deleted.Load(), r.stats.limited.Load())
	}
	guard := &pressureExpirationStore{expirationStore: db, stalled: true}
	r.db = guard
	r.maxDeletes = 100
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.stats.deleted.Load() != 1 || r.stats.guarded.Load() != 3 || guard.calls != 1 {
		t.Fatalf("pressure deleted=%d guarded=%d calls=%d", r.stats.deleted.Load(), r.stats.guarded.Load(), guard.calls)
	}
	guard.stalled = false
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.stats.deleted.Load() != 4 {
		t.Fatal("did not resume after pressure")
	}
	createExpirationMetric(t, db, "jobs.pressure")
	now = now.Add(2 * time.Hour)
	r.maxL0 = 6
	r.maxDebt = 1 << 30
	for _, pressure := range []struct {
		l0   int64
		debt uint64
	}{{6, 0}, {0, 1 << 30}} {
		guard.l0, guard.debt = pressure.l0, pressure.debt
		if err := r.sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		if r.stats.deleted.Load() != 4 {
			t.Fatal("deleted under compaction pressure")
		}
	}
	guard.l0, guard.debt = 0, 0
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.stats.deleted.Load() != 5 {
		t.Fatal("did not recover from compaction pressure")
	}
}
