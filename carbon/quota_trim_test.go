package carbon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

func trimTestRule(namespace string, cap int64) carbonserver.QuotaTrimRule {
	return carbonserver.QuotaTrimRule{Namespace: namespace, StatMetric: strings.ReplaceAll(namespace, ".", "-"), Quota: carbonserver.Quota{Pattern: namespace, Metrics: cap, OverQuota: "delete-newest"}}
}
func trimTestExpirer(db expirationStore, now time.Time, rules ...carbonserver.QuotaTrimRule) *metricExpirer {
	return &metricExpirer{db: db, now: func() time.Time { return now }, stats: &expirationStats{}, maxDeletes: 100, deleteWorkers: 4,
		trimRules: func() ([]carbonserver.QuotaTrimRule, error) { return rules, nil }}
}
func trimTestNames(t *testing.T, db *store.Store, want ...string) {
	t.Helper()
	rows, err := db.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range rows {
		names = append(names, m.Name)
	}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("names=%v want=%v", names, want)
	}
}

func TestQuotaTrimNewestFirstLimitAndReceipts(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	// Allocation order deliberately differs from name order; hot old names stay.
	for _, name := range []string{"jobs.z", "jobs.y", "jobs.x", "jobs.b", "jobs.a"} {
		createExpirationMetric(t, db, name)
	}
	if err := db.UpdateMany(context.Background(), "jobs.z", []store.Point{{Timestamp: now.Unix(), Value: 9}}); err != nil {
		t.Fatal(err)
	}
	r := trimTestExpirer(db, now, trimTestRule("jobs", 2))
	r.maxDeletes = 2
	notified := false
	r.notify = func() { notified = true }
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, db, "jobs.x", "jobs.y", "jobs.z")
	if !notified {
		t.Fatal("index refresh not notified")
	}
	receipt, err := db.ExpirationReceiptFor(context.Background(), "jobs.a")
	if err != nil || !strings.Contains(receipt.Policy, "QuotaTrim") {
		t.Fatalf("missing quota receipt: %+v %v", receipt, err)
	}
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, db, "jobs.y", "jobs.z")
	for range 3 {
		if err := r.sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	trimTestNames(t, db, "jobs.y", "jobs.z")
	stats := map[string]float64{}
	r.stats.trim.Stat(func(k string, v float64) { stats[k] = v })
	if stats["candidates"] != 3 || stats["deleted.jobs"] != 3 || stats["skipped"] != 0 {
		t.Fatal(stats)
	}
}

func TestQuotaTrimDryRunAndUnconfiguredRules(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	for _, name := range []string{"apps.a", "apps.b", "other.a", "other.b"} {
		createExpirationMetric(t, db, name)
	}
	r := trimTestExpirer(db, now, trimTestRule("apps", 1))
	r.trimDryRun = true
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, db, "apps.a", "apps.b", "other.a", "other.b")
	stats := map[string]float64{}
	r.stats.trim.Stat(func(k string, v float64) { stats[k] = v })
	if stats["candidates.apps"] != 1 || stats["deleted"] != 0 || stats["skipped.apps"] != 1 {
		t.Fatal(stats)
	}
	if _, err := db.ExpirationReceiptFor(context.Background(), "apps.b"); !errors.Is(err, store.ErrConflict) {
		t.Fatal(err)
	}
	r.trimDryRun = false
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, db, "apps.a", "other.a", "other.b")
}

func TestQuotaTrimOverlappingCapsAndSmallPasses(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	for _, name := range []string{"apps.a.old", "apps.b.old", "apps.a.new", "apps.b.new", "apps.a.newest", "apps.b.newest", "unrelated.keep"} {
		createExpirationMetric(t, db, name)
	}
	r := trimTestExpirer(db, now, trimTestRule("apps", 3), trimTestRule("apps.a", 2), trimTestRule("apps.b", 1))
	r.maxDeletes = 1
	for range 8 {
		if err := r.sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	trimTestNames(t, db, "apps.a.new", "apps.a.old", "apps.b.old", "unrelated.keep")
	// Even an over-quota parent cannot take a child below its own cap.
	r.trimRules = func() ([]carbonserver.QuotaTrimRule, error) {
		return []carbonserver.QuotaTrimRule{trimTestRule("apps", 1), trimTestRule("apps.a", 2), trimTestRule("apps.b", 1)}, nil
	}
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, db, "apps.a.new", "apps.a.old", "apps.b.old", "unrelated.keep")
}

func TestQuotaTrimArrivalInterleavings(t *testing.T) {
	for _, arrival := range []string{"pending", "inflight", "committed-after-check", "queued-after-check"} {
		t.Run(arrival, func(t *testing.T) {
			now := time.Unix(100000, 0)
			db := expirationTestDB(t, &now)
			createExpirationMetric(t, db, "jobs.old")
			newest := createExpirationMetric(t, db, "jobs.new")
			core := cache.New()
			hooks := &trimRaceStore{Store: db}
			r := trimTestExpirer(hooks, now, trimTestRule("jobs", 1))
			r.pending = core.Has
			switch arrival {
			case "pending", "inflight":
				core.Add(points.OnePoint(newest.Name, 42, now.Unix()))
				if arrival == "inflight" {
					if _, ok := core.PopNotConfirmed(newest.Name); !ok {
						t.Fatal("missing inflight write")
					}
				}
			case "committed-after-check":
				hooks.beforeDelete = func() {
					if err := db.UpdateMany(context.Background(), newest.Name, []store.Point{{Timestamp: now.Unix(), Value: 42}}); err != nil {
						t.Error(err)
					}
				}
			case "queued-after-check":
				hooks.beforeDelete = func() { core.Add(points.OnePoint(newest.Name, 42, now.Unix())) }
			}
			if err := r.sweep(context.Background()); err != nil {
				t.Fatal(err)
			}
			if arrival == "queued-after-check" {
				trimTestNames(t, db, "jobs.old")
				queued := core.Get(newest.Name)
				if len(queued) != 1 || queued[0].Value != 42 {
					t.Fatalf("lost arrival: %v", queued)
				}
				recreated := createExpirationMetric(t, db, newest.Name)
				if recreated.ID == newest.ID {
					t.Fatal("reused deleted identity")
				}
				if err := db.UpdateMany(context.Background(), newest.Name, queued); err != nil {
					t.Fatal(err)
				}
			}
			trimTestNames(t, db, "jobs.new", "jobs.old")
			if arrival == "committed-after-check" || arrival == "queued-after-check" {
				snapshot, err := db.Snapshot(context.Background(), newest.Name)
				if err != nil || len(snapshot.Archives) != 1 || len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 42 {
					t.Fatalf("new point was lost: %+v %v", snapshot, err)
				}
			}
			stats := map[string]float64{}
			r.stats.trim.Stat(func(k string, v float64) { stats[k] = v })
			if arrival != "queued-after-check" && (stats["deleted"] != 0 || stats["skipped.jobs"] != 1) {
				t.Fatal(stats)
			}
		})
	}
}

func TestQuotaTrimConfig(t *testing.T) {
	cfg := NewConfig()
	if cfg.Whisper.QuotaTrimEnabled || !cfg.Whisper.QuotaTrimDryRun {
		t.Fatal("unsafe defaults")
	}
	cfg.Whisper.QuotaTrimEnabled = true
	if err := validateStorageConfig(cfg); err == nil {
		t.Fatal("file backend accepted")
	}
	_, cfg = sharedAppConfig(t)
	cfg.Whisper.QuotaTrimEnabled = true
	cfg.Carbonserver.TrieIndex, cfg.Carbonserver.ConcurrentIndex, cfg.Carbonserver.RealtimeIndex = true, true, 64
	cfg.Whisper.QuotasFilename = filepath.Join(t.TempDir(), "quotas.conf")
	if err := validateStorageConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.Quotas = persister.WhisperQuotas{{Pattern: "apps", Metrics: 2, OverQuota: "delete-newest", DroppingPolicy: "new"}}
	rules := cfg.getCarbonserverQuotas(time.Minute)
	if len(rules) != 1 || rules[0].OverQuota != "delete-newest" || rules[0].DroppingPolicy != carbonserver.QDPNew {
		t.Fatal(rules)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Whisper.StoreMirrorFiles = true },
		func(c *Config) { c.Whisper.StoreIncrementalMigration = true },
		func(c *Config) { c.Carbonserver.TrieIndex = false },
		func(c *Config) { c.Carbonserver.ConcurrentIndex = false },
		func(c *Config) { c.Carbonserver.RealtimeIndex = 0 },
		func(c *Config) { c.Whisper.QuotasFilename = "" },
	} {
		copy := *cfg
		change(&copy)
		if err := validateStorageConfig(&copy); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestQuotaTrimErrorsAndPressure(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	createExpirationMetric(t, db, "jobs.a")
	createExpirationMetric(t, db, "jobs.b")
	guard := &pressureExpirationStore{expirationStore: db, stalled: true}
	r := trimTestExpirer(guard, now, trimTestRule("jobs", 1))
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, db, "jobs.a", "jobs.b")
	r.trimRules = func() ([]carbonserver.QuotaTrimRule, error) { return nil, fmt.Errorf("catalog unavailable") }
	if err := r.sweep(context.Background()); err == nil || !strings.Contains(err.Error(), "catalog unavailable") {
		t.Fatal(err)
	}
	fields := map[string]float64{}
	r.stats.trim.Stat(func(k string, v float64) { fields[k] = v })
	if fields["errors"] != 1 || fields["skipped.jobs"] != 1 {
		t.Fatal(fields)
	}
}

// Interpose immediately before the real batched receipt transaction, after the
// shared executor's cache check. This exercises both winners of the race.
type trimRaceStore struct {
	*store.Store
	beforeDelete func()
}

func (s *trimRaceStore) DeleteBatchWithExpirationReceipts(ctx context.Context, rows []store.Metadata, receipts map[string]store.ExpirationReceipt) ([]error, error) {
	if s.beforeDelete != nil {
		s.beforeDelete()
	}
	return s.Store.DeleteBatchWithExpirationReceipts(ctx, rows, receipts)
}

func TestQuotaTrimPollingLifecycleAndIndex(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	cfg.Buckyd.Enabled = false
	cfg.Carbonserver.TrieIndex, cfg.Carbonserver.ConcurrentIndex, cfg.Carbonserver.RealtimeIndex = true, true, 64
	cfg.Carbonserver.QuotaUsageReportFrequency = &Duration{time.Minute}
	cfg.Carbonserver.ScanFrequency = &Duration{time.Hour}
	cfg.Whisper.QuotasFilename = filepath.Join(t.TempDir(), "quotas.conf")
	cfg.Whisper.QuotasReloadInterval = Duration{10 * time.Millisecond}
	cfg.Whisper.StoreExpirationCheckInterval = &Duration{time.Hour}
	cfg.Whisper.StoreExpirationDeleteRate = 1_000_000_000
	cfg.Whisper.StoreExpirationScanRate = 1_000_000_000
	writeQuota := func(option string) {
		t.Helper()
		text := "[apps.*]\nmetrics=1\nstat-metric-prefix=team.\n" + option
		tmp := cfg.Whisper.QuotasFilename + ".tmp"
		if err := os.WriteFile(tmp, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, cfg.Whisper.QuotasFilename); err != nil {
			t.Fatal(err)
		}
	}
	writeQuota("")
	writeSharedConfig(t, path, cfg)
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"apps.a.old", "apps.a.new", "apps.b.old", "apps.b.new"} {
		createExpirationMetric(t, db, name)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	if app.expirer != nil {
		t.Fatal("global opt-in disabled but worker started")
	}
	// Polling publishes the new option without replacing the application.
	writeQuota("over-quota=delete-newest\n")
	waitExpiration(t, func() bool { rules, err := app.Carbonserver.QuotaTrimRules(); return err == nil && len(rules) == 2 })
	r := trimTestExpirer(app.MetricStore, time.Now())
	r.trimRules = app.Carbonserver.QuotaTrimRules
	r.notify = app.Carbonserver.RequestFileScan
	r.pending = app.Cache.Has
	r.trimDryRun = true
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, app.MetricStore, "apps.a.new", "apps.a.old", "apps.b.new", "apps.b.old")
	r.trimDryRun = false
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, app.MetricStore, "apps.a.old", "apps.b.old")
	waitExpiration(t, func() bool {
		return !app.Carbonserver.MetricExists("apps.a.new") && !app.Carbonserver.MetricExists("apps.b.new")
	})
	// Existing cap and usage enforcement still reject a fresh name after deletion.
	if !app.Carbonserver.ShouldThrottleMetric(points.OnePoint("apps.a.refill", 1, 1), false) {
		t.Fatal("namespace refilled at cap")
	}
	writeQuota("")
	waitExpiration(t, func() bool { rules, err := app.Carbonserver.QuotaTrimRules(); return err == nil && len(rules) == 0 })
	createExpirationMetric(t, app.MetricStore, "apps.a.extra")
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, app.MetricStore, "apps.a.extra", "apps.a.old", "apps.b.old")
	// Exercise the notification fallback used when no dedicated refresher runs.
	app.Lock()
	app.metricStoreIndex.close()
	app.metricStoreIndex = nil
	app.Unlock()
	// Global controls use the existing joined SIGHUP lifecycle, even without TTL.
	cfg.Whisper.QuotaTrimEnabled = true
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.expirer == nil || !app.expirer.trimDryRun || app.expirer.notify == nil {
		t.Fatal("global opt-in / default dry-run lost")
	}
	previous := app.expirer
	cfg.Whisper.QuotaTrimEnabled = false
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.expirer != nil {
		t.Fatal("disabled trim worker still running")
	}
	select {
	case <-previous.done:
	default:
		t.Fatal("reload did not join old trim worker")
	}
}

func TestQuotaTrimAndTTLShareLimitAcrossStatsCollection(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	for _, name := range []string{"jobs.old", "jobs.new", "outside.old"} {
		createExpirationMetric(t, db, name)
	}
	now = now.Add(2 * time.Hour)
	hooks := &trimRaceStore{Store: db}
	r := trimTestExpirer(hooks, now, trimTestRule("jobs", 1))
	r.expiration = time.Hour
	r.maxDeletes = 1
	r.stats.deleted.Store(100)
	hooks.beforeDelete = func() { r.stats.Stat(func(string, float64) {}) }
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, db, "jobs.old", "outside.old")
}
