package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
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
	r.stats = app.expirationStats
	r.trimRules = app.Carbonserver.QuotaTrimRules
	r.notify = app.Carbonserver.RequestFileScan
	r.pending = app.Cache.Has
	r.trimDryRun = true
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	trimTestNames(t, app.MetricStore, "apps.a.new", "apps.a.old", "apps.b.new", "apps.b.old")
	response, err := http.Get("http://" + app.Carbonserver.HTTPListener().Addr().String() + "/admin/info?scopes=quotaTrim")
	if err != nil {
		t.Fatal(err)
	}
	var admin struct {
		QuotaTrim struct {
			Plan quotaTrimPlan `json:"plan"`
		} `json:"quotaTrim"`
	}
	err = json.NewDecoder(response.Body).Decode(&admin)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || admin.QuotaTrim.Plan.Candidates != 2 || !admin.QuotaTrim.Plan.DryRun || len(admin.QuotaTrim.Plan.Names) != 2 {
		t.Fatalf("admin plan=%+v status=%d err=%v", admin, response.StatusCode, err)
	}

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

func TestQuotaTrimBudgetSplit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		namespaces []string
		excess     []int64
		limit      int
		want       []int
	}{
		{"proportional", []string{"a", "b", "c"}, []int64{10, 30, 60}, 10, []int{1, 3, 6}},
		{"remainder", []string{"c", "b", "a"}, []int64{1, 1, 1}, 2, []int{0, 1, 1}},
		{"bounded", []string{"a", "b"}, []int64{1, 2}, 1000, []int{1, 2}},
		{"at cap", []string{"a", "b"}, []int64{0, 4}, 2, []int{0, 2}},
		{"nested", []string{"apps.child", "apps", "other"}, []int64{2, 4, 4}, 4, []int{0, 2, 2}},
		{"root", []string{"a", "/"}, []int64{2, 4}, 2, []int{0, 2}},
		{"large counts", []string{"a", "b"}, []int64{1 << 61, 1 << 61}, 1000, []int{500, 500}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules := make([]carbonserver.QuotaTrimRule, len(tc.namespaces))
			for i, ns := range tc.namespaces {
				rules[i] = trimTestRule(ns, 1)
			}
			_, got := quotaTrimBudgets(rules, tc.excess, tc.limit)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("budgets=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestQuotaTrimPlanSurvivesSamplingAndMatchesLive(t *testing.T) {
	defer zapwriter.Test()()
	logPath := filepath.Join(t.TempDir(), "quota.json")
	cfg := zapwriter.NewConfig()
	cfg.Logger, cfg.File, cfg.Encoding, cfg.Level = "quota", logPath, "json", "info"
	cfg.SampleTick, cfg.SampleInitial, cfg.SampleThereafter = "1h", 5, 100
	if err := zapwriter.ApplyConfig([]zapwriter.Config{cfg}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	var rules []carbonserver.QuotaTrimRule
	for i := range 12 {
		ns := fmt.Sprintf("ns%02d", i)
		rules = append(rules, trimTestRule(ns, 1))
		for j := range 3 {
			createExpirationMetric(t, db, fmt.Sprintf("%s.m%d", ns, j))
		}
	}
	createExpirationMetric(t, db, "under.keep")
	rules = append(rules, trimTestRule("under", 2))
	r := trimTestExpirer(db, now, rules...)
	r.maxDeletes, r.trimDryRun = 12, true
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	preview := r.stats.trim.Info()["plan"].(*quotaTrimPlan)
	if preview.Candidates != 12 || len(preview.Namespaces) != 13 {
		t.Fatalf("plan=%+v", preview)
	}
	for i, node := range preview.Namespaces {
		if i == 12 {
			if node.Candidates != 0 || node.Excess != 0 {
				t.Fatal(node)
			}
			continue
		}
		if node.Excess != 2 || node.Candidates != 1 || len(node.Samples) != 1 || node.Samples[0] != fmt.Sprintf("ns%02d.m2", i) {
			t.Fatalf("incorrect newest candidate: %+v", node)
		}
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	var reported []quotaTrimNamespacePlan
	for _, line := range strings.Split(string(data), "\n") {
		var event struct {
			Message    string                   `json:"message"`
			Namespaces []quotaTrimNamespacePlan `json:"namespaces"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Namespaces != nil {
			reported = event.Namespaces
		}
	}
	if !reflect.DeepEqual(reported, preview.Namespaces) {
		t.Fatalf("sampled log lost plan: %s", data)
	}
	// Concurrent admin reads and interval resets retain one immutable snapshot.
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 100 {
				_ = r.stats.trim.Info()
				r.stats.trim.Stat(func(string, float64) {})
			}
		})
	}
	if r.stats.trim.Info()["plan"] != preview {
		t.Fatal("collector cleared plan")
	}
	r.trimDryRun = false
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
	live := r.stats.trim.Info()["plan"].(*quotaTrimPlan)
	if !reflect.DeepEqual(preview.Names, live.Names) || !reflect.DeepEqual(preview.Namespaces, live.Namespaces) {
		t.Fatalf("preview/live differ: %+v / %+v", preview, live)
	}
	for _, candidate := range preview.Names {
		if _, err := db.Metadata(context.Background(), candidate.Name); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("preview name survived: %s %v", candidate.Name, err)
		}
		if _, err := db.ExpirationReceiptFor(context.Background(), candidate.Name); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.List(context.Background(), "")
	if err != nil || len(rows) != 25 {
		t.Fatalf("unplanned deletion: rows=%d err=%v", len(rows), err)
	}
}

func TestQuotaTrimTaggedAliasPending(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	createExpirationMetric(t, db, "jobs.old")
	createExpirationMetric(t, db, "jobs.new;a=1;z=2")
	core := cache.New()
	core.SetTagsEnabled(true)
	core.Add(points.OnePoint("jobs.new;z=2;a=1", 42, now.Unix()))
	r := trimTestExpirer(db, now, trimTestRule("jobs", 1))
	r.pending = core.Has
	if err := r.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan := r.stats.trim.Info()["plan"].(*quotaTrimPlan)
	if plan.Candidates != 1 || plan.Names[0].Name != "jobs.new;a=1;z=2" {
		t.Fatal(plan)
	}
	trimTestNames(t, db, "jobs.new;a=1;z=2", "jobs.old")
	if r.stats.trim.total.deleted != 0 || r.stats.trim.total.skipped != 1 {
		t.Fatal(r.stats.trim.total)
	}
}

func TestQuotaTrimSelectsNewestWithinProportionalShares(t *testing.T) {
	now := time.Unix(100000, 0)
	db := expirationTestDB(t, &now)
	for _, ns := range []string{"older", "newer"} {
		n := 3
		if ns == "newer" {
			n = 7
		}
		for i := range n {
			createExpirationMetric(t, db, fmt.Sprintf("%s.m%d", ns, i))
		}
	}
	rules := []carbonserver.QuotaTrimRule{trimTestRule("older", 1), trimTestRule("newer", 1)}
	r := trimTestExpirer(db, now, rules...)
	r.maxDeletes = 4
	selected, excess, err := r.planQuotaTrim(context.Background(), rules)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range selected {
		names = append(names, m.Name)
	}
	if !reflect.DeepEqual(excess, []int64{2, 6}) || !reflect.DeepEqual(names, []string{"newer.m6", "newer.m5", "newer.m4", "older.m2"}) {
		t.Fatalf("excess=%v selected=%v", excess, names)
	}
}
