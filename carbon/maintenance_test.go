package carbon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
)

func TestStoreMaintenanceWaitsForWarmAndPacing(t *testing.T) {
	defer zapwriter.Test()()
	synctest.Test(t, func(t *testing.T) {
		warmed := make(chan struct{})
		var paced atomic.Bool
		paced.Store(true)
		g := newStoreMaintenanceGate(warmed, paced.Load, storeMaintenanceMaxWait)
		done := make(chan error, 1)
		go func() { done <- g.wait(context.Background(), "test") }()
		synctest.Wait()
		// Pacing alone ending cannot release a running checkpoint.
		paced.Store(false)
		time.Sleep(2 * time.Second)
		assertMaintenancePending(t, done)
		paced.Store(true)
		close(warmed)
		time.Sleep(2 * time.Second)
		assertMaintenancePending(t, done)
		paced.Store(false)
		time.Sleep(time.Second)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
	logs := zapwriter.TestString()
	if !strings.Contains(logs, "store maintenance waiting for restart warm-up") || !strings.Contains(logs, "store maintenance starting") || !strings.Contains(logs, "restart warming and pacing complete") {
		t.Fatal(logs)
	}
}

func assertMaintenancePending(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatal("maintenance ran before both startup phases ended", err)
	default:
	}
}

func TestStoreMaintenanceBoundAndReload(t *testing.T) {
	defer zapwriter.Test()()
	synctest.Test(t, func(t *testing.T) {
		g := newStoreMaintenanceGate(make(chan struct{}), func() bool { return true }, 5*time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		first := make(chan error, 1)
		go func() { first <- g.wait(ctx, "old-policy") }()
		synctest.Wait()
		time.Sleep(3 * time.Second)
		cancel() // expiration reload joins the obsolete policy's waiter
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		// Replacement policy uses the same store gate and original deadline.
		next := make(chan error, 1)
		go func() { next <- g.wait(context.Background(), "new-policy") }()
		synctest.Wait()
		time.Sleep(time.Second)
		assertMaintenancePending(t, next)
		time.Sleep(time.Second)
		if err := <-next; err != nil {
			t.Fatal(err)
		}
		t.Log("maintenance released at original 5s bound across policy reload")
	})
	logs := zapwriter.TestString()
	if !strings.Contains(logs, "WARN") || !strings.Contains(logs, "store maintenance restart wait expired") {
		t.Fatal(logs)
	}
}

func TestMetadataCheckpointFailureVisibility(t *testing.T) {
	defer zapwriter.Test()()
	logger := zapwriter.Logger("storage")
	budget := restartcpu.NewWithCPU(1, func() float64 { return 0 })
	reg := prometheus.NewPedanticRegistry()
	budget.InitPrometheus(reg)
	for _, tc := range []struct {
		valid bool
		err   error
		level string
	}{
		{true, nil, "INFO"},
		{false, nil, "WARN"},
		{false, fmt.Errorf("load: %w", context.DeadlineExceeded), "WARN"},
		{false, errors.New("corrupt checkpoint"), "WARN"},
	} {
		before := len(zapwriter.TestString())
		reportMetadataCheckpoint(logger, "test", store.MetadataCheckpointStats{Valid: tc.valid}, tc.err, budget)
		line := zapwriter.TestString()[before:]
		if !strings.Contains(line, tc.level) {
			t.Fatalf("valid=%v err=%v log=%s", tc.valid, tc.err, line)
		}
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"restart_cpu_checkpoint_load_failures_total": 3, "restart_cpu_checkpoint_load_timeouts_total": 1}
	for _, f := range families {
		if value, ok := want[f.GetName()]; ok {
			if got := f.Metric[0].GetCounter().GetValue(); got != value {
				t.Fatalf("%s=%v want %v", f.GetName(), got, value)
			}
			delete(want, f.GetName())
		}
	}
	if len(want) != 0 {
		t.Fatal("missing metrics", want)
	}
}

func TestStoreMaintenanceSIGHUPKeepsStartupGate(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreTimeBuckets = true
	cfg.Whisper.StoreMetadataCacheSize = 2 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	original := app.storeMaintenance
	if original == nil {
		t.Fatal("missing startup gate")
	}
	cfg.Whisper.StoreColdPack = "exact"
	cfg.Whisper.StoreExpiration = &Duration{time.Hour}
	// Prewarm knobs are next-start settings; disabling them on SIGHUP must
	// not remove the current store's startup protection.
	cfg.Whisper.StoreMetadataCachePrewarm = false
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.storeMaintenance != original || app.expirer == nil || app.MetricStore.ColdPackStats().Mode != "exact" {
		t.Fatal("reload replaced gate or lost policies")
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case <-original.done:
		t.Fatal("SIGHUP bypassed restart pacing")
	default:
	}
	if app.expirationStats.lastSuccess.Load() != 0 || app.MetricStore.ColdPackStats().Pages != 0 {
		t.Fatal("maintenance ran through startup gate")
	}
}

// Drive the service's real load path, including a valid header with corrupt
// record data, rather than relying only on synthetic result reporting.
func TestMetadataCheckpointInvalidLoadReportsFailure(t *testing.T) {
	defer zapwriter.Test()()
	dir := t.TempDir()
	opts := store.Options{MetadataCacheSize: 2 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Create(context.Background(), store.MetricConfig{Name: "corrupt.metadata", Retentions: []store.Retention{{Step: 1, Count: 100}}, AggregationMethod: store.Average})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Metadata(context.Background(), "corrupt.metadata"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "metadata-cache-v1")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 60 {
		t.Fatal("checkpoint has no record")
	}
	data[60] ^= 1 // keep the clean fence, break the first record checksum
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conf := NewConfig().Whisper
	conf.StoreMetadataCachePrewarm, conf.StoreMetadataCacheSize = true, 2<<20
	budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
	reg := prometheus.NewPedanticRegistry()
	budget.InitPrometheus(reg)
	done := startMetadataPrewarm(db, conf, "corrupt-test", nil, nil, new(store.PrewarmProgress), nil, budget)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("invalid load did not finish")
	}
	logs := zapwriter.TestString()
	if !strings.Contains(logs, "WARN [storage] metadata cache checkpoint load failed") || !strings.Contains(logs, "checksum") {
		t.Fatal("invalid load was not loud", logs)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "restart_cpu_checkpoint_load_failures_total" && f.Metric[0].GetCounter().GetValue() == 1 {
			return
		}
	}
	t.Fatal("invalid load not counted")
}

// Only the scan boundary matters here; avoiding filesystem work keeps the
// simulated clock independent of real disk completion.
type maintenanceExpirationStore struct {
	expirationStore
	scanned chan struct{}
}

func (s *maintenanceExpirationStore) ListPage(context.Context, string, string, int) ([]store.Metadata, error) {
	s.scanned <- struct{}{}
	return nil, nil
}

func TestExpirationWaitsForMaintenanceAndRestore(t *testing.T) {
	for _, release := range []string{"warm", "timeout", "shutdown"} {
		t.Run(release, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db := &maintenanceExpirationStore{scanned: make(chan struct{}, 1)}
				restored, warmed := make(chan struct{}), make(chan struct{})
				gate := newStoreMaintenanceGate(warmed, func() bool { return false }, storeMaintenanceMaxWait)
				r := &metricExpirer{db: db, now: time.Now, expiration: time.Hour,
					interval: time.Hour, stats: &expirationStats{}, restored: restored,
					maintenanceWait: gate.wait,
				}
				r.start()
				defer r.close()
				synctest.Wait()
				if release == "timeout" {
					// The maintenance safety bound cannot bypass restoration.
					time.Sleep(storeMaintenanceMaxWait)
					select {
					case <-db.scanned:
						t.Fatal("maintenance timeout bypassed restoration")
					default:
					}
					close(restored)
				} else {
					close(restored)
					synctest.Wait()
					select {
					case <-db.scanned:
						t.Fatal("expiration bypassed maintenance warming")
					default:
					}
					if release == "shutdown" {
						return // close must cancel the waiter without a sweep
					}
					close(warmed)
				}
				select {
				case <-db.scanned:
				case <-time.After(2 * time.Second):
					t.Fatal("maintenance did not run after both waits ended")
				}
			})
		})
	}
}
