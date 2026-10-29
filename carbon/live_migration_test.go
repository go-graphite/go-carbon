package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func liveMigrationFixture(t *testing.T, configure ...func(*Config)) (*App, *Config, *os.File) {
	t.Helper()
	path, cfg := sharedAppConfig(t)
	cfg.Tags.Enabled = false
	cfg.Buckyd.Enabled = false
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreMirrorFiles = true
	cfg.Whisper.StoreLiveMigration = true
	cfg.Whisper.StoreMigrationWorkers = 2
	cfg.Whisper.FLock = true
	cfg.Whisper.Compressed = true
	cfg.Whisper.OutOfOrder = true
	cfg.Whisper.Workers = 2
	cfg.Whisper.MaxUpdatesPerSecond = 0
	cfg.Cache.WriteoutMinPoints = 1
	cfg.Cache.WriteoutMaxDelay = Duration{time.Millisecond}
	cfg.Carbonserver.QueryCacheEnabled = false
	cfg.Carbonserver.FindCacheEnabled = false
	cfg.Carbonserver.GlobCacheEnabled = false
	cfg.Carbonserver.RealtimeIndex = 1000
	cfg.Common.MetricInterval = &Duration{time.Hour}
	if err := os.MkdirAll(cfg.Whisper.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(cfg.Whisper.DataDir, "cold.wsp")
	w, err := whisper.CreateWithOptions(source, whisper.MustParseRetentionDefs("1s:2m"), whisper.Average, 0, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix() - 10
	if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now), Value: 1.25}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	config := store.MetricConfig{Name: "cold", Retentions: []store.Retention{{Step: 1, Count: 120}}, AggregationMethod: store.Average}
	if _, err = db.CreateFromSnapshot(context.Background(), store.Snapshot{Metadata: store.Metadata{MetricConfig: config}, Archives: []store.Archive{{Retention: config.Retentions[0], Points: []store.Point{{Timestamp: now, Value: 999}}}}}); err != nil {
		t.Fatal(err)
	}
	config.Name = "vanished"
	if _, err = db.Create(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(cfg.Whisper.StoreDir, "NEEDS-CATCHUP"), []byte("live baseline"), 0600); err != nil {
		t.Fatal(err)
	}
	// Hold one cold original so repair cannot finish, without obstructing the
	// independent live metric whose freshness is the liveness oracle.
	f, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	for _, apply := range configure {
		apply(cfg)
	}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err = app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	return app, cfg, f
}

func waitMigration(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("migration condition did not become true")
}

func TestLiveMigrationServesFreshOriginalThenVerifiedShared(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app, cfg, cold := liveMigrationFixture(t)
	app.Lock()
	cache, db, server := app.Cache, app.MetricStore, app.Carbonserver
	app.Unlock()
	if app.sharedStoreReads.Load() {
		t.Fatal("shared reads enabled before cold source was verified")
	}
	now := time.Now().Unix() - 40
	address := "http://" + server.HTTPListener().Addr().String()
	client := &http.Client{Timeout: time.Second}
	for i := range 12 {
		stamp := now + int64(i)
		value := float64(i) + .125
		cache.Add(points.OnePoint("live", value, stamp))
		waitMigration(t, func() bool {
			response, err := client.Get(address + "/render/?" + url.Values{"target": {"live"}, "from": {strconv.FormatInt(now-1, 10)}, "until": {strconv.FormatInt(now+20, 10)}, "format": {"json"}}.Encode())
			if err != nil {
				return false
			}
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			var out struct {
				Metrics []struct {
					StartTime int64
					StepTime  int64
					Values    []float64
					IsAbsent  []bool
				}
			}
			if response.StatusCode != 200 || json.Unmarshal(body, &out) != nil {
				return false
			}
			for _, metric := range out.Metrics {
				index := (stamp - metric.StartTime) / metric.StepTime
				if index >= 0 && index < int64(len(metric.Values)) && !metric.IsAbsent[index] && metric.Values[index] == value {
					return true
				}
			}
			return false
		})
	}
	if app.sharedStoreReads.Load() {
		t.Fatal("migration published while its source lock remained held")
	}
	if err := syscall.Flock(int(cold.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	waitMigration(t, app.sharedStoreReads.Load)
	want, err := whisperio.ReadSnapshot(context.Background(), "cold", filepath.Join(cfg.Whisper.DataDir, "cold.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(context.Background(), "cold")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EqualSnapshots(want, got); err != nil {
		t.Fatalf("stale baseline was not repaired: %v", err)
	}
	if _, err = db.Metadata(context.Background(), "vanished"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("vanished original remained in shared catalog: %v", err)
	}
	if _, err = os.Stat(filepath.Join(cfg.Whisper.StoreDir, "NEEDS-CATCHUP")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("catch-up guard retained after verification: %v", err)
	}
	if _, err = os.Stat(filepath.Join(cfg.Whisper.StoreDir, "MIRROR-RECONCILE-REQUIRED")); err != nil {
		t.Fatal("rollback session guard was removed")
	}
	// Workers restart on the same object: writes after the in-process reader
	// switch must still persist to both original and shared archives.
	cache.Add(points.OnePoint("live", 73.5, now+13))
	waitMigration(t, func() bool {
		snapshot, err := db.Snapshot(context.Background(), "live")
		if err != nil {
			return false
		}
		for _, p := range snapshot.Archives[0].Points {
			if p.Timestamp == now+13 && p.Value == 73.5 {
				return true
			}
		}
		return false
	})
}

func TestLiveMigrationStopCancelsHeldSourceAndKeepsGuards(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app, cfg, _ := liveMigrationFixture(t)
	stopped := make(chan struct{})
	go func() { app.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("stop waited for a cold source lock instead of cancelling capture")
	}
	if app.sharedStoreReads.Load() {
		t.Fatal("cancelled migration enabled shared reads")
	}
	for _, marker := range []string{"NEEDS-CATCHUP", "MIRROR-RECONCILE-REQUIRED"} {
		if _, err := os.Stat(filepath.Join(cfg.Whisper.StoreDir, marker)); err != nil {
			t.Fatalf("cancelled migration removed %s", marker)
		}
	}
}

func TestLiveBaselineReceiptBindsBothTrees(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	_, cfg := sharedAppConfig(t)
	cfg.Buckyd.Enabled = false
	cfg.Tags.Enabled = false
	cfg.Whisper.StoreMirrorFiles = true
	cfg.Whisper.StoreLiveMigration = true
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreMigrationBaselineCopy = t.TempDir()
	for _, dir := range []string{cfg.Whisper.DataDir, cfg.Whisper.StoreDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	id := store.CaptureIdentity{Source: cfg.Whisper.DataDir, Copy: cfg.Whisper.StoreMigrationBaselineCopy, Destination: cfg.Whisper.StoreDir, SourceID: "verified-fixture", Live: true}
	write := func(path string, v any) {
		t.Helper()
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(cfg.Whisper.StoreDir, "CONVERSION.json"), id)
	write(filepath.Join(cfg.Whisper.StoreMigrationBaselineCopy, "CAPTURE.json"), id)
	write(filepath.Join(cfg.Whisper.StoreDir, "VERIFIED-CAPTURES.json"), store.CaptureReceipt{Version: 1, Identity: id, Verification: "test fixture: complete verification"})
	if err := os.WriteFile(filepath.Join(cfg.Whisper.StoreDir, "NEEDS-CATCHUP"), []byte("live baseline"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := checkStoreStartup(cfg); err != nil {
		t.Fatal(err)
	}
	wrong := id
	wrong.SourceID = "different-source"
	write(filepath.Join(cfg.Whisper.StoreMigrationBaselineCopy, "CAPTURE.json"), wrong)
	if err := checkStoreStartup(cfg); err == nil {
		t.Fatal("different capture identity accepted")
	}
	write(filepath.Join(cfg.Whisper.StoreMigrationBaselineCopy, "CAPTURE.json"), id)
	for _, marker := range []string{"CONVERSION-IN-PROGRESS", "MIRROR-RECONCILE-REQUIRED"} {
		p := filepath.Join(cfg.Whisper.StoreDir, marker)
		if err := os.WriteFile(p, []byte("guard"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := checkStoreStartup(cfg); err == nil {
			t.Fatalf("baseline shortcut accepted %s", marker)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	// An interrupted session cannot infer its original limit from the now
	// mutated destination catalog. A bound must accompany the verified pair.
	marker := filepath.Join(cfg.Whisper.StoreDir, "MIRROR-RECONCILE-REQUIRED")
	if err := os.WriteFile(marker, []byte("guard"), 0600); err != nil {
		t.Fatal(err)
	}
	write(filepath.Join(cfg.Whisper.StoreDir, "VERIFIED-CAPTURES.json"), store.CaptureReceipt{Version: 1, Identity: id, Verification: "test fixture", BaselineMaxMetricID: 37})
	if err := checkStoreStartup(cfg); err != nil {
		t.Fatalf("bounded interrupted migration rejected: %v", err)
	}
	if err := os.Remove(filepath.Join(cfg.Whisper.StoreDir, "NEEDS-CATCHUP")); err != nil {
		t.Fatal(err)
	}
	if err := checkStoreStartup(cfg); err != nil {
		t.Fatalf("bounded mirror session could not restart after shared publication: %v", err)
	}
	write(filepath.Join(cfg.Whisper.StoreMigrationBaselineCopy, "CAPTURE.json"), wrong)
	if err := checkStoreStartup(cfg); err == nil {
		t.Fatal("mirror-only restart accepted another capture identity")
	}
	write(filepath.Join(cfg.Whisper.StoreMigrationBaselineCopy, "CAPTURE.json"), id)
	cfg.Whisper.StoreMirrorFiles = false
	if err := checkStoreStartup(cfg); err == nil {
		t.Fatal("mirror-only restart accepted without original-file mirroring")
	}
	cfg.Whisper.StoreMirrorFiles = true
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := checkStoreStartup(cfg); err == nil {
		t.Fatal("already served baseline accepted for fast reuse")
	}

}

func TestLiveMigrationReloadDuringColdRepairKeepsFreshLifecycle(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app, _, cold := liveMigrationFixture(t)
	reloaded := make(chan error, 1)
	go func() { reloaded <- app.ReloadConfig() }()
	select {
	case err := <-reloaded:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reload could not cancel the old repair")
	}
	if app.sharedStoreReads.Load() {
		t.Fatal("reload published an unverified store")
	}
	if err := syscall.Flock(int(cold.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	waitMigration(t, app.sharedStoreReads.Load)
}

func TestLiveMigrationConcurrentReloadsRetainCurrentRepair(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app, _, cold := liveMigrationFixture(t)
	done := make(chan error, 2)
	for range 2 {
		go func() { done <- app.ReloadConfig() }()
	}
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent reloads lost migration cancellation")
		}
	}
	if err := syscall.Flock(int(cold.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	waitMigration(t, app.sharedStoreReads.Load)
}

func TestInterruptedBaselineRetainsOriginalCatalogCeiling(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	_, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreMirrorFiles = true
	cfg.Whisper.StoreLiveMigration = true
	cfg.Whisper.StoreMigrationBaselineCopy = t.TempDir()
	if err := os.MkdirAll(cfg.Whisper.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"baseline", "created-after-baseline"} {
		if _, err = db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 120}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
	}
	current, err := db.CatalogHighWatermark(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if current <= 1 {
		t.Fatal("fixture must have a post-baseline ID")
	}
	id := store.CaptureIdentity{Source: cfg.Whisper.DataDir, Copy: cfg.Whisper.StoreMigrationBaselineCopy, Destination: cfg.Whisper.StoreDir, SourceID: "verified-interrupted-fixture", Live: true}
	write := func(path string, v any) {
		t.Helper()
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	write(filepath.Join(cfg.Whisper.StoreDir, "CONVERSION.json"), id)
	write(filepath.Join(cfg.Whisper.StoreMigrationBaselineCopy, "CAPTURE.json"), id)
	write(filepath.Join(cfg.Whisper.StoreDir, "VERIFIED-CAPTURES.json"), store.CaptureReceipt{Version: 1, Identity: id, Verification: "complete fixture", BaselineMaxMetricID: 1})
	for _, marker := range []string{"NEEDS-CATCHUP", "MIRROR-RECONCILE-REQUIRED"} {
		if err = os.WriteFile(filepath.Join(cfg.Whisper.StoreDir, marker), []byte("guard"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	app := New("")
	app.Config = cfg
	if _, err = app.startStorage(); err != nil {
		t.Fatal(err)
	}
	if got := app.mirrorState.BaselineIDLimit; got != 1 {
		t.Fatalf("original bound enlarged to mutated catalog: got %d, current %d", got, current)
	}
	app.stopAll()
	write(filepath.Join(cfg.Whisper.StoreDir, "VERIFIED-CAPTURES.json"), store.CaptureReceipt{Version: 1, Identity: id, Verification: "complete fixture", BaselineMaxMetricID: current + 1})
	next := New("")
	next.Config = cfg
	defer next.stopAll()
	if _, err = next.startStorage(); err == nil {
		t.Fatal("future catalog ceiling accepted")
	}
}
