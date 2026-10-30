package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

func incrementalAppFixture(t *testing.T) (string, *Config) {
	t.Helper()
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreIncrementalMigration = true
	cfg.Whisper.FLock = true
	cfg.Whisper.Compressed = true
	cfg.Whisper.StoreMigrationWorkers = 4
	cfg.Whisper.StoreMigrationMinFreeBytes = 0
	cfg.Whisper.StoreMigrationBytesInFlight = 8 << 20
	cfg.Whisper.StoreMigrationControlFile = filepath.Join(filepath.Dir(path), "migration-control.json")
	cfg.Buckyd.Enabled = false
	cfg.Carbonserver.TrieIndex = true
	cfg.Carbonserver.TrigramIndex = false
	cfg.Carbonserver.ConcurrentIndex = true
	cfg.Carbonserver.RealtimeIndex = 100
	cfg.Carbonserver.FileListCache = filepath.Join(filepath.Dir(path), "files.flc")
	cfg.Carbonserver.FileListCacheVersion = 2
	cfg.Carbonserver.ScanFrequency = &Duration{time.Hour}
	if err := os.MkdirAll(cfg.Whisper.DataDir, 0750); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	for _, name := range []string{"first", "second", "third"} {
		w, err := whisper.CreateWithOptions(filepath.Join(cfg.Whisper.DataDir, name+".wsp"), whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 3600)}), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now - 30), Value: 1}, {Time: int(now - 20), Value: 2}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writeSharedConfig(t, path, cfg)
	incrementalLease(t, cfg, 1, 0)
	return path, cfg
}

func incrementalLease(t *testing.T, cfg *Config, generation uint64, workers int) {
	t.Helper()
	data := []byte(fmtLease(generation, workers))
	temporary := cfg.Whisper.StoreMigrationControlFile + ".next"
	if err := os.WriteFile(temporary, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, cfg.Whisper.StoreMigrationControlFile); err != nil {
		t.Fatal(err)
	}
}
func fmtLease(generation uint64, workers int) string {
	return fmt.Sprintf(`{"version":1,"generation":%d,"workers":%d,"expires_at":%d}`, generation, workers, time.Now().Add(2*time.Minute).Unix())
}
func startIncrementalApp(t *testing.T, path string) *App {
	t.Helper()
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	waitMigration(t, func() bool {
		return app.Carbonserver.CurrentFileIndex() != nil && app.incrementalMigration.Load() != nil
	})
	return app
}

func TestIncrementalPriorityConvertsLiveMetricBeforeBlockedColdSource(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreMigrationWorkers = 1
	cfg.Whisper.OutOfOrder = true
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	// The sole worker is paused before touching the first lexical source.
	// Hold that source's real lock: cold-first scheduling cannot pass this test.
	w, err := whisper.OpenWithOptions(filepath.Join(cfg.Whisper.DataDir, "first.wsp"), &whisper.Options{FLock: true, OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if w != nil {
			_ = w.Close()
		}
	})
	now := time.Now().Unix()
	app.Cache.Add(points.OnePoint("third", 42.25, now-1))
	app.Cache.Add(points.OnePoint("third", 99.5, now-30))
	m := app.incrementalMigration.Load()
	waitMigration(t, func() bool {
		m.priority.mu.Lock()
		defer m.priority.mu.Unlock()
		_, exists := m.priority.pending["third"]
		return exists
	})
	ctx := context.Background()
	if _, err := app.MetricStore.Metadata(ctx, "third"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("paused conversion published ownership: %v", err)
	}
	incrementalLease(t, cfg, 2, 1)
	waitMigration(t, func() bool {
		_, err := app.MetricStore.Metadata(ctx, "third")
		return err == nil
	})
	if _, err := app.MetricStore.Metadata(ctx, "first"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("locked cold source converted ahead of the live metric: %v", err)
	}
	snapshot, err := app.MetricStore.Snapshot(ctx, "third")
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]float64{now - 1: 42.25, now - 30: 99.5}
	for _, point := range snapshot.Archives[0].Points {
		if value, exists := want[point.Timestamp]; exists && point.Value == value {
			delete(want, point.Timestamp)
		}
	}
	if len(want) != 0 {
		t.Fatalf("prioritized conversion lost live or OOO writes: %v", want)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = nil
	waitMigration(t, func() bool { return m.receipt()["phase"] == "shared" })
}

func TestIncrementalAppContinuesFromRemovedFilesAcrossRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreOOODirectWrites = true
	writeSharedConfig(t, path, cfg)
	source := filepath.Join(cfg.Whisper.DataDir, "first.wsp")
	w, err := whisper.OpenWithOptions(source, &whisper.Options{OutOfOrder: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(time.Now().Unix() - 25), Value: 17.25}}); err != nil {
		t.Fatal(err)
	}
	if w.OutOfOrderPath() == "" {
		t.Fatal("conversion fixture has no sidecar")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	app := startIncrementalApp(t, path)
	ctx := context.Background()
	for _, name := range []string{"first", "second", "third"} {
		if _, err := app.MetricStore.Metadata(ctx, name); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("zero-worker lease converted %s: %v", name, err)
		}
	}
	if _, err := app.Persister.ConvertFile(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	app.Cache.Add(points.OnePoint("first", 42, now-1))
	app.Cache.Add(points.OnePoint("first", 99.5, now-30))
	app.Cache.Add(points.OnePoint("brand.new", 7, now-1))
	waitMigration(t, func() bool {
		snapshot, err := app.MetricStore.Snapshot(ctx, "first")
		if err != nil {
			return false
		}
		fresh, late := false, false
		for _, p := range snapshot.Archives[0].Points {
			fresh = fresh || p.Timestamp == now-1 && p.Value == 42
			late = late || p.Timestamp == now-30 && p.Value == 99.5
		}
		_, err = app.MetricStore.Metadata(ctx, "brand.new")
		return fresh && late && err == nil
	})
	if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, "first.wsp")); !os.IsNotExist(err) {
		t.Fatal("shared-only writer recreated first.wsp")
	}
	if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, "second.wsp")); err != nil {
		t.Fatal("unconverted original was removed")
	}
	before, err := app.MetricStore.Metadata(ctx, "first")
	if err != nil {
		t.Fatal(err)
	}
	app.Stop()
	app = startIncrementalApp(t, path)
	after, err := app.MetricStore.Metadata(ctx, "first")
	if err != nil {
		t.Fatal(err)
	}
	if before.ID != after.ID || before.Generation != after.Generation || before.Revision != after.Revision {
		t.Fatal("restart discarded completed conversion progress")
	}
	for _, name := range []string{"first", "second", "third"} {
		if !app.Carbonserver.MetricExists(name) {
			t.Fatalf("restart lost mixed discovery for %s", name)
		}
	}
	incrementalLease(t, cfg, 2, 4)
	waitMigration(t, func() bool { return app.incrementalMigration.Load().receipt()["phase"] == "shared" })
	if err := app.MetricStore.CheckNoSidecars(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "second", "third"} {
		metadata, err := app.MetricStore.Metadata(ctx, name)
		if err != nil || metadata.FileMigration != store.FileReclaimed {
			t.Fatalf("incomplete conversion %s: %+v %v", name, metadata, err)
		}
		if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, name+".wsp")); !os.IsNotExist(err) {
			t.Fatalf("original retained for %s", name)
		}
	}
	after, err = app.MetricStore.Metadata(ctx, "first")
	if err != nil || after.Generation != before.Generation {
		t.Fatal("remaining walk re-imported the completed metric")
	}
}

func TestIncrementalDiskPauseCancelsWithoutLosingOwnership(t *testing.T) {
	m := &incrementalStoreMigration{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	directory := t.TempDir()
	go func() { _, err := m.waitDisk(ctx, directory, ^uint64(0), 0); done <- err }()
	waitMigration(t, func() bool { return m.spaceWaiting.Load() == 1 })
	release, err := m.waitDisk(context.Background(), directory, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if m.spaceWaiting.Load() != 1 {
		t.Fatal("an admitted worker erased another worker's disk pause")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || m.spaceWaiting.Load() != 0 {
			t.Fatalf("disk waiter failed to cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("disk admission prevented shutdown")
	}
}

func TestIncrementalPendingCleanupRunsWithZeroWorkersAndNoAdmissionSpace(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	ctx := context.Background()
	want, err := whisperio.ReadSnapshot(ctx, "first", filepath.Join(cfg.Whisper.DataDir, "first.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: cfg.Whisper.DataDir})
	if err != nil {
		t.Fatal(err)
	}
	staged, err := db.StageFileSnapshot(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublishFileOwnership(ctx, want, staged); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.StoreMigrationMinFreeBytes = int64(^uint64(0) >> 1)
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	waitMigration(t, func() bool {
		m, err := app.MetricStore.Metadata(ctx, "first")
		return err == nil && m.FileMigration == store.FileReclaimed
	})
	if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, "first.wsp")); !os.IsNotExist(err) {
		t.Fatal("pending cleanup did not remove its original")
	}
	if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, "second.wsp")); err != nil {
		t.Fatal("zero-worker lease allowed another conversion")
	}
	got, err := app.MetricStore.Snapshot(ctx, "first")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal("cleanup changed owned history: ", err)
	}
}

func TestIncrementalDiskReservationsDoNotOvercommitOrDeadlock(t *testing.T) {
	directory := t.TempDir()
	const reservation = 600
	m := &incrementalStoreMigration{diskFree: func(string) (uint64, error) { return 1000, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := m.waitDisk(ctx, directory, reservation, reservation)
	if err != nil {
		t.Fatal(err)
	}
	// Reserve arithmetic only; these test jobs allocate no actual disk blocks.
	second := make(chan func(), 1)
	failure := make(chan error, 1)
	go func() {
		release, err := m.waitDisk(ctx, directory, reservation, reservation)
		if err != nil {
			failure <- err
		} else {
			second <- release
		}
	}()
	waitMigration(t, func() bool { return m.spaceWaiting.Load() == 1 })
	select {
	case release := <-second:
		release()
		t.Fatal("both workers reserved the same free disk space")
	default:
	}
	first()
	select {
	case release := <-second:
		release()
	case err := <-failure:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("headroom release did not allow another metric to progress")
	}
	checkMigrationDiskReleased(t, m)
}

func TestIncrementalMigrationSkipsRejectedSourceAndKeepsIt(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	for _, name := range []string{"broken", "also-broken"} {
		if err := os.WriteFile(filepath.Join(cfg.Whisper.DataDir, name+".wsp"), []byte("not a whisper file, but long enough to look like a header"), 0640); err != nil {
			t.Fatal(err)
		}
	}
	app := startIncrementalApp(t, path)
	defer app.Stop()
	incrementalLease(t, cfg, 2, 4)
	m := app.incrementalMigration.Load()
	waitMigration(t, func() bool { phase := m.receipt()["phase"]; return phase == "shared" || phase == "failed" })
	if r := m.receipt(); r["phase"] != "shared" {
		t.Fatalf("rejected source stopped the run: %v", r["error"])
	}
	ctx := context.Background()
	for _, name := range []string{"first", "second", "third"} {
		if metadata, err := app.MetricStore.Metadata(ctx, name); err != nil || metadata.FileMigration != store.FileReclaimed {
			t.Fatalf("rejected source stopped conversion of %s: %+v %v", name, metadata, err)
		}
	}
	for _, name := range []string{"broken", "also-broken"} {
		if _, err := app.MetricStore.Metadata(ctx, name); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("rejected source %s was published: %v", name, err)
		}
		if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, name+".wsp")); err != nil {
			t.Fatalf("rejected original %s was not kept: %v", name, err)
		}
	}
	if skipped := m.receipt()["skipped"]; skipped != uint64(2) {
		t.Fatalf("skipped = %v", skipped)
	}
	<-m.done
	if metrics := m.receipt()["skipped_metrics"].(map[string]string); len(metrics) != 2 || metrics["broken"] == "" || metrics["also-broken"] == "" {
		t.Fatalf("skipped metric names and errors missing: %v", metrics)
	}
	data, err := os.ReadFile(filepath.Join(cfg.Whisper.StoreDir, incrementalReceiptName))
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		SkippedMetrics map[string]string `json:"skipped_metrics"`
	}
	if err := json.Unmarshal(data, &receipt); err != nil || len(receipt.SkippedMetrics) != 2 || receipt.SkippedMetrics["broken"] == "" || receipt.SkippedMetrics["also-broken"] == "" {
		t.Fatalf("skipped metric names not saved in receipt: %s %v", data, err)
	}
	if completedIncrementalMigration(cfg) || m.sourceComplete.Load() {
		t.Fatal("skipped source marked migration complete")
	}
	for _, name := range []string{"broken", "also-broken"} {
		broken := filepath.Join(cfg.Whisper.DataDir, name+".wsp")
		if err := os.Remove(broken); err != nil {
			t.Fatal(err)
		}
		w, err := whisper.CreateWithOptions(broken, whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 60)}), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	db := app.MetricStore
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	m = app.incrementalMigration.Load()
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		t.Fatal("repaired source was not retried on reload")
	}
	if r := m.receipt(); r["phase"] != "shared" || r["skipped"] != uint64(0) || r["source_complete"] != true || len(r["skipped_metrics"].(map[string]string)) != 0 {
		t.Fatalf("retry did not clear repaired source: %v", r)
	}
	if app.MetricStore != db || !completedIncrementalMigration(cfg) {
		t.Fatal("retry did not complete on the same open store")
	}
	for _, name := range []string{"broken", "also-broken"} {
		if metadata, err := db.Metadata(ctx, name); err != nil || metadata.FileMigration != store.FileReclaimed {
			t.Fatalf("repaired source %s was not converted: %+v %v", name, metadata, err)
		}
		if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, name+".wsp")); !os.IsNotExist(err) {
			t.Fatalf("repaired source %s was not reclaimed: %v", name, err)
		}
	}
}
