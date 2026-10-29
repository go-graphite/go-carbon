package carbon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func legacyIncrementalAppFixture(t *testing.T) (string, *Config) {
	t.Helper()
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreEncoding = "exact"
	cfg.Whisper.StoreIncrementalMigration = true
	cfg.Whisper.StoreOOODirectWrites = true
	cfg.Whisper.FLock = true
	cfg.Whisper.Compressed = true
	cfg.Whisper.StoreMigrationWorkers = 1
	cfg.Whisper.StoreMigrationMinFreeBytes = 0
	cfg.Whisper.StoreMigrationBytesInFlight = 1 << 20
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
	writeSharedConfig(t, path, cfg)
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: cfg.Whisper.DataDir, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	_, createErr := db.Create(context.Background(), store.MetricConfig{Name: "converted", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average})
	if err := errors.Join(createErr, db.Close()); err != nil {
		t.Fatal(err)
	}
	// Simulate a store predating the inventory marker, with no source files
	// left. The empty metric contains no merge operands.
	raw, err := pebble.Open(cfg.Whisper.StoreDir, &pebble.Options{MemTableSize: 1 << 20, Merger: &pebble.Merger{
		Name: "go-carbon.chunks.exact.v1",
		Merge: func([]byte, []byte) (pebble.ValueMerger, error) {
			return nil, errors.New("unexpected merge operand in empty fixture")
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(raw.Delete([]byte("z/sidecars-ready"), pebble.Sync), raw.Close()); err != nil {
		t.Fatal(err)
	}
	return path, cfg
}

func TestIncrementalEmptySourceWaitsForSidecarInventory(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := legacyIncrementalAppFixture(t)
	app := startIncrementalApp(t, path)
	m := app.incrementalMigration.Load()
	waitMigration(t, func() bool { return m.receipt()["phase"] != "files" && m.receipt()["phase"] != "index" })
	if r := m.receipt(); r["phase"] != "sidecars" || r["source_complete"] != false {
		t.Fatalf("empty source finalized before the inventory scan: %v", r)
	}
	if entries, err := os.ReadDir(cfg.Whisper.DataDir); err != nil || len(entries) != 0 {
		t.Fatalf("source tree is not empty: %v %v", entries, err)
	}
	if app.MetricStore.Sidecars().Ready {
		t.Fatal("legacy inventory was already ready")
	}
	ctx := context.Background()
	if _, err := app.MetricStore.ScanSidecars(ctx, "", 10, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.done:
		t.Fatal("dry scan completed migration")
	default:
	}
	if _, err := app.MetricStore.ScanSidecars(ctx, "", 10, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		t.Fatal("inventory completion did not resume migration")
	}
	if r := m.receipt(); r["phase"] != "shared" || r["source_complete"] != true || !completedIncrementalMigration(cfg) {
		t.Fatalf("scan did not complete migration: %v", r)
	}
	// Catalog-only discovery must no longer walk the original tree.
	if err := os.Symlink(t.TempDir(), filepath.Join(cfg.Whisper.DataDir, "must-not-walk")); err != nil {
		t.Fatal(err)
	}
	if err := app.Carbonserver.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
}

func TestIncrementalSidecarInventoryWaitCancels(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := legacyIncrementalAppFixture(t)
	app := startIncrementalApp(t, path)
	m := app.incrementalMigration.Load()
	waitMigration(t, func() bool { return m.receipt()["phase"] == "sidecars" })
	app.stopIncrementalStoreMigration()
	if r := m.receipt(); r["phase"] != "cancelled" || r["source_complete"] != false || completedIncrementalMigration(cfg) {
		t.Fatalf("cancelled scan wait completed migration: %v", r)
	}
	if app.MetricStore.Sidecars().Ready {
		t.Fatal("cancellation changed inventory readiness")
	}
}

func TestIncrementalSidecarCheckRejectsRemainingSidecars(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := legacyIncrementalAppFixture(t)
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: cfg.Whisper.DataDir, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	ctx := context.Background()
	for _, batch := range [][]store.Point{{{Timestamp: now - 20, Value: 1}, {Timestamp: now - 10, Value: 2}}, {{Timestamp: now - 15, Value: 3}}} {
		if err := db.UpdateMany(ctx, "converted", batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	app := startIncrementalApp(t, path)
	m := app.incrementalMigration.Load()
	waitMigration(t, func() bool { return m.receipt()["phase"] == "sidecars" })
	if _, err := app.MetricStore.ScanSidecars(ctx, "", 10, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		t.Fatal("nonempty inventory did not finish the check")
	}
	if r := m.receipt(); r["phase"] != "failed" || r["source_complete"] != false || !strings.Contains(r["error"].(string), "ready=true count=1") || completedIncrementalMigration(cfg) {
		t.Fatalf("nonempty inventory did not fail migration: %v", r)
	}
}

func TestIncrementalReadinessFailureRetriesOnReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := legacyIncrementalAppFixture(t)
	app := startIncrementalApp(t, path)
	db := app.MetricStore
	m := app.incrementalMigration.Load()
	waitMigration(t, func() bool { return m.receipt()["phase"] == "sidecars" })
	err := db.CheckNoSidecars()
	if err == nil {
		t.Fatal("legacy check did not reproduce readiness failure")
	}
	app.stopIncrementalStoreMigration()
	// Recreate the terminal state left by the old final check. A config reload
	// must discard it and retry using the same open store and source directory.
	m.mu.Lock()
	m.phase, m.failure = "failed", err.Error()
	m.mu.Unlock()
	if err := m.progress(sharedStorePath(cfg), true); err != nil {
		t.Fatal(err)
	}
	app.incrementalMigration.Store(m)
	if _, err := db.ScanSidecars(context.Background(), "", 10, true); err != nil {
		t.Fatal(err)
	}
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	m = app.incrementalMigration.Load()
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness failure did not recover on reload")
	}
	if r := m.receipt(); r["phase"] != "shared" || r["error"] != "" || r["source_complete"] != true || !completedIncrementalMigration(cfg) || app.MetricStore != db {
		t.Fatalf("reload did not recover readiness failure on the same store: %v", r)
	}
}
