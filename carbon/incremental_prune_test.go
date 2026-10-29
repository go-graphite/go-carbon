package carbon

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIncrementalCompletedUsesCatalogWithoutSourceWalkOnRestart(t *testing.T) {
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreMigrationReclaimDelay = &Duration{0}
	writeSharedConfig(t, path, cfg)
	incrementalLease(t, cfg, 2, 4)
	app := startIncrementalApp(t, path)
	m := app.incrementalMigration.Load()
	select {
	case <-m.done:
	case <-time.After(10 * time.Second):
		t.Fatal("conversion did not finish")
	}
	if m.receipt()["phase"] != "shared" || !completedIncrementalMigration(cfg) {
		t.Fatalf("receipt=%v", m.receipt())
	}
	// This would fail the original-tree walk. Catalog scans, file-list cache
	// startup and quota refresh must not inspect it after complete conversion.
	if err := os.Symlink(t.TempDir(), filepath.Join(cfg.Whisper.DataDir, "must-not-walk")); err != nil {
		t.Fatal(err)
	}
	if err := app.Carbonserver.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if !app.Carbonserver.HasMappedIndex() {
		t.Fatal("completed catalog lost mapped index")
	}
	app.Stop()
	// A restart can die after writing its nonterminal cleanup receipt. Source
	// completion must survive that receipt, so another restart never re-walks.
	m.mu.Lock()
	m.phase, m.failure = "files", "interrupted cleanup"
	m.mu.Unlock()
	if err := m.progress(sharedStorePath(cfg), true); err != nil {
		t.Fatal(err)
	}
	app = startIncrementalApp(t, path)
	m = app.incrementalMigration.Load()
	select {
	case <-m.done:
	case <-time.After(10 * time.Second):
		t.Fatal("restart did not finish")
	}
	if m.receipt()["phase"] != "shared" {
		t.Fatalf("restart walked the source: %v", m.receipt())
	}
	if err := app.Carbonserver.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	if !app.Carbonserver.HasMappedIndex() {
		t.Fatal("completed catalog lost mapped index")
	}
}

func TestIncrementalCompletedReceiptRequiresNoSkippedSources(t *testing.T) {
	cfg := NewConfig()
	cfg.Whisper.StoreDir = t.TempDir()
	cfg.Whisper.StoreIncrementalMigration = true
	for _, tc := range []struct {
		body     string
		complete bool
	}{
		{`{"mode":"incremental","phase":"shared","in_flight":0,"skipped":0}`, true},
		{`{"mode":"incremental","phase":"shared","in_flight":0,"skipped":1}`, false},
		{`{"mode":"incremental","phase":"shared","in_flight":1,"skipped":0}`, false},
		{`{"mode":"incremental","phase":"files","in_flight":0,"skipped":0}`, false},
		{`{"mode":"incremental","phase":"shared"}`, false},
		{`{"mode":"incremental","phase":"files","in_flight":1,"skipped":0,"source_complete":true}`, true},
		{`{"mode":"incremental","phase":"failed","in_flight":0,"skipped":0,"error":"cleanup failed","source_complete":true}`, true},
		{`invalid`, false},
	} {
		if err := os.WriteFile(filepath.Join(cfg.Whisper.StoreDir, incrementalReceiptName), []byte(tc.body), 0600); err != nil {
			t.Fatal(err)
		}
		if got := completedIncrementalMigration(cfg); got != tc.complete {
			t.Fatalf("%s: complete=%t", tc.body, got)
		}
	}
}
