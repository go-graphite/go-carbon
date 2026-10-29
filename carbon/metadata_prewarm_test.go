package carbon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lomik/zapwriter"

	"github.com/go-graphite/go-carbon/points"
)

func TestMetadataPrewarmConfig(t *testing.T) {
	t.Parallel()
	_, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMetadataCachePrewarmWorkers = -1
	if err := validateStorageConfig(cfg); err == nil || !strings.Contains(err.Error(), "prewarm") {
		t.Fatalf("negative prewarm workers accepted: %v", err)
	}
	cfg.Whisper.StoreMetadataCachePrewarmWorkers, cfg.Whisper.StoreMetadataCachePrewarmRate = 0, -1
	if err := validateStorageConfig(cfg); err == nil || !strings.Contains(err.Error(), "prewarm") {
		t.Fatalf("negative prewarm rate accepted: %v", err)
	}
	cfg.Whisper.StoreMetadataCachePrewarmRate = 0
	next := *cfg
	next.Whisper.StoreMetadataCachePrewarm = true
	next.Whisper.StoreMetadataCachePrewarmWorkers = 8
	// Read at start only: changing it needs no restart and fails no reload.
	if storageSettingsChanged(cfg, &next) {
		t.Fatal("prewarm settings require a restart")
	}
}

// TestMetadataPrewarmAfterDumpRestart restarts an incrementally migrating
// store (the continuation codec) whose cache held unpersisted points of
// converted metrics, and checks that the start prewarms their metadata from
// the pending checkpoint.
func TestMetadataPrewarmAfterDumpRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	cfg.Dump.Enabled, cfg.Dump.Path = true, filepath.Join(t.TempDir(), "dump")
	if err := os.MkdirAll(cfg.Dump.Path, 0755); err != nil {
		t.Fatal(err)
	}
	cfg.Whisper.StoreMetadataCacheSize = 16 << 20
	cfg.Whisper.StoreMetadataCachePrewarm = true
	cfg.Whisper.StoreMetadataCachePrewarmWorkers = 2
	// Scans map the read index a pending checkpoint is written against.
	cfg.Carbonserver.ScanFrequency = &Duration{20 * time.Millisecond}
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	ctx := context.Background()
	if done := app.metadataPrewarmDone.Load(); done != nil {
		<-*done // the first start restored nothing
	}
	names := []string{"first", "second", "third"}
	now := time.Now().Unix()
	for _, name := range names {
		if _, err := app.Persister.ConvertFile(ctx, name); err != nil {
			t.Fatal(err)
		}
		app.Cache.Add(points.OnePoint(name, 1, now-10))
	}
	waitMigration(t, func() bool { return app.Cache.IsEmpty() && app.Carbonserver.HasMappedIndex() })
	// Points the persister no longer writes stay in the cache for the dump.
	app.Lock()
	app.Persister.Stop()
	app.Persister = nil
	app.Unlock()
	for _, name := range names {
		app.Cache.Add(points.OnePoint(name, 2, now-5))
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	app.ReleaseForHandoff()
	app.Stop()

	next := startIncrementalApp(t, path)
	done := next.metadataPrewarmDone.Load()
	if done == nil {
		t.Fatal("no metadata prewarm started", zapwriter.TestString())
	}
	select {
	case <-*done:
	case <-time.After(10 * time.Second):
		t.Fatal("metadata prewarm did not finish")
	}
	logs := zapwriter.TestString()
	if !logHas(logs, "source", `"pending checkpoint"`) || !logHas(logs, "names", "3") || !logHas(logs, "failed", "0") {
		t.Fatalf("prewarm log:\n%s", logs)
	}
	stats := next.MetricStore.Stats().StateCache
	// The persister writes the restored points meanwhile: a metric it wrote
	// first is skipped as cached, never read twice into the cache.
	if stats.Prewarmed == 0 || stats.Prewarmed+stats.PrewarmSkipped != 3 || stats.PrewarmFailed != 0 {
		t.Fatalf("prewarm counters %+v\n%s", stats, logs)
	}
	waitMigration(t, func() bool { return next.Cache.IsEmpty() })
	for _, name := range names {
		snapshot, err := next.MetricStore.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, p := range snapshot.Archives[0].Points {
			found = found || p.Timestamp == now-5 && p.Value == 2
		}
		if !found {
			t.Fatalf("%s: restored point lost", name)
		}
	}
}
