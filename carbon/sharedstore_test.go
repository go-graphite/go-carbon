package carbon

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/lomik/zapwriter"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
)

func writeSharedConfig(t *testing.T, path string, cfg *Config) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = toml.NewEncoder(f).Encode(cfg)
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

func sharedAppConfig(t *testing.T) (string, *Config) {
	t.Helper()
	root := t.TempDir()
	path := testConfig(t, root)
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Udp.Enabled, cfg.Tcp.Enabled, cfg.Pickle.Enabled = false, false, false
	cfg.Grpc.Enabled, cfg.Carbonlink.Enabled = false, false
	cfg.Pprof.Enabled, cfg.Prometheus.Enabled = false, false
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.StoreDir = filepath.Join(root, "shared")
	cfg.Whisper.DataDir = filepath.Join(root, "unused-file-directory")
	cfg.Whisper.StoreCacheSize = 1 << 20
	cfg.Whisper.StoreMemTableSize = 1 << 20
	cfg.Buckyd.Enabled, cfg.Buckyd.Bind = true, "127.0.0.1:0"
	cfg.Carbonserver.Enabled, cfg.Carbonserver.Listen = true, "127.0.0.1:0"
	cfg.Carbonserver.ScanFrequency = &Duration{100 * time.Millisecond}
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[default]\npattern = .*\nretentions = 1s:2m\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	return path, cfg
}

func TestSharedAppPersistsReloadsAndReopens(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, encoding := range []string{"legacy", "exact"} {
		t.Run(encoding, func(t *testing.T) { checkSharedAppPersistsReloadsAndReopens(t, encoding) })
	}
}

func checkSharedAppPersistsReloadsAndReopens(t *testing.T, encoding string) {
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreEncoding = encoding
	cfg.Cache.WriteStrategy = "lexical"
	cfg.Whisper.StoreCompactionWorkers = 4
	cfg.Whisper.StoreL0CompactionConcurrency = 2
	cfg.Whisper.StoreMaxOpenFiles = 16384
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Stop)
	if app.MetricStore == nil || app.Buckyd == nil || app.Carbonserver == nil {
		t.Fatal("shared components missing")
	}
	if got := app.MetricStore.Stats().CompactionWorkers; got != 4 {
		t.Fatalf("running store compaction workers = %d, want 4", got)
	}
	now := time.Now().Unix()
	app.Cache.Add(points.OnePoint("shared.cpu", 42, now-2))
	waitForSharedPoint(t, app.MetricStore)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	checkSharedAppReloadRequiresRestart(t, app, path, cfg)
	app.Stop()
	if app.MetricStore != nil || app.Buckyd != nil || app.Carbonserver != nil {
		t.Fatal("components retained after Stop")
	}
	cfg.Whisper.StoreDir = strings.TrimSuffix(cfg.Whisper.StoreDir, "-changed")
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{ExactBlocks: encoding == "exact"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Metadata(context.Background(), "shared.cpu"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfg.Whisper.DataDir); !os.IsNotExist(err) {
		t.Fatalf("file directory used by shared backend: %v", err)
	}
}

func checkSharedAppReloadRequiresRestart(t *testing.T, app *App, path string, cfg *Config) {
	t.Helper()
	for _, change := range []func(){
		func() { cfg.Whisper.StoreL0CompactionConcurrency++ },
		func() { cfg.Whisper.StoreMaxOpenFiles++ },
	} {
		workers, step, files := cfg.Whisper.StoreCompactionWorkers, cfg.Whisper.StoreL0CompactionConcurrency, cfg.Whisper.StoreMaxOpenFiles
		change()
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err == nil || !strings.Contains(err.Error(), "restart") {
			t.Fatalf("static compaction settings changed during reload: %v", err)
		}
		cfg.Whisper.StoreCompactionWorkers, cfg.Whisper.StoreL0CompactionConcurrency = workers, step
		cfg.Whisper.StoreMaxOpenFiles = files
	}
	cfg.Whisper.StoreSyncInterval = &Duration{0}
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("sync interval changed during reload: %v", err)
	}
	cfg.Whisper.StoreSyncInterval = &Duration{time.Second}
	cfg.Whisper.StoreDir += "-changed"
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("storage changed during reload: %v", err)
	}
}

func TestSharedAppBindFailureReleasesStore(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cfg.Buckyd.Bind = listener.Addr().String()
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err == nil {
		t.Fatal("occupied buckyd port accepted")
	}
	if app.MetricStore != nil || app.Cache != nil {
		t.Fatal("failed Start leaked shared storage")
	}
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSharedStorageConfigRejectsUnsupportedPolicies(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*Config)
	}{
		{"unknown backend", func(c *Config) { c.Whisper.StorageBackend = "other" }},
		{"unknown encoding", func(c *Config) { c.Whisper.StoreEncoding = "other" }},
		{"exact file encoding", func(c *Config) {
			c.Buckyd.Enabled = false
			c.Whisper.StorageBackend = "files"
			c.Whisper.StoreEncoding = "exact"
		}},
		{"legacy pebble", func(c *Config) { c.Whisper.StorageBackend = "pebble" }},
		{"migration", func(c *Config) { c.Whisper.OnlineMigration = true }},
		{"physical quota", func(c *Config) { c.Whisper.Quotas = persister.WhisperQuotas{{PhysicalSize: 1}} }},
		{"small memtable", func(c *Config) { c.Whisper.StoreMemTableSize = 1 }},
		{"invalid compaction rate", func(c *Config) { c.Whisper.StoreCompactionRate = -2 }},
		{"negative compaction workers", func(c *Config) { c.Whisper.StoreCompactionWorkers = -1 }},
		{"large compaction workers", func(c *Config) { c.Whisper.StoreCompactionWorkers = 65 }},
		{"negative L0 concurrency", func(c *Config) { c.Whisper.StoreL0CompactionConcurrency = -1 }},
		{"large L0 concurrency", func(c *Config) { c.Whisper.StoreL0CompactionConcurrency = 65 }},
		{"negative table-reader budget", func(c *Config) { c.Whisper.StoreMaxOpenFiles = -1 }},
		{"small table-reader budget", func(c *Config) { c.Whisper.StoreMaxOpenFiles = 63 }},
		{"large table-reader budget", func(c *Config) { c.Whisper.StoreMaxOpenFiles = 1048577 }},
		{"negative sync interval", func(c *Config) { c.Whisper.StoreSyncInterval = &Duration{-time.Second} }},
		{"file buckyd", func(c *Config) { c.Whisper.StorageBackend = "files" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Whisper.StorageBackend = "pebble-chunk"
			cfg.Buckyd.Enabled = true
			tt.change(cfg)
			err := validateStorageConfig(cfg)
			if err == nil {
				t.Fatal("invalid storage configuration accepted")
			}
			if tt.name == "legacy pebble" && !strings.Contains(err.Error(), "migrate legacy data through buckyd") {
				t.Fatalf("legacy backend guidance = %v", err)
			}
		})
	}
}

func TestSharedStorageMigrationWorkerLimits(t *testing.T) {
	root := t.TempDir()
	for _, mode := range []string{"incremental", "live"} {
		t.Run(mode, func(t *testing.T) {
			for _, workers := range []int{-1, 0, 16384, 16385, 65536, 65537} {
				t.Run(fmt.Sprint(workers), func(t *testing.T) {
					cfg := NewConfig()
					cfg.Whisper.StorageBackend = "pebble-chunk"
					cfg.Whisper.StoreEncoding = "exact"
					cfg.Whisper.DataDir = filepath.Join(root, "original")
					cfg.Whisper.StoreDir = filepath.Join(root, "shared")
					cfg.Whisper.Enabled, cfg.Whisper.FLock = true, true
					cfg.Whisper.StoreIncrementalMigration = mode == "incremental"
					cfg.Whisper.StoreLiveMigration = mode == "live"
					cfg.Whisper.StoreMirrorFiles = mode == "live"
					cfg.Whisper.StoreMigrationWorkers = workers
					cfg.Carbonserver.Enabled, cfg.Carbonserver.TrieIndex = true, true
					cfg.Carbonserver.FileListCache = filepath.Join(root, "files.flc")
					cfg.Carbonserver.FileListCacheVersion = 2
					err := validateStorageConfig(cfg)
					if workers < 0 || workers > 65536 {
						if err == nil || !strings.Contains(err.Error(), "0..65536") {
							t.Fatalf("workers = %d: want worker limit error, got %v", workers, err)
						}
					} else if err != nil {
						t.Fatalf("workers = %d: %v", workers, err)
					}
				})
			}
		})
	}
}

func TestSharedStorageMigrationWorkerLeaseLimits(t *testing.T) {
	for _, tt := range []struct {
		maximum, workers, want int
	}{
		{65536, 65536, 65536},
		{65536, 65537, 1},
		{16384, 16385, 1},
	} {
		t.Run(fmt.Sprintf("%d/%d", tt.maximum, tt.workers), func(t *testing.T) {
			now := time.Now()
			path := filepath.Join(t.TempDir(), "control.json")
			writeMigrationLease(t, path, tt.workers, 1, now.Add(time.Minute))
			budget := newMigrationBudget(tt.maximum)
			defer budget.stop()
			budget.refresh(path, now)
			if limit, _, _ := budget.status(); limit != tt.want {
				t.Fatalf("worker limit = %d, want %d", limit, tt.want)
			}
		})
	}
}

func TestSharedStorageSyncIntervalConfig(t *testing.T) {
	for _, tt := range []struct {
		name string
		text string
		want time.Duration
	}{
		{name: "default", want: time.Second},
		{name: "synchronous", text: `store-sync-interval = "0s"`},
		{name: "custom", text: `store-sync-interval = "250ms"`, want: 250 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "carbon.conf")
			if err := os.WriteFile(path, []byte("[whisper]\nstorage-backend = \"pebble-chunk\"\n"+tt.text), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateStorageConfig(cfg); err != nil {
				t.Fatal(err)
			}
			if got := cfg.Whisper.StoreSyncInterval.Value(); got != tt.want {
				t.Fatalf("sync interval = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSharedStorageAllowsUnlimitedPhysicalQuota(t *testing.T) {
	cfg := NewConfig()
	cfg.Whisper.StorageBackend = "pebble-chunk"
	cfg.Whisper.Quotas = persister.WhisperQuotas{{Pattern: "*", PhysicalSize: math.MaxInt64}}
	if err := validateStorageConfig(cfg); err != nil {
		t.Fatalf("unlimited physical quota rejected: %v", err)
	}
}

func TestStoreStatsExposeCacheAndChunkCounters(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	db, err := store.Open(t.TempDir(), store.Options{CacheSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	config := store.MetricConfig{Name: "stats.metric", Retentions: []store.Retention{{Step: 60, Count: 10}}, AggregationMethod: store.Average}
	if _, err := db.Create(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateMany(context.Background(), config.Name, []store.Point{{Timestamp: time.Now().Unix(), Value: 1}}); err != nil {
		t.Fatal(err)
	}
	stats := &storeStats{db: db}
	got := make(map[string]float64)
	stats.Stat(func(name string, value float64) { got[name] = value })
	for _, name := range []string{"diskBytes", "walBytes", "memTableBytes", "cacheBytes", "cacheHits", "cacheMisses", "chunkMaterializations", "chunkOperands", "reconcileComparedChunks", "reconcileWrittenChunks", "reconcileDeletedChunks", "reconcileUnchangedMetrics", "compactionWorkers", "compactionDebt", "compactionPacerTargetRate", "compactionPacerAuto", "compactionPacerBypass", "l0Sublevels", "writeStalls", "writeStallSeconds"} {
		if _, ok := got[name]; !ok {
			t.Fatalf("missing storage metric %q", name)
		}
	}
	if got["chunkMaterializations"] != 1 {
		t.Fatalf("chunkMaterializations = %v, want 1", got["chunkMaterializations"])
	}
	stats.Stat(func(name string, value float64) { got[name] = value })
	if got["chunkMaterializations"] != 0 || got["chunkOperands"] != 0 {
		t.Fatalf("counters must report deltas between flushes, got materializations=%v operands=%v", got["chunkMaterializations"], got["chunkOperands"])
	}
	if err := db.SetCompactionRate(-1); err != nil {
		t.Fatal(err)
	}
	stats.Stat(func(name string, value float64) { got[name] = value })
	if got["compactionPacerBypass_warmup"] != 1 || got["compactionPacerAuto"] != 1 || got["compactionPacerBypass"] != 1 {
		t.Fatalf("auto pacing stats missing: %v", got)
	}
	stats.Stat(func(name string, value float64) { got[name] = value })
	if got["compactionPacerBypass_warmup"] != 0 {
		t.Fatal("bypass counters must report interval deltas")
	}
}

func TestSharedStoreRejectsIncompleteConversion(t *testing.T) {
	for _, marker := range []string{"CONVERSION-IN-PROGRESS", "NEEDS-CATCHUP"} {
		t.Run(marker, func(t *testing.T) {
			path, cfg := sharedAppConfig(t)
			if err := os.MkdirAll(cfg.Whisper.StoreDir, 0750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cfg.Whisper.StoreDir, marker), []byte("incomplete"), 0600); err != nil {
				t.Fatal(err)
			}
			app := New(path)
			defer app.Stop()
			if err := app.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			if err := app.Start(); err == nil || !strings.Contains(err.Error(), "not ready") {
				t.Fatalf("incomplete store selected: %v", err)
			}
		})
	}
}

func TestStorageSettingsChangedIgnoresDataDirForFileBackend(t *testing.T) {
	old, next := NewConfig(), NewConfig()
	next.Whisper.DataDir = old.Whisper.DataDir + "-moved"
	if storageSettingsChanged(old, next) {
		t.Fatal("data-dir change with file backend must stay hot-reloadable")
	}
	old.Whisper.StorageBackend, next.Whisper.StorageBackend = "pebble-chunk", "pebble-chunk"
	if !storageSettingsChanged(old, next) {
		t.Fatal("derived store path change with pebble backend must require restart")
	}
	next.Whisper.DataDir = old.Whisper.DataDir
	next.Buckyd.Enabled = !old.Buckyd.Enabled
	if !storageSettingsChanged(old, next) {
		t.Fatal("buckyd change must require restart")
	}
}

func waitForSharedPoint(t *testing.T, db *store.Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := db.Snapshot(context.Background(), "shared.cpu")
		if err == nil && len(snap.Archives[0].Points) == 1 && snap.Archives[0].Points[0].Value == 42 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric not persisted: %+v %v", snap, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReloadTunesCompactionWorkersAndRelayoutRate(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreTimeBuckets = true
	cfg.Whisper.StoreTimeBucketsRelayoutRate = 1 << 20
	cfg.Whisper.StoreCompactionWorkers = 4
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	store := app.MetricStore
	if got := store.Stats().CompactionWorkers; got != 4 {
		t.Fatalf("compaction workers %d, want 4", got)
	}
	next := *cfg
	next.Whisper.StoreCompactionWorkers = 2
	next.Whisper.StoreTimeBucketsRelayoutRate = 1
	if storageSettingsChanged(cfg, &next) {
		t.Fatal("compaction workers and a positive relayout rate must hot-reload")
	}
	writeSharedConfig(t, path, &next)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.MetricStore != store {
		t.Fatal("reload replaced the store")
	}
	if got := store.Stats().CompactionWorkers; got != 2 {
		t.Fatalf("reloaded compaction workers %d, want 2", got)
	}
	if got := store.RelayoutStats().Rate; got != 1 {
		t.Fatalf("reloaded relayout rate %d, want 1", got)
	}
	off := next
	off.Whisper.StoreTimeBucketsRelayoutRate = 0
	if !storageSettingsChanged(&next, &off) {
		t.Fatal("disabling relayout must require a restart")
	}
}

func TestReloadCompactionRate(t *testing.T) {
	path, cfg := incrementalAppFixture(t)
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	db := app.MetricStore
	if got := db.Stats().CompactionRate; got != 0 {
		t.Fatalf("default rate = %d", got)
	}
	for _, rate := range []int64{1 << 20, -1, 2 << 20, 0} {
		next := *cfg
		next.Whisper.StoreCompactionRate = rate
		if storageSettingsChanged(cfg, &next) {
			t.Fatal("compaction rate requires restart")
		}
		writeSharedConfig(t, path, &next)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		st := db.Stats()
		if rate == -1 {
			if !st.CompactionPacerAuto || st.CompactionPacerBypass == "" {
				t.Fatalf("auto reload did not bypass during warmup/conversion: %+v", st)
			}
			rate = 0
		}
		if app.MetricStore != db || st.CompactionRate != rate {
			t.Fatalf("rate reload did not preserve store and set rate %d", rate)
		}
	}
}

func TestAutomaticCompactionPacingMigration(t *testing.T) {
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreCompactionRate = -1
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	if st := app.MetricStore.Stats(); st.CompactionPacerBypass != "warmup" {
		t.Fatalf("idle running migration bypassed for conversion: %+v", st)
	}
	m := app.incrementalMigration.Load()
	incrementalLease(t, cfg, 2, 4)
	select {
	case <-m.done:
	case <-time.After(10 * time.Second):
		t.Fatal("small migration did not finish")
	}
	if m.active.Load() != 0 || m.metrics.Load() == 0 {
		t.Fatal("small migration did not become idle after completing conversions")
	}
	// Refresh from the counters even if every conversion finished between
	// one-second observations; goroutine completion must not clear the hold.
	if err := app.MetricStore.SetCompactionRate(-1); err != nil {
		t.Fatal(err)
	}
	if st := app.MetricStore.Stats(); st.CompactionPacerBypass != "conversion" {
		t.Fatalf("recent conversion did not hold bypass: %+v", st)
	}
}

// A dump stop flushes the shared store when it begins and again once the
// persister stopped, so the flush that must finish after input closed has
// only the remainder; the successor reopens with nothing to replay.
func TestDumpStopFlushesSharedStoreEarly(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.StoreMemTableSize = 64 << 20 // the point stays in a memtable
	cfg.Dump.Enabled, cfg.Dump.Path = true, filepath.Join(t.TempDir(), "dump")
	if err := os.MkdirAll(cfg.Dump.Path, 0755); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	app.Cache.Add(points.OnePoint("shared.cpu", 42, now-2))
	waitForSharedPoint(t, app.MetricStore)
	if app.MetricStore.Stats().MemTableBytes == 0 {
		t.Fatal("no memtable to flush")
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	logs := zapwriter.TestString()
	if !strings.Contains(logs, "shared store flushed with input active") || !logHas(logs, "late_flush", "false") {
		t.Fatalf("stop did not flush before input closed:\n%s", logs)
	}
	app.ReleaseForHandoff()
	app.Stop()
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if snap, err := db.Snapshot(context.Background(), "shared.cpu"); err != nil || snap.Archives[0].Points[0].Value != 42 {
		t.Fatal("flushed point lost", snap, err)
	}
	if stats := db.Stats(); stats.WALBytes > 1<<20 {
		t.Fatal("successor replays a WAL", stats.WALBytes)
	}
}
