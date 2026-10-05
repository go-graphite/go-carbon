package carbon

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
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
	path := TestConfig(root)
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
	path, cfg := sharedAppConfig(t)
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
	now := time.Now().Unix()
	app.Cache.Add(points.OnePoint("shared.cpu", 42, now-2))
	waitForSharedPoint(t, app.MetricStore)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
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
	app.Stop()
	if app.MetricStore != nil || app.Buckyd != nil || app.Carbonserver != nil {
		t.Fatal("components retained after Stop")
	}
	cfg.Whisper.StoreDir = strings.TrimSuffix(cfg.Whisper.StoreDir, "-changed")
	db, err := store.Open(cfg.Whisper.StoreDir, store.Options{})
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

func TestSharedAppBindFailureReleasesStore(t *testing.T) {
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
		{"legacy pebble", func(c *Config) { c.Whisper.StorageBackend = "pebble" }},
		{"migration", func(c *Config) { c.Whisper.OnlineMigration = true }},
		{"physical quota", func(c *Config) { c.Whisper.Quotas = persister.WhisperQuotas{{PhysicalSize: 1}} }},
		{"small memtable", func(c *Config) { c.Whisper.StoreMemTableSize = 1 }},
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

func TestStoreStatsExposeCacheAndChunkCounters(t *testing.T) {
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
	for _, name := range []string{"diskBytes", "walBytes", "memTableBytes", "cacheBytes", "cacheHits", "cacheMisses", "chunkMaterializations", "chunkOperands"} {
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
