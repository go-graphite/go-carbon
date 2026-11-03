package carbon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStoreRangeReadsConfig(t *testing.T) {
	cfg := NewConfig()
	if cfg.Whisper.StoreRangeReads {
		t.Fatal("range reads must default off")
	}
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	if err := os.WriteFile(path, []byte("[whisper]\nstore-range-reads = true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	enabled, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled.Whisper.StoreRangeReads || len(enabled.unknownKeys) != 0 {
		t.Fatal("range-reads config was not consumed")
	}
	before := *enabled
	before.Whisper.StoreRangeReads = false
	before.Whisper.StorageBackend = "pebble-chunk"
	enabled.Whisper.StorageBackend = "pebble-chunk"
	if storageSettingsChanged(&before, enabled) {
		t.Fatal("range-reads changes must not require restart")
	}
}

// ReloadConfig is the SIGHUP handler's normal reload path. The existing store
// and its readers stay open while the immutable policy snapshot changes.
func TestStoreRangeReadsReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.Enabled = false
	cfg.Buckyd.Enabled, cfg.Carbonserver.Enabled = false, false
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	db := app.MetricStore
	if db == nil {
		t.Fatal("missing shared store")
	}
	for _, enabled := range []bool{true, false, true, false} {
		cfg.Whisper.StoreRangeReads = enabled
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.MetricStore != db || app.readPolicyConfig.Load().Whisper.StoreRangeReads != enabled {
			t.Fatalf("range policy %v not published to existing store", enabled)
		}
	}
}
