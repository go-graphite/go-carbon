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
	cfg.Whisper.StorageBackend = "pebble-chunk"
	enabled.Whisper.StorageBackend = "pebble-chunk"
	if !storageSettingsChanged(cfg, enabled) {
		t.Fatal("range-reads changes must require restart")
	}
}
