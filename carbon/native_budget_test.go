package carbon

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeCombineBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		workers   int
		memory    int64
		ram       uint64
		detectErr bool
		invalid   bool
	}{
		{name: "default without detection", workers: 2, memory: 2 << 30, detectErr: true},
		{name: "one worker", workers: 1, memory: 1 << 30},
		{name: "quarter RAM", workers: 8, memory: 8 << 30, ram: 32 << 30},
		{name: "maximum workers", workers: 32, memory: 32 << 30, ram: 128 << 30},
		{name: "no workers", memory: 2 << 30, invalid: true},
		{name: "negative workers", workers: -1, memory: 2 << 30, invalid: true},
		{name: "too many workers", workers: 33, memory: 64 << 30, invalid: true},
		{name: "no memory", workers: 2, invalid: true},
		{name: "negative memory", workers: 2, memory: -1, invalid: true},
		{name: "insufficient per worker", workers: 4, memory: (4 << 30) - 1, invalid: true},
		{name: "over quarter RAM", workers: 8, memory: (8 << 30) + 1, ram: 32 << 30, invalid: true},
		{name: "unknown RAM", workers: 4, memory: 4 << 30, detectErr: true, invalid: true},
		{name: "zero RAM", workers: 4, memory: 4 << 30, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Whisper.StoreNativeCombineWorkers = tc.workers
			cfg.Whisper.StoreNativeCombineMemory = tc.memory
			err := cfg.validateNativeCombineBudget(func() (uint64, error) {
				if tc.detectErr {
					return 0, errors.New("unavailable")
				}
				return tc.ram, nil
			})
			if (err != nil) != tc.invalid {
				t.Fatalf("validation: %v", err)
			}
		})
	}
}

func TestNativeCombineConfigAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "carbon.conf")
	if err := os.WriteFile(path, []byte("[whisper]\nstorage-backend='pebble-chunk'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	defaults, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Whisper.StoreNativeImportConcurrency != 1 || defaults.Whisper.StoreNativeCombineWorkers != 2 || defaults.Whisper.StoreNativeCombineMemory != 2<<30 {
		t.Fatal("unset TOML changed serial defaults")
	}
	// Use exactly the default budget so this test also runs on small hosts.
	if err := os.WriteFile(path, []byte("[whisper]\nstorage-backend='pebble-chunk'\nstore-native-import-concurrency=2\nstore-native-combine-workers=1\nstore-native-combine-memory=2147483648\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Whisper.StoreNativeImportConcurrency != 2 || cfg.Whisper.StoreNativeCombineWorkers != 1 || cfg.Whisper.StoreNativeCombineMemory != 2<<30 || len(cfg.unknownKeys) != 0 {
		t.Fatal("TOML native budget lost")
	}
	old := NewConfig()
	old.Whisper.StorageBackend = "pebble-chunk"
	if old.Whisper.StoreNativeImportConcurrency != 1 || old.Whisper.StoreNativeCombineWorkers != 2 || old.Whisper.StoreNativeCombineMemory != 2<<30 {
		t.Fatal("defaults changed")
	}
	if !storageSettingsChanged(old, cfg) {
		t.Fatal("workers must require restart")
	}
	next := *old
	next.Whisper.StoreNativeCombineMemory++
	if !storageSettingsChanged(old, &next) {
		t.Fatal("memory must require restart")
	}
	if storageSettingsChanged(old, old) {
		t.Fatal("unchanged budget requires restart")
	}
	next = *old
	next.Whisper.StoreNativeImportConcurrency = 2
	if !storageSettingsChanged(old, &next) {
		t.Fatal("admission must require restart")
	}
	if err := next.validateNativeCombineBudget(func() (uint64, error) { return 128 << 30, nil }); err == nil {
		t.Fatal("admission multiplied the scratch allowance")
	}
	next.Whisper.StoreNativeCombineMemory = 4 << 30
	if err := next.validateNativeCombineBudget(func() (uint64, error) { return 128 << 30, nil }); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("[whisper]\nstore-native-combine-workers=0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadConfig(path); err == nil {
		t.Fatal("invalid budget accepted by ReadConfig")
	}
}
