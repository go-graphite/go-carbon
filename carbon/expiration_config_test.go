package carbon

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreExpirationConfigDefaultsAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	if err := os.WriteFile(path, []byte(`[whisper]
storage-backend = "pebble-chunk"
store-expiration = "24h"
store-expiration-file = "/etc/go-carbon/storage-expiration.conf"
store-expiration-check-interval = "15m"
store-expiration-scan-rate = 250
store-expiration-dry-run = true
store-expiration-delete-rate = 50
store-expiration-delete-workers = 3
store-expiration-max-deletes = 500
store-expiration-max-l0-sublevels = 4
store-expiration-max-compaction-debt = 10000000
`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Whisper.StoreExpiration.Value(); got != 24*time.Hour {
		t.Fatalf("expiration = %v, want 24h", got)
	}
	if got := cfg.Whisper.StoreExpirationFilename; got != "/etc/go-carbon/storage-expiration.conf" {
		t.Fatalf("expiration file = %q", got)
	}
	if got := cfg.Whisper.StoreExpirationCheckInterval.Value(); got != 15*time.Minute {
		t.Fatalf("check interval = %v, want 15m", got)
	}
	if !cfg.Whisper.StoreExpirationDryRun || cfg.Whisper.StoreExpirationDeleteWorkers != 3 || cfg.Whisper.StoreExpirationDeleteRate != 50 || cfg.Whisper.StoreExpirationMaxDeletes != 500 || cfg.Whisper.StoreExpirationMaxL0 != 4 || cfg.Whisper.StoreExpirationMaxDebt != 10_000_000 {
		t.Fatal("expiration safety overrides were not loaded")
	}
	if got := cfg.Whisper.StoreExpirationScanRate; got != 250 {
		t.Fatalf("scan rate = %d, want 250", got)
	}
}

func TestStoreExpirationConfigDefaults(t *testing.T) {
	cfg := NewConfig()
	if got := cfg.Whisper.StoreExpiration.Value(); got != 0 {
		t.Fatalf("expiration = %v, want 0", got)
	}
	if got := cfg.Whisper.StoreExpirationFilename; got != "" {
		t.Fatalf("expiration file = %q, want empty", got)
	}
	if got := cfg.Whisper.StoreExpirationCheckInterval.Value(); got != time.Hour {
		t.Fatalf("check interval = %v, want 1h", got)
	}
	if got := cfg.Whisper.StoreExpirationScanRate; got != 100_000 {
		t.Fatalf("scan rate = %d, want 100000", got)
	}
	if cfg.Whisper.StoreExpirationDryRun || cfg.Whisper.StoreExpirationDeleteWorkers != 8 || cfg.Whisper.StoreExpirationDeleteRate != 100 || cfg.Whisper.StoreExpirationMaxDeletes != 10_000 || cfg.Whisper.StoreExpirationMaxL0 != 6 || cfg.Whisper.StoreExpirationMaxDebt != 1<<30 {
		t.Fatal("unexpected expiration safety defaults")
	}
}

func TestStoreExpirationSafetySettingsDoNotEnableExpiration(t *testing.T) {
	now := time.Now()
	app := &App{Config: NewConfig(), MetricStore: expirationTestDB(t, &now)}
	// Even selecting reporting mode must not create an expiration policy.
	app.Config.Whisper.StoreExpirationDryRun = true
	app.startExpiration()
	if app.expirer != nil {
		app.stopExpiration()
		t.Fatal("safety settings enabled expiration without a TTL or policy")
	}
}

func TestStoreExpirationConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"negative expiration", func(c *Config) { c.Whisper.StoreExpiration = &Duration{-time.Second} }},
		{"zero check interval", func(c *Config) { c.Whisper.StoreExpirationCheckInterval = &Duration{} }},
		{"negative check interval", func(c *Config) { c.Whisper.StoreExpirationCheckInterval = &Duration{-time.Second} }},
		{"zero delete workers", func(c *Config) { c.Whisper.StoreExpirationDeleteWorkers = 0 }},
		{"negative delete workers", func(c *Config) { c.Whisper.StoreExpirationDeleteWorkers = -1 }},
		{"zero delete rate", func(c *Config) { c.Whisper.StoreExpirationDeleteRate = 0 }},
		{"negative delete limit", func(c *Config) { c.Whisper.StoreExpirationMaxDeletes = -1 }},
		{"zero L0 limit", func(c *Config) { c.Whisper.StoreExpirationMaxL0 = 0 }},
		{"negative debt limit", func(c *Config) { c.Whisper.StoreExpirationMaxDebt = -1 }},
		{"zero scan rate", func(c *Config) { c.Whisper.StoreExpirationScanRate = 0 }},
		{"negative scan rate", func(c *Config) { c.Whisper.StoreExpirationScanRate = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.Whisper.StorageBackend = "pebble-chunk"
			tt.change(cfg)
			if err := validateStorageConfig(cfg); err == nil {
				t.Fatal("invalid expiration configuration accepted")
			}
		})
	}
}
