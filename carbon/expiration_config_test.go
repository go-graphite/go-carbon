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
	if got := cfg.Whisper.StoreExpirationScanRate; got != 1000 {
		t.Fatalf("scan rate = %d, want 1000", got)
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
