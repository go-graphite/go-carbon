package carbon

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestHandoffReadAheadConfigRejectsInvalidReloadValue(t *testing.T) {
	path := testConfig(t, t.TempDir())
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tcp.HandoffReadAheadBytes = 256 << 20
	cfg.Buckyd.WaitForJWTSecret = true
	cfg.Buckyd.JWTSecretFile = filepath.Join(t.TempDir(), "delivered-after-boot")
	writeSharedConfig(t, path, cfg)
	loaded, err := loadConfig(path)
	if err != nil || loaded.Tcp.HandoffReadAheadBytes != 256<<20 || !loaded.Buckyd.WaitForJWTSecret {
		t.Fatalf("configured successor settings: config=%v error=%v", loaded, err)
	}
	cfg.Tcp.HandoffReadAheadBytes = -1
	writeSharedConfig(t, path, cfg)
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "handoff-read-ahead-bytes") {
		t.Fatalf("unsafe configuration accepted for reload: %v", err)
	}
}
