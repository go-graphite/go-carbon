package carbon

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMirrorGuardBlocksEverySharedReopen(t *testing.T) {
	dir := t.TempDir()
	if err := checkStoreConversion(dir); err != nil {
		t.Fatal(err)
	}
	if err := armMirrorGuard(dir); err != nil {
		t.Fatal(err)
	}
	if err := checkStoreConversion(dir); err == nil || !strings.Contains(err.Error(), "MIRROR-RECONCILE-REQUIRED") {
		t.Fatalf("guard did not block shared reopen: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "MIRROR-RECONCILE-REQUIRED"))
	if err != nil {
		t.Fatal(err)
	}
	if err := armMirrorGuard(dir); !errors.Is(err, os.ErrExist) {
		t.Fatalf("existing guard was overwritten: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "MIRROR-RECONCILE-REQUIRED"))
	if err != nil || string(after) != string(before) {
		t.Fatalf("guard changed: %v", err)
	}
}

func TestMirrorPathsRejectPhysicalOverlap(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "alias")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{source, source}, {source, filepath.Join(source, "new")}, {source, filepath.Join(link, "new")}, {filepath.Join(link, "new"), source}, {"relative", filepath.Join(root, "store")}} {
		if err := validateMirrorPaths(pair[0], pair[1]); err == nil {
			t.Fatalf("accepted overlapping or relative mirror paths %v", pair)
		}
	}
	if err := validateMirrorPaths(source, filepath.Join(root, "store")); err != nil {
		t.Fatal(err)
	}
}

func TestMirrorConfigRequiresWriteOnlyCanary(t *testing.T) {
	_, base := sharedAppConfig(t)
	base.Whisper.StoreMirrorFiles = true
	base.Whisper.StoreEncoding = "exact"
	base.Buckyd.Enabled = false
	if err := validateStorageConfig(base); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"file backend", func(c *Config) { c.Whisper.StorageBackend = "files" }},
		{"administration", func(c *Config) { c.Buckyd.Enabled = true }},
		{"expiration", func(c *Config) { c.Whisper.StoreExpiration = &Duration{time.Hour} }},
		{"expiration policy", func(c *Config) { c.Whisper.StoreExpirationFilename = "expiry.conf" }},
		{"legacy encoding", func(c *Config) { c.Whisper.StoreEncoding = "legacy" }},
		{"disabled writer", func(c *Config) { c.Whisper.Enabled = false }},
		{"implicit path", func(c *Config) { c.Whisper.StoreDir = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := *base
			tc.change(&next)
			if err := validateStorageConfig(&next); err == nil {
				t.Fatal("unsafe mirror configuration accepted")
			}
		})
	}
	next := *base
	next.Whisper.StoreMirrorFiles = false
	if !storageSettingsChanged(base, &next) {
		t.Fatal("mirror could be disabled by hot reload")
	}
}
