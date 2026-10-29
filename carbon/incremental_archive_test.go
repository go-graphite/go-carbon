package carbon

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestIncrementalArchiveConfigurationAndReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	_, cfg := incrementalAppFixture(t)
	root := filepath.Dir(cfg.Whisper.StoreDir)
	cfg.Whisper.StoreMigrationArchiveDir = filepath.Join(root, "archive")
	if err := validateStorageConfig(cfg); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"relative", cfg.Whisper.DataDir, filepath.Join(cfg.Whisper.DataDir, "archive"), filepath.Join(cfg.Whisper.StoreDir, "archive"), root} {
		next := *cfg
		next.Whisper.StoreMigrationArchiveDir = invalid
		if err := validateStorageConfig(&next); err == nil {
			t.Fatal("accepted invalid original archive", invalid)
		}
	}
	next := *cfg
	next.Whisper.StoreMigrationArchiveDir = ""
	if !storageSettingsChanged(cfg, &next) {
		t.Fatal("archive policy could change through hot reload")
	}
	next = *cfg
	next.Whisper.StoreIncrementalMigration = false
	if err := validateStorageConfig(&next); err == nil {
		t.Fatal("accepted an archive without incremental migration")
	}
}

func TestIncrementalArchiveApplicationRestart(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreMigrationArchiveDir = filepath.Join(filepath.Dir(path), "original-archive")
	writeSharedConfig(t, path, cfg)
	originals := make(map[string][]byte)
	if err := filepath.Walk(cfg.Whisper.DataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		relative, err := filepath.Rel(cfg.Whisper.DataDir, path)
		if err != nil {
			return err
		}
		originals[relative], err = os.ReadFile(path)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	app := startIncrementalApp(t, path)
	incrementalLease(t, cfg, 2, 4)
	waitMigration(t, func() bool { return app.incrementalMigration.Load().receipt()["phase"] == "shared" })
	check := func() {
		t.Helper()
		for name, want := range originals {
			if _, err := os.Lstat(filepath.Join(cfg.Whisper.DataDir, name)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("active original remains for %s: %v", name, err)
			}
			got, err := os.ReadFile(filepath.Join(cfg.Whisper.StoreMigrationArchiveDir, name))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("archived original changed for %s: %v", name, err)
			}
		}
	}
	check()
	stamp := time.Now().Unix() - 10
	app.Cache.Add(points.OnePoint("first", 42, stamp))
	waitMigration(t, func() bool {
		series, err := app.MetricStore.Fetch(context.Background(), "first", int(stamp-1), int(stamp+2))
		if err != nil {
			return false
		}
		for _, value := range series.Values {
			if value == 42 {
				return true
			}
		}
		return false
	})
	app.Stop()
	app = startIncrementalApp(t, path)
	series, err := app.MetricStore.Fetch(context.Background(), "first", int(stamp-1), int(stamp+2))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, value := range series.Values {
		found = found || value == 42
	}
	if !found {
		t.Fatal("shared-only write did not survive application restart")
	}
	check()
}
