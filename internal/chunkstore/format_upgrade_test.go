package chunkstore

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestInterruptedFormatUpgradeCanRetryAndRecover(t *testing.T) {
	for _, stage := range []string{"file sync", "rename", "directory sync"} {
		t.Run(stage, func(t *testing.T) {
			testInterruptedFormatUpgradeCanRetryAndRecover(t, stage)
		})
	}
}

func testInterruptedFormatUpgradeCanRetryAndRecover(t *testing.T, stage string) {
	t.Helper()
	strict := vfs.NewStrictMem()
	const dir = "/store"
	now := time.Unix(100_000, 0)
	m := prepareLegacyStoreForUpgrade(t, strict, dir, now)
	fs := &upgradeFailureFS{FS: strict, stage: stage}
	failFormatUpgrade(t, dir, fs)

	legacy := retryFormatUpgrade(t, dir, fs, m.Name, now)
	if err := legacy.store.InitializeActivity(context.Background(), m.Name, legacy.metadata); err != nil {
		t.Fatal(err)
	}
	closeForRecovery(t, legacy.store, strict, true)
	assertFormatMarker(t, strict, dir, formatMarkerV2)

	now = now.Add(time.Hour)
	assertRecoveredUpgradeGrace(t, strict, dir, m, now)
}

type retriedLegacyMetric struct {
	store    *Store
	metadata Metadata
}

func prepareLegacyStoreForUpgrade(t *testing.T, fs vfs.FS, dir string, now time.Time) Metadata {
	t.Helper()
	s, err := Open(dir, Options{fs: fs, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	m := createTestMetric(t, s, "legacy")
	if err := s.db.Set(revisionKey(m), uint64Bytes(m.Revision), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	writeTestFormatMarker(t, fs, dir, formatMarkerV1)
	return m
}

func failFormatUpgrade(t *testing.T, dir string, fs *upgradeFailureFS) {
	t.Helper()
	fs.fail.Store(true)
	if opened, err := Open(dir, Options{fs: fs}); err == nil {
		closeTestStore(t, opened)
		t.Fatal("injected upgrade failure accepted")
	} else if !strings.Contains(err.Error(), "injected") {
		t.Fatalf("unexpected upgrade error: %v", err)
	}
	fs.fail.Store(false)
}

func retryFormatUpgrade(t *testing.T, dir string, fs *upgradeFailureFS, name string, now time.Time) retriedLegacyMetric {
	t.Helper()
	// Retry without resetting memory: directory-sync failure leaves the
	// renamed v2 marker visible but not yet durable.
	retried, err := Open(dir, Options{fs: fs, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := retried.Metadata(context.Background(), name)
	if err != nil || !legacy.LastUpdate.IsZero() {
		t.Fatalf("legacy metric changed during marker upgrade: %+v, %v", legacy, err)
	}
	return retriedLegacyMetric{store: retried, metadata: legacy}
}

func assertFormatMarker(t *testing.T, fs vfs.FS, dir, want string) {
	t.Helper()
	f, err := fs.Open(dir + "/CHUNKSTORE")
	if err != nil {
		t.Fatal(err)
	}
	marker, readErr := io.ReadAll(f)
	if err := errors.Join(readErr, f.Close()); err != nil || string(marker) != want {
		t.Fatalf("marker after crash=%q, error=%v", marker, err)
	}
}

func assertRecoveredUpgradeGrace(t *testing.T, fs vfs.FS, dir string, m Metadata, now time.Time) {
	t.Helper()
	recovered, err := Open(dir, Options{fs: fs, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, recovered)
	got, err := recovered.Metadata(context.Background(), m.Name)
	if err != nil || !got.LastUpdate.Equal(now.Add(-time.Hour)) {
		t.Fatalf("bootstrap grace was lost across restart: %+v, %v", got, err)
	}
	if err := recovered.InitializeActivity(context.Background(), m.Name, got); !errors.Is(err, ErrConflict) {
		t.Fatalf("restart reset grace period: %v", err)
	}
}

func writeTestFormatMarker(t *testing.T, fs vfs.FS, dir, marker string) {
	t.Helper()
	f, err := fs.Create(dir + "/CHUNKSTORE")
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write([]byte(marker))
	if err := errors.Join(writeErr, f.Sync(), f.Close(), syncStoreDirectory(fs, dir)); err != nil {
		t.Fatal(err)
	}
}

type upgradeFailureFS struct {
	vfs.FS
	stage   string
	fail    atomic.Bool
	renamed atomic.Bool
}

func (fs *upgradeFailureFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err == nil && strings.HasSuffix(name, "/CHUNKSTORE.upgrade") && fs.stage == "file sync" {
		return syncFailFile{File: f, fail: &fs.fail}, nil
	}
	return f, err
}

func (fs *upgradeFailureFS) Rename(oldname, newname string) error {
	if strings.HasSuffix(newname, "/CHUNKSTORE") {
		if fs.fail.Load() && fs.stage == "rename" {
			return errors.New("injected upgrade rename failure")
		}
		if err := fs.FS.Rename(oldname, newname); err != nil {
			return err
		}
		fs.renamed.Store(true)
		return nil
	}
	return fs.FS.Rename(oldname, newname)
}

func (fs *upgradeFailureFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	if err == nil && fs.stage == "directory sync" {
		return upgradeDirectory{File: f, fs: fs}, nil
	}
	return f, err
}

type upgradeDirectory struct {
	vfs.File
	fs *upgradeFailureFS
}

func (d upgradeDirectory) Sync() error {
	if d.fs.fail.Load() && d.fs.renamed.Load() {
		return errors.New("injected upgrade directory sync failure")
	}
	return d.File.Sync()
}
