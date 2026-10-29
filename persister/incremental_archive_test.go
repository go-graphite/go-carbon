package persister

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestIncrementalArchiveRetainsOriginalBytesAndSharedWrites(t *testing.T) {
	p, cache, db, destination := incrementalPersisterFixture(t)
	defer db.Close()
	archive := filepath.Join(filepath.Dir(destination), "archive")
	if err := p.SetIncrementalMigrationArchive(archive); err != nil {
		t.Fatal(err)
	}
	before := archiveSourceImages(t, p.rootPath)
	result, err := p.ConvertFile(context.Background(), "metric")
	if err != nil || result.UnlinkedBytes != 0 || result.ArchivedBytes <= 0 {
		t.Fatalf("archive result %+v: %v", result, err)
	}
	checkArchivedImages(t, before, p.rootPath, archive)
	stamp := time.Now().Unix() - 15
	cache.add("metric", stamp, -0.125)
	p.store("metric")
	snapshot, err := db.Snapshot(context.Background(), "metric")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, archive := range snapshot.Archives {
		for _, point := range archive.Points {
			found = found || point.Timestamp == stamp && point.Value == -0.125
		}
	}
	if !found {
		t.Fatal("shared write was not visible after archiving")
	}
	checkArchivedImages(t, before, p.rootPath, archive)
	if err := p.SetIncrementalMigrationArchive(""); err == nil {
		t.Fatal("silently disabled an existing archive policy")
	}
	if err := p.SetIncrementalMigrationArchive(filepath.Join(filepath.Dir(destination), "other-archive")); err == nil {
		t.Fatal("silently changed the bound archive directory")
	}
}

func TestIncrementalArchiveCrashBoundariesResume(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"owned", "archived-2", "unlink-2", "directory-synced"} {
		t.Run(boundary, func(t *testing.T) {
			p, _, db, destination := incrementalPersisterFixture(t)
			defer func() { _ = db.Close() }()
			archive := filepath.Join(filepath.Dir(destination), "new", "archive")
			if err := p.SetIncrementalMigrationArchive(archive); err != nil {
				t.Fatal(err)
			}
			before := archiveSourceImages(t, p.rootPath)
			interrupted := errors.New("interrupted archive")
			_, err := p.convertFile(context.Background(), "metric", func(stage string) error {
				if stage == boundary {
					return interrupted
				}
				return nil
			})
			if !errors.Is(err, interrupted) {
				t.Fatalf("did not reach %s: %v", boundary, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath, SyncInterval: time.Hour}))
			if err != nil {
				t.Fatal(err)
			}
			p.SetMetricStore(db)
			if err := p.SetIncrementalMigration(db, destination); err != nil {
				t.Fatal(err)
			}
			if err := p.SetIncrementalMigrationArchive(archive); err != nil {
				t.Fatal(err)
			}
			if _, err := p.ConvertFile(context.Background(), "metric"); err != nil {
				t.Fatal(err)
			}
			checkArchivedImages(t, before, p.rootPath, archive)
			pending, err := db.PendingFileCleanup(context.Background(), 10)
			if err != nil || len(pending) != 0 {
				t.Fatalf("archive cleanup did not settle: %+v %v", pending, err)
			}
		})
	}
}

func TestIncrementalArchiveRejectsForeignTargetAndChangedRoot(t *testing.T) {
	p, _, db, destination := incrementalPersisterFixture(t)
	defer db.Close()
	archive := filepath.Join(filepath.Dir(destination), "archive")
	if err := p.SetIncrementalMigrationArchive(archive); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(p.rootPath, "metric.wsp")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(archive, "metric.wsp")
	if err := os.WriteFile(target, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConvertFile(context.Background(), "metric"); err == nil {
		t.Fatal("replaced a foreign archive target")
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, before) {
		t.Fatal("removed or changed the unarchived source")
	}
	if got, _ := os.ReadFile(target); string(got) != "foreign" {
		t.Fatal("overwrote the foreign target")
	}
	if err := os.Rename(archive, archive+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(archive, 0750); err != nil {
		t.Fatal(err)
	}
	if err := p.SetIncrementalMigrationArchive(archive); err == nil {
		t.Fatal("accepted a replaced archive directory")
	}
}

func TestIncrementalArchiveNestedNamespaceRejectsSymlink(t *testing.T) {
	p, _, db, destination := incrementalPersisterFixture(t)
	defer db.Close()
	// Move an existing original into a nested namespace before conversion.
	// New metrics would correctly start in shared storage without originals.
	if err := os.MkdirAll(filepath.Join(p.rootPath, "nested"), 0750); err != nil {
		t.Fatal(err)
	}
	for _, path := range originalPaths(filepath.Join(p.rootPath, "metric.wsp")) {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := os.Rename(path, filepath.Join(p.rootPath, "nested", filepath.Base(path))); err != nil {
			t.Fatal(err)
		}
	}
	archive := filepath.Join(filepath.Dir(destination), "archive")
	if err := p.SetIncrementalMigrationArchive(archive); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(p.rootPath, "nested"), filepath.Join(archive, "nested")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConvertFile(context.Background(), "nested.metric"); err == nil {
		t.Fatal("accepted an archive namespace pointing back into the source")
	}
	if _, err := os.Stat(filepath.Join(p.rootPath, "nested", "metric.wsp")); err != nil {
		t.Fatal("source removed through archive alias", err)
	}
	if err := os.Remove(filepath.Join(archive, "nested")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConvertFile(context.Background(), "nested.metric"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(archive, "nested", "metric.wsp")); err != nil {
		t.Fatal("nested source was not archived", err)
	}
}

type archiveSourceImage struct {
	data []byte
	info os.FileInfo
}

func archiveSourceImages(t *testing.T, root string) map[string]archiveSourceImage {
	t.Helper()
	images := make(map[string]archiveSourceImage)
	for _, path := range originalPaths(filepath.Join(root, "metric.wsp")) {
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		images[filepath.Base(path)] = archiveSourceImage{data: data, info: info}
	}
	if len(images) == 0 {
		t.Fatal("empty original archive fixture")
	}
	return images
}

func checkArchivedImages(t *testing.T, images map[string]archiveSourceImage, source, archive string) {
	t.Helper()
	for name, want := range images {
		if _, err := os.Lstat(filepath.Join(source, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("active original remains: %s, %v", name, err)
		}
		path := filepath.Join(archive, name)
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want.data) {
			t.Fatalf("archived bytes differ for %s: %v", name, err)
		}
		info, err := os.Stat(path)
		if err != nil || !os.SameFile(info, want.info) || !info.ModTime().Equal(want.info.ModTime()) {
			t.Fatalf("archive changed original identity/activity for %s: %v", name, err)
		}
	}
}
