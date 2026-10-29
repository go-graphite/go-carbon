package chunkstore

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func namedMigrationSnapshot(name string) Snapshot {
	snapshot := fileMigrationSnapshot()
	snapshot.Metadata.Name = name
	return snapshot
}

// assertHiddenFile is assertUnownedFile for a store holding other, owned metrics.
func assertHiddenFile(t *testing.T, s *Store, name string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Metadata(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned metadata: %v", err)
	}
	if _, err := s.Snapshot(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned snapshot: %v", err)
	}
}

func TestReclaimOrphanedFilesMarksOnlyAbsentOriginals(t *testing.T) {
	defer func(page int) { orphanReclaimPage = page }(orphanReclaimPage)
	orphanReclaimPage = 2 // cross page boundaries
	strict := vfs.NewStrictMem()
	s := openFileMigrationStore(t, strict)
	ctx := context.Background()
	stage := func(name string) (Snapshot, Metadata) {
		t.Helper()
		want := namedMigrationSnapshot(name)
		staged, err := s.StageFileSnapshot(ctx, want)
		if err != nil {
			t.Fatal(err)
		}
		return want, staged
	}
	orphan, orphanStaged := stage("a.orphan")
	stage("b.present")
	pending, pendingStaged := stage("c.pending")
	if _, err := s.PublishFileOwnership(ctx, pending, pendingStaged); err != nil {
		t.Fatal(err)
	}
	done, doneStaged := stage("d.done")
	owned, err := s.PublishFileOwnership(ctx, done, doneStaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteFileCleanup(ctx, owned); err != nil {
		t.Fatal(err)
	}
	second, _ := stage("e.orphan")
	exists := func(name string) (bool, error) { return name == "b.present", nil }

	dry, err := s.ReclaimOrphanedFiles(ctx, exists, false)
	if err != nil {
		t.Fatal(err)
	}
	want := OrphanReclaim{Scanned: 5, Unowned: 3, SourcePresent: 1, Reclaimed: 2, CleanupPending: 1, Samples: []string{"a.orphan", "e.orphan"}}
	if !reflect.DeepEqual(dry, want) {
		t.Fatalf("dry run %+v, want %+v", dry, want)
	}
	assertHiddenFile(t, s, "a.orphan")

	applied, err := s.ReclaimOrphanedFiles(ctx, exists, true)
	if err != nil || !reflect.DeepEqual(applied, want) {
		t.Fatalf("apply %+v %v, want %+v", applied, err, want)
	}
	if err := s.db.LogData(nil, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	closeForRecovery(t, s, strict, true)
	s = openFileMigrationStore(t, strict)
	defer closeTestStore(t, s)

	for _, snapshot := range []Snapshot{orphan, second} {
		name := snapshot.Metadata.Name
		m, err := s.Metadata(ctx, name)
		if err != nil || m.FileMigration != FileReclaimed {
			t.Fatalf("%s not reclaimed: %+v %v", name, m, err)
		}
		got, err := s.Snapshot(ctx, name)
		if err != nil || EqualSnapshots(snapshot, got) != nil {
			t.Fatalf("%s history changed: %v %v", name, err, EqualSnapshots(snapshot, got))
		}
	}
	if m, _ := s.Metadata(ctx, "a.orphan"); m.ID != orphanStaged.ID || m.Generation != orphanStaged.Generation || m.Revision != orphanStaged.Revision+1 {
		t.Fatalf("reclaim must keep identity and bump the revision: %+v staged %+v", m, orphanStaged)
	}
	if _, err := s.Metadata(ctx, "b.present"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("metric with a present original must stay unowned: %v", err)
	}
	if m, err := s.Metadata(ctx, "c.pending"); err != nil || m.FileMigration != FileCleanupPending {
		t.Fatalf("pending cleanup changed: %+v %v", m, err)
	}
	if err := s.UpdateMany(ctx, "a.orphan", []Point{{Timestamp: 1790000003, Value: 99}}); err != nil {
		t.Fatalf("reclaimed metric must accept writes: %v", err)
	}
	if err := s.DiscardStagedFile(ctx, "a.orphan"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a reclaimed metric must not be discardable as a stale import: %v", err)
	}

	again, err := s.ReclaimOrphanedFiles(ctx, exists, true)
	if err != nil || again.Reclaimed != 0 || again.Unowned != 1 || again.SourcePresent != 1 {
		t.Fatalf("second run must be a no-op: %+v %v", again, err)
	}
}

func TestReclaimOrphanedFilesRequiresIncrementalStore(t *testing.T) {
	s, err := Open("/store", Options{fs: vfs.NewMem(), ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	if _, err := s.ReclaimOrphanedFiles(context.Background(), func(string) (bool, error) { return false, nil }, true); err == nil {
		t.Fatal("reclaim without an incremental source must fail")
	}
}

func TestReclaimOrphanedFilesStopsOnExistsError(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	ctx := context.Background()
	if _, err := s.StageFileSnapshot(ctx, namedMigrationSnapshot("a.orphan")); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("stat failed")
	if _, err := s.ReclaimOrphanedFiles(ctx, func(string) (bool, error) { return false, boom }, true); !errors.Is(err, boom) {
		t.Fatalf("exists error not returned: %v", err)
	}
	assertUnownedFile(t, s, "a.orphan")
}
