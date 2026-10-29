package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func fileMigrationSnapshot() Snapshot {
	retentions := []Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 2880}}
	return Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "migration.bits", Retentions: retentions, AggregationMethod: Average, XFilesFactor: .5}}, Archives: []Archive{
		{Retention: retentions[0], Points: []Point{{Timestamp: 1790000000, Value: math.Copysign(0, -1)}, {Timestamp: 1790000001, Value: .1}, {Timestamp: 1790000002, Value: math.Float64frombits(0x7ff8000000000042)}}},
		{Retention: retentions[1], Points: []Point{{Timestamp: 1789999980, Value: 1e100}}},
	}}
}

func openFileMigrationStore(t *testing.T, fs vfs.FS) *Store {
	t.Helper()
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	if err := syncStoreDirectory(fs, "/"); err != nil {
		t.Fatal(err)
	}
	s, err := Open("/store", Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy", SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func assertUnownedFile(t *testing.T, s *Store, name string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Metadata(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned metadata: %v", err)
	}
	if _, err := s.Fetch(ctx, name, 1789999900, 1790000100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned fetch: %v", err)
	}
	if _, err := s.Snapshot(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned snapshot: %v", err)
	}
	if err := s.UpdateManyWithArchiveValues(ctx, name, nil, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned corrected update: %v", err)
	}
	if err := s.UpdateMany(ctx, name, []Point{{Timestamp: 1790000003, Value: 99}}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned update: %v", err)
	}
	page, err := s.ListPage(ctx, "", "", 1)
	if err != nil || len(page) != 0 {
		t.Fatalf("unowned catalog: %+v %v", page, err)
	}
}

func TestFileMigrationVerifiesBeforePublishingOwnership(t *testing.T) {
	ctx := context.Background()
	fs := &walSyncFS{FS: vfs.NewMem()}
	s := openFileMigrationStore(t, fs)
	defer closeTestStore(t, s)
	want := fileMigrationSnapshot()
	before := fs.syncs.Load()
	staged, err := s.StageFileSnapshot(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	if fs.syncs.Load() != before {
		t.Fatal("staging introduced a synchronous WAL commit")
	}
	assertUnownedFile(t, s, want.Metadata.Name)
	wrong := want
	wrong.Metadata.XFilesFactor = .25
	if _, err := s.PublishFileOwnership(ctx, wrong, staged); err == nil {
		t.Fatal("published a snapshot that did not match the stored data")
	}
	assertUnownedFile(t, s, want.Metadata.Name)
	owned, err := s.PublishFileOwnership(ctx, want, staged)
	if err != nil {
		t.Fatal(err)
	}
	if fs.syncs.Load() != before+1 || owned.FileMigration != FileCleanupPending {
		t.Fatalf("ownership syncs=%d metadata=%+v", fs.syncs.Load()-before, owned)
	}
	got, err := s.Snapshot(ctx, want.Metadata.Name)
	if err != nil || EqualSnapshots(want, got) != nil {
		t.Fatalf("owned archive bits changed: %v", err)
	}
	if _, err := s.StageFileSnapshot(ctx, wrong); !errors.Is(err, ErrExists) {
		t.Fatalf("re-imported an owned metric: %v", err)
	}
	if err := s.Delete(ctx, owned.Name); !errors.Is(err, ErrConflict) {
		t.Fatalf("deleted before original cleanup: %v", err)
	}
	if err := s.UpdateMany(ctx, owned.Name, []Point{{Timestamp: time.Now().Unix() - 1, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	if fs.syncs.Load() != before+1 {
		t.Fatal("ordinary ingestion unexpectedly synced the WAL")
	}
	pending, err := s.PendingFileCleanup(ctx, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != owned.ID {
		t.Fatalf("cleanup queue: %+v %v", pending, err)
	}
	var visited []string
	if err := s.VisitPendingFileCleanup(ctx, func(name string) error { visited = append(visited, name); return nil }); err != nil || len(visited) != 1 || visited[0] != owned.Name {
		t.Fatalf("visited cleanup queue: %v %v", visited, err)
	}
	if err := s.CompleteFileCleanup(ctx, owned); err != nil {
		t.Fatal(err)
	}
	visited = nil
	if err := s.VisitPendingFileCleanup(ctx, func(name string) error { visited = append(visited, name); return nil }); err != nil || len(visited) != 0 {
		t.Fatalf("completed cleanup still visited: %v %v", visited, err)
	}
	if err := s.CompleteFileCleanup(ctx, owned); err != nil {
		t.Fatalf("cleanup is not idempotent: %v", err)
	}
	pending, err = s.PendingFileCleanup(ctx, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("completed cleanup remains queued: %+v %v", pending, err)
	}
	m, err := s.Metadata(ctx, owned.Name)
	if err != nil || m.FileMigration != FileReclaimed {
		t.Fatalf("reclaimed metadata: %+v %v", m, err)
	}
	if err := s.DeleteIfUnchanged(ctx, m.Name, m); err != nil {
		t.Fatalf("reclaimed metric cannot expire: %v", err)
	}
}

func TestFileMigrationCrashOwnershipAndCleanup(t *testing.T) {
	for _, phase := range []string{"staged", "owned", "uncommitted-cleanup", "durable-cleanup"} {
		t.Run(phase, func(t *testing.T) {
			strict := vfs.NewStrictMem()
			s := openFileMigrationStore(t, strict)
			ctx := context.Background()
			want := fileMigrationSnapshot()
			staged, err := s.StageFileSnapshot(ctx, want)
			if err != nil {
				t.Fatal(err)
			}
			if phase != "staged" {
				owned, err := s.PublishFileOwnership(ctx, want, staged)
				if err != nil {
					t.Fatal(err)
				}
				if phase == "uncommitted-cleanup" || phase == "durable-cleanup" {
					if err := s.CompleteFileCleanup(ctx, owned); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "durable-cleanup" {
					if err := s.db.LogData(nil, pebble.Sync); err != nil {
						t.Fatal(err)
					}
				}
			}
			closeForRecovery(t, s, strict, true)
			reopened := openFileMigrationStore(t, strict)
			defer closeTestStore(t, reopened)
			if phase == "staged" {
				assertUnownedFile(t, reopened, want.Metadata.Name)
				return
			}
			got, err := reopened.Snapshot(ctx, want.Metadata.Name)
			if err != nil || EqualSnapshots(want, got) != nil {
				t.Fatalf("crash lost published history: %v", err)
			}
			m, err := reopened.Metadata(ctx, want.Metadata.Name)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := reopened.PendingFileCleanup(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "durable-cleanup" {
				if m.FileMigration != FileReclaimed || len(pending) != 0 {
					t.Fatalf("completed cleanup not durable: %+v %+v", m, pending)
				}
			} else if m.FileMigration != FileCleanupPending || len(pending) != 1 {
				t.Fatalf("pending cleanup did not resume: %+v %+v", m, pending)
			}
		})
	}
}

func TestFileMigrationRejectsWrongModeAndExistingBaseline(t *testing.T) {
	fs := vfs.NewMem()
	s := openFileMigrationStore(t, fs)
	closeTestStore(t, s)
	for _, source := range []string{"", "/another-original"} {
		if other, err := Open("/store", Options{fs: fs, ExactBlocks: true, IncrementalSource: source}); err == nil {
			closeTestStore(t, other)
			t.Fatalf("accepted source %q", source)
		}
	}
	fs = vfs.NewMem()
	s, _ = openTestStore(t, "/existing", Options{fs: fs, ExactBlocks: true})
	createTestMetric(t, s, "baseline")
	closeTestStore(t, s)
	if err := fs.MkdirAll("/legacy", 0750); err != nil {
		t.Fatal(err)
	}
	if other, err := Open("/existing", Options{fs: fs, ExactBlocks: true, IncrementalSource: "/legacy"}); err == nil {
		closeTestStore(t, other)
		t.Fatal("adopted an unverified baseline")
	}
}

func TestFileMigrationCatalogStatesRequireIncrementalFormat(t *testing.T) {
	for _, state := range []FileMigrationState{FileCleanupPending, FileReclaimed} {
		m := catalogFixture()
		m.FileMigration = state
		if _, err := (&Store{exactBlocks: true}).encodeCatalog(m); err == nil {
			t.Fatal("encoded ownership in a nonincremental store")
		}
		encoded, err := (&Store{exactBlocks: true, incrementalSource: "/original"}).encodeCatalog(m)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeCatalog(m.Name, encoded)
		if err != nil || !reflect.DeepEqual(got, m) {
			t.Fatalf("ownership catalog roundtrip: %+v %v", got, err)
		}
	}
}

func TestFileMigrationRejectsReplacedOriginalDirectory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	original, destination := filepath.Join(root, "original"), filepath.Join(root, "exact")
	if err := os.Mkdir(original, 0750); err != nil {
		t.Fatal(err)
	}
	s, err := Open(destination, Options{ExactBlocks: true, IncrementalSource: original})
	if err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	if err := os.Rename(original, original+".previous"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(original, 0750); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(destination, Options{ExactBlocks: true, IncrementalSource: original}); !errors.Is(err, ErrFormat) {
		if reopened != nil {
			closeTestStore(t, reopened)
		}
		t.Fatalf("accepted a different source tree at the same pathname: %v", err)
	}
}

func TestFileMigrationConcurrentReservationsAndNewMetricsNeverReuseIDs(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	const count = 64
	var group sync.WaitGroup
	ids := make(chan uint64, count)
	errors := make(chan error, count)
	for i := range count {
		group.Go(func() {
			snapshot := fileMigrationSnapshot()
			snapshot.Metadata.Name = fmt.Sprintf("parallel.%d", i)
			var m Metadata
			var err error
			if i%2 == 0 {
				m, err = s.StageFileSnapshot(context.Background(), snapshot)
				if err == nil {
					m, err = s.PublishFileOwnership(context.Background(), snapshot, m)
				}
			} else {
				m, err = s.Create(context.Background(), snapshot.Metadata.MetricConfig)
			}
			if err != nil {
				errors <- err
			} else {
				ids <- m.ID
			}
		})
	}
	group.Wait()
	close(errors)
	close(ids)
	for err := range errors {
		t.Error(err)
	}
	seen := make(map[uint64]bool)
	for id := range ids {
		if id == 0 || seen[id] {
			t.Fatalf("duplicate/invalid metric ID: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("only %d/%d IDs reserved", len(seen), count)
	}
}

func TestFileMigrationReopenReclaimsAbandonedScratchOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	original, destination := filepath.Join(root, "original"), filepath.Join(root, "exact")
	if err := os.Mkdir(original, 0750); err != nil {
		t.Fatal(err)
	}
	s, err := Open(destination, Options{ExactBlocks: true, IncrementalSource: original})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".incremental-capture", ".incremental-index"} {
		path := filepath.Join(destination, name, "abandoned")
		if err := os.MkdirAll(path, 0750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "temporary"), []byte("discardable"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	keep := filepath.Join(destination, "operator-receipt.json")
	if err := os.WriteFile(keep, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	closeTestStore(t, s)
	s, err = Open(destination, Options{ExactBlocks: true, IncrementalSource: original})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, s)
	for _, name := range []string{".incremental-capture", ".incremental-index"} {
		if _, err := os.Stat(filepath.Join(destination, name)); !os.IsNotExist(err) {
			t.Fatalf("abandoned scratch retained: %s %v", name, err)
		}
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("removed an unrelated file: ", err)
	}
}
