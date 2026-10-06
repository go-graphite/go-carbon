package chunkstore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestPeriodicSyncKeepsTransferMutationsDurable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strict := vfs.NewStrictMem()
		fs := &walSyncFS{FS: strict}
		s, now := openTestStore(t, "/store", Options{fs: fs, SyncInterval: time.Hour})
		closed := false
		defer func() {
			if !closed {
				closeTestStore(t, s)
			}
		}()
		before := fs.syncs.Load()
		m := createTestMetric(t, s, "created")
		input := Snapshot{
			Metadata: m,
			Archives: []Archive{{Retention: m.Retentions[0], Points: []Point{{Timestamp: int64(now - 120), Value: 1}}}},
		}
		input.Metadata.Name = "imported"
		if _, err := s.CreateFromSnapshot(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		input.Archives[0].Points[0].Value = 2
		if _, err := s.Replace(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		input.Archives[0].Points = []Point{{Timestamp: int64(now - 60), Value: 3}}
		if _, err := s.Fill(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteIfUnchanged(context.Background(), "created", m); err != nil {
			t.Fatal(err)
		}
		if got := fs.syncs.Load(); got != before+5 {
			t.Fatalf("administrative WAL syncs = %d, want %d", got, before+5)
		}
		strict.SetIgnoreSyncs(true)
		closed = true
		closeTestStore(t, s)
		strict.ResetToSyncedState()
		strict.SetIgnoreSyncs(false)
		reopened, _ := openTestStore(t, "/store", Options{fs: strict})
		defer closeTestStore(t, reopened)
		if _, err := reopened.Metadata(context.Background(), "created"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted metric recovered: %v", err)
		}
		snapshot, err := reopened.Snapshot(context.Background(), "imported")
		if err != nil {
			t.Fatal(err)
		}
		points := snapshot.Archives[0].Points
		if len(points) != 2 || points[0].Value != 2 || points[1].Value != 3 {
			t.Fatalf("recovered import = %+v", snapshot)
		}
	})
}

func TestPeriodicSyncWithConcurrentUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strict := vfs.NewStrictMem()
		s, now := openTestStore(t, "/store", Options{fs: strict, SyncInterval: time.Millisecond})
		closed := false
		defer func() {
			if !closed {
				closeTestStore(t, s)
			}
		}()
		createTestMetric(t, s, "concurrent")
		const writers = 8
		const updates = 10
		var wg sync.WaitGroup
		for i := range writers {
			wg.Go(func() {
				for j := range updates {
					if err := s.UpdateMany(context.Background(), "concurrent", []Point{{Timestamp: int64(now - (i+1)*60), Value: float64(j)}}); err != nil {
						t.Error(err)
						return
					}
					time.Sleep(time.Millisecond)
				}
			})
		}
		wg.Wait()
		time.Sleep(time.Millisecond)
		synctest.Wait()
		strict.SetIgnoreSyncs(true)
		closed = true
		closeTestStore(t, s)
		strict.ResetToSyncedState()
		strict.SetIgnoreSyncs(false)
		reopened, _ := openTestStore(t, "/store", Options{fs: strict})
		defer closeTestStore(t, reopened)
		snapshot, err := reopened.Snapshot(context.Background(), "concurrent")
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Archives[0].Points) != writers || snapshot.Metadata.Revision != writers*updates+1 {
			t.Fatalf("recovered concurrent writes = %+v", snapshot)
		}
		for _, point := range snapshot.Archives[0].Points {
			if point.Value != updates-1 {
				t.Fatalf("recovered stale point = %+v", point)
			}
		}
	})
}

func TestSyncPolicyCrashRecovery(t *testing.T) {
	for _, tt := range []struct {
		name       string
		interval   time.Duration
		wait       bool
		crash      bool
		wantPoints int
	}{
		{name: "synchronous", crash: true, wantPoints: 2},
		{name: "unsynced updates lost", interval: time.Second, crash: true},
		{name: "periodic sync", interval: time.Second, wait: true, crash: true, wantPoints: 2},
		{name: "shutdown sync", interval: time.Hour, wantPoints: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				strict := vfs.NewStrictMem()
				fs := &walSyncFS{FS: strict}
				const dir = "/store"
				s, now := openTestStore(t, dir, Options{fs: fs, SyncInterval: tt.interval})
				closed := false
				defer func() {
					if !closed {
						closeTestStore(t, s)
					}
				}()
				createTestMetric(t, s, "sync")
				synctest.Wait()
				before := fs.syncs.Load()
				if err := s.UpdateMany(context.Background(), "sync", []Point{{Timestamp: int64(now - 120), Value: 1}}); err != nil {
					t.Fatal(err)
				}
				if err := s.UpdateManyForArchive(context.Background(), "sync", []Point{{Timestamp: int64(now - 60), Value: 2}}, 60*256); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				wantSyncs := before
				if tt.interval == 0 {
					wantSyncs += 2
				}
				if got := fs.syncs.Load(); got != wantSyncs {
					t.Fatalf("WAL syncs before interval = %d, want %d", got, wantSyncs)
				}
				if tt.wait {
					checkPeriodicSyncBoundary(t, s, fs, before, tt.interval, now)
				}

				// Discard all unsynced bytes, including any writes Pebble makes
				// while closing, to model loss of both process and OS buffers.
				strict.SetIgnoreSyncs(tt.crash)
				closed = true
				closeTestStore(t, s)
				strict.ResetToSyncedState()
				strict.SetIgnoreSyncs(false)
				reopened, _ := openTestStore(t, dir, Options{fs: strict})
				defer closeTestStore(t, reopened)
				snapshot, err := reopened.Snapshot(context.Background(), "sync")
				if err != nil {
					t.Fatal(err)
				}
				points := snapshot.Archives[0].Points
				if len(points) != tt.wantPoints || snapshot.Metadata.Revision != uint64(tt.wantPoints+1) {
					t.Fatalf("recovered snapshot = %+v, want %d points and revision %d", snapshot, tt.wantPoints, tt.wantPoints+1)
				}
				for i, p := range points {
					if p.Value != float64(i+1) {
						t.Fatalf("recovered point %d = %+v", i, p)
					}
				}
			})
		})
	}
}

func checkPeriodicSyncBoundary(t *testing.T, s *Store, fs *walSyncFS, before int64, interval time.Duration, now int) {
	t.Helper()
	time.Sleep(interval - time.Nanosecond)
	synctest.Wait()
	if got := fs.syncs.Load(); got != before {
		t.Fatalf("WAL synced before configured interval: %d", got)
	}
	time.Sleep(time.Nanosecond)
	synctest.Wait()
	if got := fs.syncs.Load(); got != before+1 {
		t.Fatalf("periodic WAL syncs = %d, want %d", got, before+1)
	}
	time.Sleep(2 * interval)
	synctest.Wait()
	if got := fs.syncs.Load(); got != before+1 {
		t.Fatalf("idle store synced its WAL: %d", got)
	}
	// A later, unsynced update must not survive the simulated crash.
	if err := s.UpdateMany(context.Background(), "sync", []Point{{Timestamp: int64(now), Value: 99}}); err != nil {
		t.Fatal(err)
	}
}

func TestPeriodicSyncFailureIsFailStop(t *testing.T) {
	const childEnv = "GO_CARBON_CHUNKSTORE_PERIODIC_SYNC_FAILURE_CHILD"
	if os.Getenv(childEnv) == "1" {
		synctest.Test(t, func(t *testing.T) {
			fs := &syncFailFS{FS: vfs.NewMem()}
			s, now := openTestStore(t, "/store", Options{fs: fs, SyncInterval: time.Second})
			createTestMetric(t, s, "sync-failure")
			if err := s.UpdateMany(context.Background(), "sync-failure", []Point{{Timestamp: int64(now - 60), Value: 1}}); err != nil {
				t.Fatal(err)
			}
			fs.fail.Store(true)
			time.Sleep(2 * time.Second)
			t.Fatal("periodic WAL Sync failure did not stop the process")
		})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPeriodicSyncFailureIsFailStop$")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "pebble: fatal commit error: injected sync failure") {
		t.Fatalf("child did not fail from WAL sync error: %v\n%s", err, output)
	}
}

func TestOpenRejectsNegativeSyncInterval(t *testing.T) {
	fs := vfs.NewMem()
	if _, err := Open("/store", Options{fs: fs, SyncInterval: -time.Second}); err == nil {
		t.Fatal("negative sync interval accepted")
	}
	if _, err := fs.Stat("/store"); !os.IsNotExist(err) {
		t.Fatalf("invalid options created store directory: %v", err)
	}
}

// Count only WAL syncs; manifest and directory syncs do not make point updates durable.
type walSyncFS struct {
	vfs.FS
	syncs atomic.Int64
}

func (fs *walSyncFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err == nil && strings.HasSuffix(name, ".log") {
		return walSyncFile{File: f, syncs: &fs.syncs}, nil
	}
	return f, err
}

type walSyncFile struct {
	vfs.File
	syncs *atomic.Int64
}

func (f walSyncFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	f.syncs.Add(1)
	return nil
}

func (f walSyncFile) SyncData() error {
	if err := f.File.SyncData(); err != nil {
		return err
	}
	f.syncs.Add(1)
	return nil
}
