package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestExactStoreWALRotationDoesNotBlockUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := &rotationFS{FS: vfs.NewMem()}
		s, now := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, SyncInterval: time.Hour})
		defer closeTestStore(t, s)
		createTestMetric(t, s, "rotation")
		if err := s.db.LogData(nil, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		gate := fs.arm("sync")
		defer gate.unblock()
		if _, err := s.AsyncFlush(); err != nil {
			t.Fatal(err)
		}
		<-gate.entered
		if err := s.UpdateMany(context.Background(), "rotation", []Point{{Timestamp: int64(now - 60), Value: 123}}); err != nil {
			t.Fatal(err)
		}
		snapshot, err := s.Snapshot(context.Background(), "rotation")
		if err != nil || len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 123 {
			t.Fatalf("write during WAL handoff not visible: %+v, %v", snapshot, err)
		}
		gate.unblock()
	})
}

// These tests drive the real vendored commit/flush/recovery pipeline. An
// indefinitely blocked disk must not hold the commit mutex during one handoff,
// acknowledge a Sync write, flush/recycle the old WAL, or publish a new WAL.
func TestWALRotationBlockedSync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		strict := vfs.NewStrictMem()
		fs := &rotationFS{FS: strict}
		db := openRotationDB(t, fs, true)
		mustSetRotation(t, db, "before", pebble.Sync)
		gate := fs.arm("sync")
		defer gate.unblock()
		flush := make(chan (<-chan struct{}), 1)
		go func() {
			done, err := db.AsyncFlush()
			if err != nil {
				t.Error(err)
			}
			flush <- done
		}()
		<-gate.entered
		synctest.Wait()
		logs, err := fs.List("/db")
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range logs {
			if strings.HasSuffix(name, ".log") && name != fs.firstLog {
				t.Fatalf("next WAL published before old EOF sync: %s", name)
			}
		}
		write := make(chan error, 1)
		go func() { write <- db.Set([]byte("after"), []byte("after"), pebble.NoSync) }()
		synctest.Wait()
		assertRotationResult(t, write)
		assertRotationValue(t, db, "after")
		select {
		case done := <-flush:
			assertRotationBlocked(t, done, "flush completed before WAL handoff")
		default:
			t.Fatal("AsyncFlush blocked on fsync")
		}
		syncWrite := make(chan error, 1)
		go func() { syncWrite <- db.Set([]byte("synced"), []byte("synced"), pebble.Sync) }()
		checkpoint := make(chan error, 1)
		go func() { checkpoint <- db.Checkpoint("/checkpoint") }()
		synctest.Wait()
		secondFlush := make(chan error, 1)
		go func() { secondFlush <- db.Flush() }()
		synctest.Wait()
		assertRotationBlocked(t, syncWrite, "Sync acknowledged before handoff")
		assertRotationBlocked(t, checkpoint, "checkpoint captured unpublished WAL")
		assertRotationBlocked(t, secondFlush, "unbounded handoffs accepted")
		gate.unblock()
		assertRotationResult(t, syncWrite)
		assertRotationResult(t, checkpoint)
		assertRotationResult(t, secondFlush)
		strict.SetIgnoreSyncs(true)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		strict.ResetToSyncedState()
		strict.SetIgnoreSyncs(false)
		db = openRotationDB(t, strict, false)
		defer db.Close()
		for _, key := range []string{"before", "after", "synced"} {
			assertRotationValue(t, db, key)
		}
	})
}

func TestWALRotationFailuresAreFailStop(t *testing.T) {
	t.Parallel()
	const childEnv = "GO_CARBON_WAL_ROTATION_FAILURE"
	if point := os.Getenv(childEnv); point != "" {
		synctest.Test(t, func(t *testing.T) {
			fs := &rotationFS{FS: vfs.NewMem()}
			db := openRotationDB(t, fs, true)
			mustSetRotation(t, db, "before", pebble.Sync)
			fs.mu.Lock()
			fs.failurePoint = point
			fs.mu.Unlock()
			if _, err := db.AsyncFlush(); err != nil {
				t.Fatal(err)
			}
			_ = db.LogData(nil, pebble.Sync)
			t.Fatal("WAL handoff failure did not stop the process")
		})
		return
	}
	for _, point := range []string{"sync", "create", "dir-sync", "next-sync"} {
		t.Run(point, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestWALRotationFailuresAreFailStop$")
			cmd.Env = append(os.Environ(), childEnv+"="+point)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "injected WAL rotation failure: "+point) {
				t.Fatalf("child did not fail from WAL handoff error: %v\n%s", err, output)
			}
		})
	}
}

func TestWALRotationConcurrentNaturalFlushAndRecycle(t *testing.T) {
	t.Parallel()
	var rotations, recycles atomic.Int64
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{
		WALRotationAsync: true, MemTableSize: 1 << 20,
		EventListener: &pebble.EventListener{WALCreated: func(info pebble.WALCreateInfo) {
			rotations.Add(1)
			if info.RecycledFileNum != 0 {
				recycles.Add(1)
			}
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	const writers, writes = 8, 1000
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Go(func() {
			for i := range writes {
				key := fmt.Sprintf("%d/%04d", writer, i)
				value := bytes.Repeat([]byte(key), 64)
				policy := pebble.NoSync
				if i%100 == 0 {
					policy = pebble.Sync
				}
				if err := db.Set([]byte(key), value, policy); err != nil {
					t.Error(err)
					return
				}
				got, closer, err := db.Get([]byte(key))
				if err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(got, value) {
					t.Errorf("write not visible: %s", key)
				}
				_ = closer.Close()
			}
		})
	}
	wg.Wait()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte{0}, []byte{255}, true); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if rotations.Load() < 4 {
		t.Fatalf("natural WAL rotations not exercised: %d", rotations.Load())
	}
	if recycles.Load() == 0 {
		t.Fatal("WAL recycling not exercised")
	}
	// The original synchronous implementation must read the same files.
	db, err = pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for writer := range writers {
		for i := range writes {
			key := fmt.Sprintf("%d/%04d", writer, i)
			got, closer, err := db.Get([]byte(key))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, bytes.Repeat([]byte(key), 64)) {
				t.Fatalf("recovered wrong bytes: %s", key)
			}
			_ = closer.Close()
		}
	}
}

func TestWALRotationPowerLoss(t *testing.T) {
	for _, cut := range []string{"sync", "create", "dir-sync", "next-sync"} {
		t.Run(cut, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				strict := vfs.NewStrictMem()
				fs := &rotationFS{FS: strict}
				db := openRotationDB(t, fs, true)
				mustSetRotation(t, db, "durable", pebble.Sync)
				mustSetRotation(t, db, "old-unsynced", pebble.NoSync)
				gate := fs.arm(cut)
				defer gate.unblock()
				done, err := db.AsyncFlush()
				if err != nil {
					t.Fatal(err)
				}
				mustSetRotation(t, db, "next-unsynced", pebble.NoSync)
				syncResult := make(chan error, 1)
				go func() { syncResult <- db.LogData(nil, pebble.Sync) }()
				<-gate.entered
				synctest.Wait()
				assertRotationBlocked(t, syncResult, "sync crossed crash boundary")
				// No subsequent Sync may persist data or directory entries. Drain
				// userspace cleanly, then discard all unpersisted filesystem state.
				strict.SetIgnoreSyncs(true)
				gate.unblock()
				assertRotationResult(t, syncResult)
				<-done
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				strict.ResetToSyncedState()
				strict.SetIgnoreSyncs(false)
				db = openRotationDB(t, strict, true)
				defer db.Close()
				assertRotationValue(t, db, "durable")
				if cut != "sync" {
					assertRotationValue(t, db, "old-unsynced")
				}
				_, closer, err := db.Get([]byte("next-unsynced"))
				if closer != nil {
					_ = closer.Close()
				}
				if !errors.Is(err, pebble.ErrNotFound) {
					t.Fatalf("unsynced next WAL survived: %v", err)
				}
			})
		})
	}
}

func TestWALRotationCloseDrainsHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := &rotationFS{FS: vfs.NewMem()}
		db := openRotationDB(t, fs, true)
		mustSetRotation(t, db, "before", pebble.Sync)
		gate := fs.arm("sync")
		defer gate.unblock()
		if _, err := db.AsyncFlush(); err != nil {
			t.Fatal(err)
		}
		<-gate.entered
		mustSetRotation(t, db, "after", pebble.NoSync)
		closed := make(chan error, 1)
		go func() { closed <- db.Close() }()
		synctest.Wait()
		assertRotationBlocked(t, closed, "Close abandoned WAL handoff")
		gate.unblock()
		assertRotationResult(t, closed)
		db = openRotationDB(t, fs, true)
		defer db.Close()
		assertRotationValue(t, db, "before")
		assertRotationValue(t, db, "after")
	})
}

func openRotationDB(t *testing.T, fs vfs.FS, async bool) *pebble.DB {
	t.Helper()
	if err := fs.MkdirAll("/db", 0750); err != nil {
		t.Fatal(err)
	}
	parent, err := fs.OpenDir("/")
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = parent.Close()
	db, err := pebble.Open("/db", &pebble.Options{FS: fs, WALRotationAsync: async, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func mustSetRotation(t *testing.T, db *pebble.DB, key string, options *pebble.WriteOptions) {
	t.Helper()
	if err := db.Set([]byte(key), []byte(key), options); err != nil {
		t.Fatal(err)
	}
}

func assertRotationValue(t *testing.T, db *pebble.DB, key string) {
	t.Helper()
	value, closer, err := db.Get([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if !bytes.Equal(value, []byte(key)) {
		t.Fatalf("%s: %q", key, value)
	}
}

func assertRotationResult(t *testing.T, result <-chan error) {
	t.Helper()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func assertRotationBlocked[T any](t *testing.T, result <-chan T, message string) {
	t.Helper()
	select {
	case <-result:
		t.Fatal(message)
	default:
	}
}

type rotationGate struct {
	point   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *rotationGate) unblock() { g.once.Do(func() { close(g.release) }) }

type rotationFS struct {
	vfs.FS
	mu           sync.Mutex
	gate         *rotationGate
	firstLog     string
	failurePoint string
}

func (fs *rotationFS) fail(point string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.failurePoint == point {
		return fmt.Errorf("injected WAL rotation failure: %s", point)
	}
	return nil
}

func (fs *rotationFS) arm(point string) *rotationGate {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.gate = &rotationGate{point: point, entered: make(chan struct{}), release: make(chan struct{})}
	return fs.gate
}

func (fs *rotationFS) block(point string) {
	fs.mu.Lock()
	gate := fs.gate
	if gate == nil || gate.point != point {
		fs.mu.Unlock()
		return
	}
	fs.gate = nil
	fs.mu.Unlock()
	close(gate.entered)
	<-gate.release
}

func (fs *rotationFS) Create(name string) (vfs.File, error) {
	if !strings.HasSuffix(name, ".log") {
		return fs.FS.Create(name)
	}
	fs.block("create")
	if err := fs.fail("create"); err != nil {
		return nil, err
	}
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	first := fs.firstLog == ""
	if first {
		fs.firstLog = fs.PathBase(name)
	}
	fs.mu.Unlock()
	point := "next-sync"
	if first {
		point = "sync"
	}
	return rotationFile{File: f, fs: fs, point: point}, nil
}

func (fs *rotationFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	return rotationFile{File: f, fs: fs, point: "dir-sync"}, err
}

type rotationFile struct {
	vfs.File
	fs    *rotationFS
	point string
}

var _ io.Closer = rotationFile{}

func (f rotationFile) Sync() error {
	f.fs.block(f.point)
	if err := f.fs.fail(f.point); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f rotationFile) SyncData() error {
	f.fs.block(f.point)
	if err := f.fs.fail(f.point); err != nil {
		return err
	}
	return f.File.SyncData()
}
