package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

// countingSyncFS counts the data syncs that reach storage per file and can
// fail one file's sync.
type countingSyncFS struct {
	vfs.FS
	mu     sync.Mutex
	syncs  map[string]int
	failOn string
}

func (fs *countingSyncFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &countingSyncFile{File: f, fs: fs, name: name}, nil
}

func (fs *countingSyncFS) count(name string) int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.syncs[name]
}

type countingSyncFile struct {
	vfs.File
	fs   *countingSyncFS
	name string
}

func (f *countingSyncFile) SyncData() error {
	f.fs.mu.Lock()
	f.fs.syncs[f.name]++
	fail := f.name == f.fs.failOn
	f.fs.mu.Unlock()
	if fail {
		return errors.New("injected sync failure")
	}
	return f.File.SyncData()
}

func (f *countingSyncFile) Sync() error { return f.SyncData() }

func writeAndFinish(t *testing.T, fs vfs.FS, name string) {
	t.Helper()
	f, err := fs.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("table")); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
}

func syncDir(fs vfs.FS, dir string) error {
	d, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func TestDeferredSyncBatchesTablesAtDirectorySync(t *testing.T) {
	base := &countingSyncFS{FS: vfs.NewMem(), syncs: map[string]int{}}
	for _, dir := range []string{"/store", "/store/ingest"} {
		if err := base.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fs := newDeferredSyncFS(base, "/store", 4)
	var tables []string
	for i := range 10 {
		tables = append(tables, fmt.Sprintf("/store/%06d.sst", i+1))
		writeAndFinish(t, fs, tables[i])
	}
	for _, name := range []string{"/store/MANIFEST-000001", "/store/ingest/000001.sst", "/store/stage.sst"} {
		writeAndFinish(t, fs, name)
		if base.count(name) != 1 {
			t.Fatalf("%s sync deferred, want synchronous", name)
		}
	}
	for _, name := range tables {
		if base.count(name) != 0 {
			t.Fatalf("%s synced before the directory sync", name)
		}
	}
	if err := syncDir(fs, "/store"); err != nil {
		t.Fatal(err)
	}
	for _, name := range tables {
		if base.count(name) != 1 {
			t.Fatalf("%s synced %d times by the directory sync, want 1", name, base.count(name))
		}
	}
	if err := syncDir(fs, "/store"); err != nil {
		t.Fatal(err)
	}
	if base.count(tables[0]) != 1 {
		t.Fatal("a later directory sync repeated a finished batch")
	}
}

// A failed deferred sync must fail every later directory sync: another job's
// tables may have been in the failed batch.
func TestDeferredSyncFailureIsSticky(t *testing.T) {
	base := &countingSyncFS{FS: vfs.NewMem(), syncs: map[string]int{}, failOn: "/store/000002.sst"}
	if err := base.MkdirAll("/store", 0o755); err != nil {
		t.Fatal(err)
	}
	fs := newDeferredSyncFS(base, "/store", 2)
	writeAndFinish(t, fs, "/store/000001.sst")
	writeAndFinish(t, fs, "/store/000002.sst")
	if err := syncDir(fs, "/store"); err == nil {
		t.Fatal("failed table sync reported success")
	}
	writeAndFinish(t, fs, "/store/000003.sst")
	if err := syncDir(fs, "/store"); err == nil {
		t.Fatal("directory sync succeeded after a lost table sync")
	}
}

// A directory sync must not return while another one is still syncing tables
// that were finished before it started.
func TestDeferredSyncWaitsForRunningBatch(t *testing.T) {
	gate := make(chan struct{})
	base := &blockingSyncFS{FS: vfs.NewMem(), gate: gate}
	if err := base.MkdirAll("/store", 0o755); err != nil {
		t.Fatal(err)
	}
	fs := newDeferredSyncFS(base, "/store", 1)
	writeAndFinish(t, fs, "/store/000001.sst")
	first := make(chan error, 1)
	go func() { first <- syncDir(fs, "/store") }()
	for base.started.Load() == 0 {
	}
	second := make(chan error, 1)
	go func() { second <- syncDir(fs, "/store") }()
	select {
	case <-second:
		t.Fatal("directory sync returned while the table was still syncing")
	default:
	}
	close(gate)
	if err := errors.Join(<-first, <-second); err != nil {
		t.Fatal(err)
	}
}

type blockingSyncFS struct {
	vfs.FS
	gate    chan struct{}
	started atomic.Int32
}

func (fs *blockingSyncFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &blockingSyncFile{File: f, fs: fs}, nil
}

type blockingSyncFile struct {
	vfs.File
	fs *blockingSyncFS
}

func (f *blockingSyncFile) SyncData() error {
	f.fs.started.Add(1)
	<-f.fs.gate
	return f.File.SyncData()
}

// After a flush with deferred table syncs, a crash that drops every unsynced
// change must keep the flushed data.
func TestDeferredTableSyncsSurviveCrash(t *testing.T) {
	strict := vfs.NewStrictMem()
	const dir = "/store"
	s, _ := openTestStore(t, dir, Options{fs: strict, ParallelTableSyncs: 8})
	var names []string
	for i := range 200 {
		names = append(names, fmt.Sprintf("crash.metric%03d", i))
		createTestMetric(t, s, names[i])
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if s.deferredSync == nil {
		t.Fatal("deferred table syncs not enabled")
	}
	closeForRecovery(t, s, strict, true)
	reopened, _ := openTestStore(t, dir, Options{fs: strict, ParallelTableSyncs: 8})
	defer closeTestStore(t, reopened)
	for _, name := range names {
		if _, err := reopened.Metadata(context.Background(), name); err != nil {
			t.Fatalf("%s lost after crash: %v", name, err)
		}
	}
}

// Batches overlap: a later directory sync syncs its own tables while an
// earlier batch is still syncing, yet returns only after that batch finishes.
func TestDeferredSyncBatchesOverlap(t *testing.T) {
	gate := make(chan struct{})
	base := &selectiveBlockFS{FS: vfs.NewMem(), block: "/store/000001.sst", gate: gate}
	if err := base.MkdirAll("/store", 0o755); err != nil {
		t.Fatal(err)
	}
	fs := newDeferredSyncFS(base, "/store", 4)
	writeAndFinish(t, fs, "/store/000001.sst")
	first := make(chan error, 1)
	go func() { first <- syncDir(fs, "/store") }()
	for !base.blocked.Load() {
	}
	writeAndFinish(t, fs, "/store/000002.sst")
	second := make(chan error, 1)
	go func() { second <- syncDir(fs, "/store") }()
	for base.synced.Load() == 0 {
	}
	select {
	case <-second:
		t.Fatal("directory sync returned before an earlier batch finished")
	default:
	}
	close(gate)
	if err := errors.Join(<-first, <-second); err != nil {
		t.Fatal(err)
	}
}

type selectiveBlockFS struct {
	vfs.FS
	block   string
	gate    chan struct{}
	blocked atomic.Bool
	synced  atomic.Int32
}

func (fs *selectiveBlockFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &selectiveBlockFile{File: f, fs: fs, name: name}, nil
}

type selectiveBlockFile struct {
	vfs.File
	fs   *selectiveBlockFS
	name string
}

func (f *selectiveBlockFile) SyncData() error {
	if f.name == f.fs.block {
		f.fs.blocked.Store(true)
		<-f.fs.gate
	} else {
		f.fs.synced.Add(1)
	}
	return f.File.SyncData()
}
