package chunkstore

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
)

// deferredSyncFS turns Pebble's per-table syncs into parallel batches.
//
// Pebble syncs each finished table on its own: a flush that writes a thousand
// 2 MiB L0 tables waits for a thousand fdatasyncs in turn, and behind a busy
// device those waits are most of the flush. Pebble relies on new tables only
// after syncing their directory, just before it records them in the manifest
// (objstorage provider Sync), so the wrapper records each finished table and
// syncs all of them in parallel when a directory is synced. Every manifest
// commit still follows durable tables.
//
// Stock Pebble skips that directory sync when no object was created or
// removed since the provider's previous one, and clears the flag before
// syncing. A job whose table was created before another job's provider Sync
// and finished after it would then commit the table unsynced. The store sets
// pebble.Options.Local.AlwaysSyncDir (patches/pebble-provider-dir-sync.patch)
// so every job's provider Sync reaches this barrier.
//
// Only Pebble's own tables (NNNNNN.sst) directly in the store directory are
// deferred; go-carbon's staged ingest tables live in a subdirectory and keep
// their synchronous syncs.
type deferredSyncFS struct {
	vfs.FS
	dir      string
	parallel int

	mu      sync.Mutex
	pending []vfs.File
	// active holds batches still syncing. A directory sync waits for the
	// batches that were running when it started: they may hold tables
	// finished before it. Batches themselves overlap, so jobs committing
	// together do not queue behind each other's syncs.
	active []*syncBatch
	// failed is sticky: a lost table sync cannot be retried, and another job
	// may already have handed its tables to the failed batch.
	failed error
}

type syncBatch struct{ done chan struct{} }

func newDeferredSyncFS(fs vfs.FS, dir string, parallel int) *deferredSyncFS {
	return &deferredSyncFS{FS: fs, dir: filepath.Clean(dir), parallel: parallel}
}

func (fs *deferredSyncFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil || !fs.deferrable(name) {
		return f, err
	}
	return &deferredSyncFile{File: f, fs: fs}, nil
}

func (fs *deferredSyncFS) deferrable(name string) bool {
	if filepath.Dir(filepath.Clean(name)) != fs.dir {
		return false
	}
	base := filepath.Base(name)
	digits, ok := strings.CutSuffix(base, ".sst")
	if !ok || digits == "" {
		return false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// OpenDir puts the sync barrier on the store directory only: a WAL directory
// elsewhere is synced when a log is created and need not wait for tables.
func (fs *deferredSyncFS) OpenDir(name string) (vfs.File, error) {
	d, err := fs.FS.OpenDir(name)
	if err != nil || filepath.Clean(name) != fs.dir {
		return d, err
	}
	return &syncBarrierDir{File: d, fs: fs}, nil
}

// syncPending makes every table finished before the call durable: it syncs
// and closes the pending ones and waits for batches already syncing others.
func (fs *deferredSyncFS) syncPending() error {
	fs.mu.Lock()
	files, prior := fs.pending, slices.Clone(fs.active)
	fs.pending = nil
	batch := &syncBatch{done: make(chan struct{})}
	if len(files) > 0 {
		fs.active = append(fs.active, batch)
	}
	fs.mu.Unlock()
	err := fs.syncBatch(files)
	if len(files) > 0 {
		fs.mu.Lock()
		if err != nil && fs.failed == nil {
			fs.failed = err
		}
		fs.active = slices.DeleteFunc(fs.active, func(b *syncBatch) bool { return b == batch })
		fs.mu.Unlock()
		close(batch.done)
	}
	for _, b := range prior {
		<-b.done
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.failed
}

func (fs *deferredSyncFS) syncBatch(files []vfs.File) error {
	fs.mu.Lock()
	failed := fs.failed
	fs.mu.Unlock()
	if failed != nil {
		return errors.Join(failed, closeFiles(files))
	}
	if len(files) == 0 {
		return nil
	}
	errs := make([]error, len(files))
	work := make(chan int)
	var wg sync.WaitGroup
	for range min(max(1, fs.parallel), len(files)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				errs[i] = errors.Join(files[i].SyncData(), files[i].Close())
			}
		}()
	}
	for i := range files {
		work <- i
	}
	close(work)
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("deferred table sync: %w", err)
	}
	return nil
}

func closeFiles(files []vfs.File) error {
	var err error
	for _, f := range files {
		err = errors.Join(err, f.Close())
	}
	return err
}

// deferredSyncFile records syncs instead of performing them. A synced file is
// handed to its FS on Close and stays open until the next directory sync.
type deferredSyncFile struct {
	vfs.File
	fs       *deferredSyncFS
	deferred bool
}

func (f *deferredSyncFile) Sync() error {
	f.deferred = true
	return nil
}

func (f *deferredSyncFile) SyncData() error {
	f.deferred = true
	return nil
}

func (f *deferredSyncFile) Close() error {
	if !f.deferred {
		return f.File.Close()
	}
	f.fs.mu.Lock()
	f.fs.pending = append(f.fs.pending, f.File)
	f.fs.mu.Unlock()
	return nil
}

// syncBarrierDir makes deferred table syncs durable before the directory sync
// that precedes every manifest commit.
type syncBarrierDir struct {
	vfs.File
	fs *deferredSyncFS
}

func (d *syncBarrierDir) Sync() error {
	if err := d.fs.syncPending(); err != nil {
		return err
	}
	return d.File.Sync()
}

func (d *syncBarrierDir) SyncData() error { return d.Sync() }
