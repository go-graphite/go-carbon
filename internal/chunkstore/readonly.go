package chunkstore

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
)

// ReadOnly reports whether mutations are disabled for this handle.
func (s *Store) ReadOnly() bool { return s.readOnly }

// Keep process-local lock ownership too: POSIX record locks alone do not reject
// a second open by the same process. The ordinary path still delegates to VFS.
var storeLocks = struct {
	sync.Mutex
	held map[string]bool
}{held: make(map[string]bool)}

type storeLockFS struct{ vfs.FS }

func (fs *storeLockFS) Lock(name string) (io.Closer, error) { return fs.lock(name, false) }
func (fs *storeLockFS) lock(name string, readOnly bool) (io.Closer, error) {
	key := fmt.Sprintf("%T:%p:%s", fs.FS, fs.FS, name)
	storeLocks.Lock()
	defer storeLocks.Unlock()
	if storeLocks.held[key] {
		return nil, fmt.Errorf("store lock already held: %s", name)
	}
	var c io.Closer
	var err error
	if readOnly {
		c, err = lockExistingStoreFile(fs.FS, name)
	} else {
		c, err = fs.FS.Lock(name)
	}
	if err != nil {
		return nil, err
	}
	storeLocks.held[key] = true
	return &storeLockCloser{key: key, Closer: c}, nil
}

type storeLockCloser struct {
	io.Closer
	key string
}

func (c *storeLockCloser) Close() error {
	storeLocks.Lock()
	defer storeLocks.Unlock()
	err := c.Closer.Close()
	delete(storeLocks.held, c.key)
	return err
}

// The read-only filesystem is a second boundary around all store I/O. Pebble's
// ReadOnly option prevents its own WAL/manifest/compaction writes; this wrapper
// also refuses accidental writes in the surrounding format and admin code.
type readOnlyFS struct{ vfs.FS }

func (fs readOnlyFS) Create(string) (vfs.File, error) { return nil, ErrReadOnly }
func (fs readOnlyFS) OpenReadWrite(string, ...vfs.OpenOption) (vfs.File, error) {
	return nil, ErrReadOnly
}
func (fs readOnlyFS) ReuseForWrite(string, string) (vfs.File, error) { return nil, ErrReadOnly }
func (fs readOnlyFS) Link(string, string) error                      { return ErrReadOnly }
func (fs readOnlyFS) Rename(string, string) error                    { return ErrReadOnly }
func (fs readOnlyFS) Remove(string) error                            { return ErrReadOnly }
func (fs readOnlyFS) RemoveAll(string) error                         { return ErrReadOnly }
func (fs readOnlyFS) MkdirAll(string, os.FileMode) error             { return ErrReadOnly }
func (fs readOnlyFS) Lock(name string) (io.Closer, error) {
	return fs.FS.(*storeLockFS).lock(name, true)
}
func (fs readOnlyFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return readOnlyFile{f}, nil
}
func (fs readOnlyFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return readOnlyFile{f}, nil
}

type readOnlyFile struct{ vfs.File }

func (readOnlyFile) Write([]byte) (int, error)          { return 0, ErrReadOnly }
func (readOnlyFile) WriteAt([]byte, int64) (int, error) { return 0, ErrReadOnly }
func (readOnlyFile) Preallocate(int64, int64) error     { return ErrReadOnly }
func (readOnlyFile) Sync() error                        { return ErrReadOnly }
func (readOnlyFile) SyncData() error                    { return ErrReadOnly }
func (readOnlyFile) SyncTo(int64) (bool, error)         { return false, ErrReadOnly }
