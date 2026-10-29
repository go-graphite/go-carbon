// Package testutil contains filesystem helpers for tests.
package testutil

import "github.com/cockroachdb/pebble/vfs"

// NoSyncFS keeps real files, locks and renames, but elides device flushes.
// Use it for logical storage tests, never for durability or crash recovery.
type NoSyncFS struct{ vfs.FS }

func noSyncFile(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return f, err
	}
	return noSync{f}, nil
}

func (fs NoSyncFS) Create(name string) (vfs.File, error) {
	return noSyncFile(fs.FS.Create(name))
}

func (fs NoSyncFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return noSyncFile(fs.FS.Open(name, opts...))
}

func (fs NoSyncFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return noSyncFile(fs.FS.OpenReadWrite(name, opts...))
}

func (fs NoSyncFS) OpenDir(name string) (vfs.File, error) {
	return noSyncFile(fs.FS.OpenDir(name))
}

func (fs NoSyncFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	return noSyncFile(fs.FS.ReuseForWrite(oldname, newname))
}

type noSync struct{ vfs.File }

func (noSync) Sync() error                { return nil }
func (noSync) SyncData() error            { return nil }
func (noSync) SyncTo(int64) (bool, error) { return true, nil }
