package chunkstore

import "github.com/cockroachdb/pebble/vfs"

// noSyncFS keeps real files, locks and renames for logical storage tests.
// Durability and crash tests supply their own filesystem instead.
type noSyncFS struct{ vfs.FS }

func noSyncFile(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return f, err
	}
	return noSync{f}, nil
}

func (fs noSyncFS) Create(name string) (vfs.File, error) {
	return noSyncFile(fs.FS.Create(name))
}

func (fs noSyncFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return noSyncFile(fs.FS.Open(name, opts...))
}

func (fs noSyncFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return noSyncFile(fs.FS.OpenReadWrite(name, opts...))
}

func (fs noSyncFS) OpenDir(name string) (vfs.File, error) {
	return noSyncFile(fs.FS.OpenDir(name))
}

func (fs noSyncFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	return noSyncFile(fs.FS.ReuseForWrite(oldname, newname))
}

type noSync struct{ vfs.File }

func (noSync) Sync() error                { return nil }
func (noSync) SyncData() error            { return nil }
func (noSync) SyncTo(int64) (bool, error) { return true, nil }
