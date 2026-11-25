package v2core

import "github.com/cockroachdb/pebble/vfs"

// Owner databases persist small metadata updates. Pebble otherwise reserves
// 110% of the memtable limit for each WAL, regardless of the bytes written.
// Suppress that optional reservation without changing memtables or durability.
type ownerMetadataFS struct{ vfs.FS }

type ownerMetadataFile struct{ vfs.File }

func (ownerMetadataFile) Preallocate(int64, int64) error { return nil }

func (fs ownerMetadataFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return ownerMetadataFile{f}, nil
}

func (fs ownerMetadataFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.OpenReadWrite(name, opts...)
	if err != nil {
		return nil, err
	}
	return ownerMetadataFile{f}, nil
}

func (fs ownerMetadataFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err != nil {
		return nil, err
	}
	return ownerMetadataFile{f}, nil
}
