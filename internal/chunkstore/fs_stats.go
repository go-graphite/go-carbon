package chunkstore

import (
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/pebble/vfs"
)

// FS operation kinds and file kinds counted by countingFS.
const (
	FSRead = iota
	FSWrite
	FSSync
	FSCreate
	FSRemove
	numFSOps
)

const (
	FSTable    = iota // Pebble tables (NNNNNN.sst): flush and compaction writes, compaction and point reads
	FSWAL             // write-ahead log (.log)
	FSManifest        // MANIFEST-*
	FSIngest          // go-carbon's staged conversion tables (other .sst names)
	FSOther           // directories, markers, options and everything else
	numFSKinds
)

// FSOpNames and FSKindNames label the counters for export.
var (
	FSOpNames   = [numFSOps]string{"read", "write", "sync", "create", "remove"}
	FSKindNames = [numFSKinds]string{"table", "wal", "manifest", "ingest", "other"}
)

// FSStats holds cumulative operation and byte counts by operation and file
// kind. Bytes are counted for reads and writes only.
type FSStats struct {
	Ops, Bytes [numFSOps][numFSKinds]uint64
}

type paddedCounter struct {
	n atomic.Uint64
	_ [56]byte
}

// countingFS counts the store's filesystem operations by kind of file, so
// dashboards can show table read and write IOPS, WAL and manifest traffic and
// sync rates. Pebble v1.1 does not tag a table read as compaction or user
// read, so table reads cover both, and table writes cover flushes and
// compactions. It is always installed and costs an atomic add or two per
// operation, small beside the system call it wraps.
type countingFS struct {
	vfs.FS
	ops, bytes [numFSOps][numFSKinds]paddedCounter
}

func newCountingFS(fs vfs.FS) *countingFS { return &countingFS{FS: fs} }

func fsKind(name string) int {
	base := filepath.Base(name)
	switch {
	case strings.HasSuffix(base, ".sst"):
		digits := strings.TrimSuffix(base, ".sst")
		for _, c := range digits {
			if c < '0' || c > '9' {
				return FSIngest
			}
		}
		if digits == "" {
			return FSIngest
		}
		return FSTable
	case strings.HasSuffix(base, ".log"):
		return FSWAL
	case strings.HasPrefix(base, "MANIFEST-"):
		return FSManifest
	}
	return FSOther
}

func (fs *countingFS) count(op, kind int) { fs.ops[op][kind].n.Add(1) }

func (fs *countingFS) wrap(f vfs.File, err error, kind int) (vfs.File, error) {
	if err != nil {
		return f, err
	}
	return &countingFile{File: f, fs: fs, kind: kind}, nil
}

func (fs *countingFS) Create(name string) (vfs.File, error) {
	kind := fsKind(name)
	f, err := fs.FS.Create(name)
	if err == nil {
		fs.count(FSCreate, kind)
	}
	return fs.wrap(f, err, kind)
}

func (fs *countingFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	kind := fsKind(name)
	f, err := fs.FS.Open(name, opts...)
	return fs.wrap(f, err, kind)
}

func (fs *countingFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	kind := fsKind(name)
	f, err := fs.FS.OpenReadWrite(name, opts...)
	return fs.wrap(f, err, kind)
}

func (fs *countingFS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.FS.OpenDir(name)
	return fs.wrap(f, err, FSOther)
}

func (fs *countingFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	kind := fsKind(newname)
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err == nil {
		fs.count(FSCreate, kind)
	}
	return fs.wrap(f, err, kind)
}

func (fs *countingFS) Remove(name string) error {
	err := fs.FS.Remove(name)
	if err == nil {
		fs.count(FSRemove, fsKind(name))
	}
	return err
}

// Stats returns the cumulative counts.
func (fs *countingFS) Stats() FSStats {
	var out FSStats
	for op := range numFSOps {
		for kind := range numFSKinds {
			out.Ops[op][kind] = fs.ops[op][kind].n.Load()
			out.Bytes[op][kind] = fs.bytes[op][kind].n.Load()
		}
	}
	return out
}

type countingFile struct {
	vfs.File
	fs   *countingFS
	kind int
}

func (f *countingFile) io(op, n int) {
	f.fs.ops[op][f.kind].n.Add(1)
	if n > 0 {
		f.fs.bytes[op][f.kind].n.Add(uint64(n))
	}
}

func (f *countingFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.io(FSRead, n)
	return n, err
}

func (f *countingFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.io(FSRead, n)
	return n, err
}

func (f *countingFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	f.io(FSWrite, n)
	return n, err
}

func (f *countingFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.io(FSWrite, n)
	return n, err
}

func (f *countingFile) Sync() error {
	f.fs.count(FSSync, f.kind)
	return f.File.Sync()
}

func (f *countingFile) SyncData() error {
	f.fs.count(FSSync, f.kind)
	return f.File.SyncData()
}

// SyncTo counts only a sync that made the prefix durable; a range writeback
// request is not a sync.
func (f *countingFile) SyncTo(length int64) (bool, error) {
	full, err := f.File.SyncTo(length)
	if full {
		f.fs.count(FSSync, f.kind)
	}
	return full, err
}
