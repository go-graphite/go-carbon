package testutil

import (
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

var storeFiles sync.Map

// MemoryStoreFS retains one logical store's files across graceful reopen.
// Source directories remain on disk so migration markers check their real identity.
func MemoryStoreFS(t testing.TB, dir, source string) vfs.FS {
	t.Helper()
	fs, loaded := storeFiles.LoadOrStore(dir, vfs.NewMem())
	if !loaded {
		t.Cleanup(func() { storeFiles.Delete(dir) })
	}
	return sourceDirectoryFS{FS: fs.(vfs.FS), source: source}
}

type sourceDirectoryFS struct {
	vfs.FS
	source string
}

func (fs sourceDirectoryFS) Stat(name string) (os.FileInfo, error) {
	if fs.source != "" && name == fs.source {
		return os.Stat(name)
	}
	return fs.FS.Stat(name)
}
