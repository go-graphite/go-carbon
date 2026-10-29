package persister

import (
	"os"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

var testStoreFiles sync.Map

// Logical mirror tests retain source files and reopen behavior without device
// flushes. Crash and acknowledgement tests open the store directly.
func openTestStore(t testing.TB, dir string, opts store.Options) (*store.Store, error) {
	t.Helper()
	// Captures and migration scratch files still use the real destination tree.
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	fs, loaded := testStoreFiles.LoadOrStore(dir, vfs.NewMem())
	if !loaded {
		t.Cleanup(func() { testStoreFiles.Delete(dir) })
	}
	return store.Open(dir, opts.WithFS(testSourceDirectoryFS{FS: fs.(vfs.FS), source: opts.IncrementalSource}))
}

type testSourceDirectoryFS struct {
	vfs.FS
	source string
}

func (fs testSourceDirectoryFS) Stat(name string) (os.FileInfo, error) {
	if fs.source != "" && name == fs.source {
		return os.Stat(name)
	}
	return fs.FS.Stat(name)
}
