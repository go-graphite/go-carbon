package whisperio

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/testutil"
)

// These tests compare storage contents across graceful reopen, not power loss.
// Keep their real files and locking without waiting for device flushes.
func openTestStore(dir string, opts store.Options) (*store.Store, error) {
	return store.Open(dir, opts.WithFS(testutil.NoSyncFS{FS: vfs.Default}))
}

// Each directory keeps its own filesystem across graceful store reopen.
func openMemoryTestStore(t testing.TB, dir string, opts store.Options) (*store.Store, error) {
	t.Helper()
	if _, ok := t.(*testing.B); ok {
		return store.Open(dir, opts)
	}
	return store.Open(dir, opts.WithFS(testutil.MemoryStoreFS(t, dir, opts.IncrementalSource)))
}
