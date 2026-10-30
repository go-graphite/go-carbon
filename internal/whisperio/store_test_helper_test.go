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
	// Oracle fixtures need only a few MiB; parallel cases should not reserve
	// production-sized caches and memtables for every store.
	if opts.CacheSize <= 0 || opts.CacheSize > 4<<20 {
		opts.CacheSize = 4 << 20
	}
	if opts.MemTableSize <= 0 || opts.MemTableSize > 4<<20 {
		opts.MemTableSize = 4 << 20
	}
	return store.Open(dir, opts.WithFS(testutil.MemoryStoreFS(t, dir, opts.IncrementalSource)))
}
