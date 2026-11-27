package v2core

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestOwnerMetadataAllocatedSpace(t *testing.T) {
	e, err := openCore(t.TempDir(), coreTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.close(); err != nil {
			t.Error(err)
		}
	})
	for _, owner := range e.owners {
		// Multiple rotations exercise both new and recycled WALs. Two synced
		// writes trigger preallocation even when the initial offset is zero.
		for round := range 6 {
			for range 2 {
				if err := owner.db.Set([]byte("reservation-probe"), []byte("value"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			wals, err := filepath.Glob(filepath.Join(owner.dir, "*.log"))
			if err != nil || len(wals) == 0 {
				t.Fatalf("WALs=%v: %v", wals, err)
			}
			for _, path := range wals {
				info, err := os.Stat(path)
				if err != nil {
					if os.IsNotExist(err) { // Obsolete logs may be deleted concurrently.
						continue
					}
					t.Fatal(err)
				}
				allocated := info.Sys().(*syscall.Stat_t).Blocks * 512
				t.Logf("%s round=%d allocated=%d length=%d", path, round, allocated, info.Size())
				if allocated > info.Size()+64<<10 {
					t.Errorf("WAL allocated %d bytes for length %d, want <= length + 64 KiB", allocated, info.Size())
				}
			}
			if err := owner.db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
