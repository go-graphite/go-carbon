package chunkstore

import (
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

type nativeLowDiskFS struct{ vfs.FS }

func (nativeLowDiskFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{TotalBytes: 100, AvailBytes: 5}, nil
}

func TestNativeDiskCheckLowSpace(t *testing.T) {
	s := &Store{fs: nativeLowDiskFS{vfs.NewMem()}, dir: "/db"}
	if err := s.nativeDiskCheck(); err == nil || !strings.Contains(err.Error(), "native bulk hard stop: less than 10% free disk") {
		t.Fatalf("5%% free disk must stop native bulk operations: %v", err)
	}
}
