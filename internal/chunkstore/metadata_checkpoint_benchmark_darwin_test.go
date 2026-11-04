package chunkstore

import (
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sys/unix"
)

// Bypass the filesystem data cache for disk measurements, including writes.
func checkpointBenchmarkNoCache(f vfs.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_NOCACHE, 1)
	return err
}
