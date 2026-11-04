package chunkstore

import (
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sys/unix"
)

// Preparation syncs before load. Drop its clean pages before opening the reader.
func checkpointBenchmarkNoCache(f vfs.File) error {
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
