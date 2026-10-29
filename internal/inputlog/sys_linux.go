//go:build linux

package inputlog

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// preallocate reserves size bytes, so later writes inside the file neither
// allocate blocks nor change its size, and fdatasync need not commit either.
func preallocate(f *os.File, size int64) error {
	for {
		err := unix.Fallocate(int(f.Fd()), 0, 0, size)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EOPNOTSUPP) {
			return f.Truncate(size)
		}
		return err
	}
}

// datasync writes the file's data, and only the metadata needed to read it.
func datasync(f *os.File) error {
	for {
		err := unix.Fdatasync(int(f.Fd()))
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// dropCache releases synced log pages: they are read again only by a replay,
// and must not evict the store's working set from the page cache.
func dropCache(f *os.File, off, length int64) {
	_ = unix.Fadvise(int(f.Fd()), off, length, unix.FADV_DONTNEED)
}
