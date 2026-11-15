//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package chunkstore

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sys/unix"
)

// A shared POSIX lock conflicts with the exclusive lock taken by ordinary VFS
// writers, and can be acquired on an existing read-only file without truncation.
func lockExistingStoreFile(fs vfs.FS, name string) (io.Closer, error) {
	if _, ok := fs.(*vfs.MemFS); ok {
		return fs.Lock(name)
	}
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	if f.Fd() == vfs.InvalidFd {
		_ = f.Close()
		// Memory filesystems have no OS descriptor and their Lock is in memory.
		// Refuse unknown implementations rather than assume their Lock is safe.
		if _, ok := fs.(*vfs.MemFS); ok {
			return fs.Lock(name)
		}
		return nil, fmt.Errorf("read-only lock unsupported for %T", fs)
	}
	lock := unix.Flock_t{Type: unix.F_RDLCK, Whence: io.SeekStart}
	if err = unix.FcntlFlock(f.Fd(), unix.F_SETLK, &lock); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
