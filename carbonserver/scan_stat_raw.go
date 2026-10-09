//go:build linux && (amd64 || arm64)

package carbonserver

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

// scanStatPath stats path, which ends with a NUL byte, without the copy
// unix.Fstatat makes of every path: a scan stats tens of millions.
func scanStatPath(dirfd int, path []byte, follow bool, st *unix.Stat_t) error {
	flags := unix.AT_SYMLINK_NOFOLLOW
	if follow {
		flags = 0
	}
	for {
		_, _, e := unix.Syscall6(scanFstatatTrap, uintptr(dirfd), uintptr(unsafe.Pointer(&path[0])), uintptr(unsafe.Pointer(st)), uintptr(flags), 0, 0) // skipcq: GSC-G103
		if e == 0 {
			return nil
		}
		if err := error(e); !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}
