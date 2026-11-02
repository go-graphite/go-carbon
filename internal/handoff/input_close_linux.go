//go:build linux

package handoff

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func kernelUnread(raw syscall.RawConn) (int, error) {
	n := -1
	var ioctlErr error
	err := raw.Control(func(fd uintptr) {
		n, ioctlErr = unix.IoctlGetInt(int(fd), unix.SIOCINQ)
	})
	if err != nil {
		return -1, err
	}
	if ioctlErr != nil {
		return -1, ioctlErr
	}
	return n, nil
}
