//go:build linux

package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

func openChildDir(parent int, name string, device uint64) (int, error) {
	fd, err := unix.Openat2(parent, name, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV})
	if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) {
		return fd, err
	}
	return openChildSameDevice(parent, name, device)
}
