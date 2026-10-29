//go:build linux

package main

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// A successful kernel path walk proves that no component is a symlink without
// issuing a separate lstat per component. No file contents are opened or changed.
// Symlinks keep the previous resolve-and-check behavior, including internal ones.
func provePlainSourcePath(path string) (bool, error) {
	fd, err := unix.Openat2(unix.AT_FDCWD, path, &unix.OpenHow{Flags: unix.O_PATH | unix.O_CLOEXEC, Resolve: unix.RESOLVE_NO_SYMLINKS})
	if err == nil {
		return true, unix.Close(fd)
	}
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) {
		return false, nil
	}
	return false, &os.PathError{Op: "validate source", Path: path, Err: err}
}
