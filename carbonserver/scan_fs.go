package carbonserver

import (
	"errors"

	"golang.org/x/sys/unix"
)

// Directory primitives for the parallel scan. Opening and stating entries
// relative to an open directory resolves one path component per call, where
// filepath.Walk resolves the whole path again for every entry.

type scanType uint8

const (
	scanTypeUnknown scanType = iota
	scanTypeDir
	scanTypeRegular
	scanTypeSymlink
	scanTypeOther
)

type scanDirent struct {
	name string
	typ  scanType
}

func scanModeType(mode uint32) scanType {
	switch mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return scanTypeDir
	case unix.S_IFREG:
		return scanTypeRegular
	case unix.S_IFLNK:
		return scanTypeSymlink
	}
	return scanTypeOther
}

func scanOpenRoot(path string) (int, error) {
	for {
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if !errors.Is(err, unix.EINTR) {
			return fd, err
		}
	}
}

// scanOpenDir does not follow symbolic links, like filepath.Walk.
func scanOpenDir(dirfd int, name string) (int, error) {
	for {
		fd, err := unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if !errors.Is(err, unix.EINTR) {
			return fd, err
		}
	}
}

func scanStat(dirfd int, name string, follow bool, st *unix.Stat_t) error {
	flags := unix.AT_SYMLINK_NOFOLLOW
	if follow {
		flags = 0
	}
	for {
		err := unix.Fstatat(dirfd, name, st, flags)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}
