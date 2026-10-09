package carbonserver

import (
	"errors"
	"unsafe"

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

// scanNames stores a directory's entry names in chunks, so names are views
// that cost no allocation each and growing never copies (and so never keeps)
// earlier names. The chunks are reused for the next directory at the same
// depth, when the entries of this one are gone; names must not be retained
// beyond that.
type scanNames struct {
	chunks [][]byte
	cur    int
}

func (n *scanNames) reset() {
	n.cur = 0
	if len(n.chunks) > 0 {
		n.chunks[0] = n.chunks[0][:0]
	}
}

func (n *scanNames) add(name []byte) string {
	if len(n.chunks) == 0 {
		n.chunks = append(n.chunks, make([]byte, 0, max(64<<10, len(name))))
	}
	c := &n.chunks[n.cur]
	if len(*c)+len(name) > cap(*c) {
		n.cur++
		if n.cur == len(n.chunks) {
			n.chunks = append(n.chunks, nil)
		}
		c = &n.chunks[n.cur]
		if cap(*c) < len(name) {
			*c = make([]byte, 0, max(64<<10, len(name)))
		}
		*c = (*c)[:0]
	}
	start := len(*c)
	*c = append(*c, name...)
	return unsafe.String(unsafe.SliceData((*c)[start:]), len(name)) // skipcq: GSC-G103
}
