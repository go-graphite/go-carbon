package chunkstore

import (
	"errors"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestIncrementalStatxIdentity(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mask    uint32
		ino     uint64
		minor   uint32
		birth   unix.StatxTimestamp
		statErr error
		want    string
		fail    bool
	}{
		{"birth", unix.STATX_INO | unix.STATX_BTIME, 42, 5, unix.StatxTimestamp{Sec: 1790000000, Nsec: 1}, nil, "birth:42:1790000000:000000001", false},
		{"no birth", unix.STATX_INO, 42, 5, unix.StatxTimestamp{}, nil, "", false},
		{"zero birth", unix.STATX_INO | unix.STATX_BTIME, 42, 5, unix.StatxTimestamp{}, nil, "", false},
		{"no syscall", 0, 0, 0, unix.StatxTimestamp{}, unix.ENOSYS, "", false},
		{"unsupported", 0, 0, 0, unix.StatxTimestamp{}, unix.EOPNOTSUPP, "", false},
		{"seccomp", 0, 0, 0, unix.StatxTimestamp{}, unix.EPERM, "", false},
		{"io error", 0, 0, 0, unix.StatxTimestamp{}, unix.EIO, "", true},
		{"changed inode", unix.STATX_INO, 43, 5, unix.StatxTimestamp{}, nil, "", true},
		{"changed mount", unix.STATX_INO, 42, 1, unix.StatxTimestamp{}, nil, "", true},
		{"missing inode", 0, 42, 5, unix.StatxTimestamp{}, nil, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stat := &syscall.Stat_t{Dev: unix.Mkdev(259, 5), Ino: 42}
			got, err := incrementalStatxIdentity("/source", stat, func(fd int, path string, flags, mask int, current *unix.Statx_t) error {
				if fd != unix.AT_FDCWD || path != "/source" || flags != 0 || mask != unix.STATX_INO|unix.STATX_BTIME {
					t.Fatal("unexpected statx arguments")
				}
				*current = unix.Statx_t{Mask: tc.mask, Ino: tc.ino, Dev_major: 259, Dev_minor: tc.minor, Btime: tc.birth}
				return tc.statErr
			})
			if got != tc.want || (err != nil) != tc.fail {
				t.Fatalf("identity=%q err=%v", got, err)
			}
			if errors.Is(tc.statErr, unix.EIO) && !errors.Is(err, unix.EIO) {
				t.Fatal("lost statx error")
			}
		})
	}
}
