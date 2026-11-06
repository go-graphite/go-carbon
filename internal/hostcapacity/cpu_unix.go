//go:build linux || darwin

package hostcapacity

import (
	"errors"
	"golang.org/x/sys/unix"
	"path/filepath"
)

func DiskSpace(path string) (free, total uint64, err error) {
	var stat unix.Statfs_t
	for {
		err = unix.Statfs(path, &stat)
		if err == nil {
			return uint64(stat.Bavail) * uint64(stat.Bsize), uint64(stat.Blocks) * uint64(stat.Bsize), nil
		}
		if !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTDIR) {
			return 0, 0, err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return 0, 0, err
		}
		path = parent
	}
}

func processCPU() float64 {
	var r unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
