package chunkstore

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

func incrementalBirthIdentity(source string, stat *syscall.Stat_t) (string, error) {
	return incrementalStatxIdentity(source, stat, unix.Statx)
}

func incrementalStatxIdentity(source string, stat *syscall.Stat_t, statx func(int, string, int, int, *unix.Statx_t) error) (string, error) {
	var current unix.Statx_t
	err := statx(unix.AT_FDCWD, source, 0, unix.STATX_INO|unix.STATX_BTIME, &current)
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EPERM) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if current.Mask&unix.STATX_INO == 0 || current.Ino != stat.Ino || unix.Mkdev(current.Dev_major, current.Dev_minor) != uint64(stat.Dev) {
		return "", errors.New("incremental source changed while reading its identity")
	}
	if current.Mask&unix.STATX_BTIME == 0 || current.Btime.Sec == 0 && current.Btime.Nsec == 0 {
		return "", nil
	}
	return fmt.Sprintf("birth:%d:%d:%09d", current.Ino, current.Btime.Sec, current.Btime.Nsec), nil
}
