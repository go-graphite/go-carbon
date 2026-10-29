package storecopy

import (
	"errors"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func CheckFreeSpace(directory string, reserve int64) error {
	var stat unix.Statfs_t
	for {
		if err := unix.Statfs(directory, &stat); err != nil {
			if !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.ENOTDIR) {
				return err
			}
			parent := filepath.Dir(directory)
			if parent == directory {
				return err
			}
			directory = parent
			continue
		}
		break
	}
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	if reserve < 0 || free < uint64(reserve) {
		return fmt.Errorf("disk reserve breached: free=%d required=%d", free, reserve)
	}
	return nil
}
