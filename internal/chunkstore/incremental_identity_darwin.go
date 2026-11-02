package chunkstore

import (
	"fmt"
	"syscall"
)

func incrementalBirthIdentity(_ string, stat *syscall.Stat_t) (string, error) {
	if stat.Birthtimespec.Sec == 0 && stat.Birthtimespec.Nsec == 0 {
		return "", nil
	}
	return fmt.Sprintf("birth:%d:%d:%09d", stat.Ino, stat.Birthtimespec.Sec, stat.Birthtimespec.Nsec), nil
}
