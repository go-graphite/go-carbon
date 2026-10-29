package main

import (
	"fmt"
	"os"
	"syscall"
)

func checkpointFileIdentity(info os.FileInfo) (uint64, uint64, int64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, 0, fmt.Errorf("unsupported file identity for %s", info.Name())
	}
	return uint64(stat.Dev), stat.Ino, stat.Ctimespec.Sec*1e9 + stat.Ctimespec.Nsec, nil
}
