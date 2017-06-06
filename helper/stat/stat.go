// +build !linux

package stat

import (
	"os"
	"syscall"
)

// GetStat provides needed subset of FileInfo plus RealSize, Atime (if any) and Ctime (if any)
func GetStat(i os.FileInfo) FileStats {
	res := FileStats{
		Size:  i.Size(),
		MTime: i.ModTime().Unix(),
		CTime: i.ModTime().Unix(),
	}

	s := i.Sys().(*syscall.Stat_t)
	if s != nil {
		res.RealSize = s.Blocks * 512
	}

	return res
}
