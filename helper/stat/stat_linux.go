// +build linux

package stat

import (
	"os"
	"syscall"
)

func GetStat(i os.FileInfo) FileStats {
	res := FileStats{
		Size: i.Size(),
	}

	s := i.Sys().(*syscall.Stat_t)
	if s != nil {
		res.MTime, res.MTimeNS = s.Mtim.Unix()
		res.ATime, res.ATimeNS = s.Atim.Unix()
		res.CTime, res.CTimeNS = s.Ctim.Unix()
		res.RealSize = int64(s.Blocks) * 512
	}

	return res
}
