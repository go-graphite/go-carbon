package partlog

import (
	"os"
	"syscall"
)

func fileStamp(info os.FileInfo) (segmentStamp, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return segmentStamp{}, false
	}
	return segmentStamp{uint64(s.Dev), s.Ino, info.Size(), info.ModTime().UnixNano(), int64(s.Ctim.Sec), int64(s.Ctim.Nsec)}, true
}
