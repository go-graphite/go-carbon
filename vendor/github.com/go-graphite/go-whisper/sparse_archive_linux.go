package whisper

import (
	"os"
	"syscall"
)

func newArchiveExtentReader(source file, archiveEnd int64, readBytes int) *archiveExtentReader {
	// Small windows and ordinary dense files do not justify extent queries.
	if readBytes < 256<<10 {
		return nil
	}
	// memFile.Fd is not an operating-system descriptor. Only use real files.
	f, ok := source.(*os.File)
	if !ok {
		return nil
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < archiveEnd {
		return nil
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Blocks < 0 || stat.Blocks*512 >= info.Size()/4 {
		return nil
	}
	return &archiveExtentReader{seek: f.Seek, size: func() (int64, error) {
		info, err := f.Stat()
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}}
}
