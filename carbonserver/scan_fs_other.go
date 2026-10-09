//go:build !linux

package carbonserver

import (
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

// scanReadDir appends the entries of the open directory fd, using the
// portable directory reader on a duplicate descriptor.
func scanReadDir(fd int, _ []byte, _ *scanNames, entries []scanDirent) ([]scanDirent, error) {
	dup, err := unix.Dup(fd)
	if err != nil {
		return entries, err
	}
	f := os.NewFile(uintptr(dup), "")
	defer f.Close()
	list, err := f.ReadDir(-1)
	for _, e := range list {
		entries = append(entries, scanDirent{name: e.Name(), typ: scanFileModeType(e.Type())})
	}
	return entries, err
}

func scanFileModeType(mode fs.FileMode) scanType {
	switch {
	case mode.IsDir():
		return scanTypeDir
	case mode.IsRegular():
		return scanTypeRegular
	case mode&fs.ModeSymlink != 0:
		return scanTypeSymlink
	}
	return scanTypeOther
}
