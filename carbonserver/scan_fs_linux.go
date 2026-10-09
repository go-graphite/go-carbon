//go:build linux

package carbonserver

import (
	"bytes"
	"encoding/binary"
	"errors"

	"golang.org/x/sys/unix"
)

// scanReadDir appends the entries of the open directory fd, their names
// stored in names. getdents64 reports each entry's type, so
// directories need no separate stat call.
func scanReadDir(fd int, buf []byte, names *scanNames, entries []scanDirent) ([]scanDirent, error) {
	for {
		n, err := unix.Getdents(fd, buf)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil || n <= 0 {
			return entries, err
		}
		for off := 0; off < n; {
			// struct linux_dirent64 { u64 ino; s64 off; u16 reclen; u8 type; char name[]; }
			reclen := int(binary.NativeEndian.Uint16(buf[off+16:]))
			if reclen < 20 || off+reclen > n {
				return entries, unix.EIO
			}
			ino := binary.NativeEndian.Uint64(buf[off:])
			typ := buf[off+18]
			name := buf[off+19 : off+reclen]
			if i := bytes.IndexByte(name, 0); i >= 0 {
				name = name[:i]
			}
			off += reclen
			if ino == 0 || string(name) == "." || string(name) == ".." {
				continue
			}
			entries = append(entries, scanDirent{name: names.add(name), typ: scanDirentType(typ)})
		}
	}
}

func scanDirentType(typ uint8) scanType {
	switch typ {
	case unix.DT_UNKNOWN:
		return scanTypeUnknown
	case unix.DT_DIR:
		return scanTypeDir
	case unix.DT_REG:
		return scanTypeRegular
	case unix.DT_LNK:
		return scanTypeSymlink
	}
	return scanTypeOther
}
