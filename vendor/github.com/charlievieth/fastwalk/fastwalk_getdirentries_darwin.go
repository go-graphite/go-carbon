//go:build darwin && go1.13 && !appengine && !nogetdirentries
// +build darwin,go1.13,!appengine,!nogetdirentries

package fastwalk

import (
	"io/fs"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const direntBufSize = 32 * 1024

var direntBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, direntBufSize)
		return &b
	},
}

func readDir(dirName string, fn func(dirName, entName string, de fs.DirEntry) error) error {
	fd, err := syscall.Open(dirName, syscall.O_RDONLY, 0)
	if err != nil {
		return &os.PathError{Op: "open", Path: dirName, Err: err}
	}
	defer syscall.Close(fd)

	p := direntBufPool.Get().(*[]byte)
	defer direntBufPool.Put(p)
	dbuf := *p

	var skipFiles bool
	var basep uintptr
	for {
		length, err := getdirentries(fd, dbuf, &basep)
		if err != nil {
			return &os.PathError{Op: "getdirentries64", Path: dirName, Err: err}
		}
		if length == 0 {
			break
		}
		buf := dbuf[:length]

		for i := 0; len(buf) > 0; i++ {
			reclen, ok := direntReclen(buf)
			if !ok || reclen > uint64(len(buf)) {
				break
			}
			rec := buf[:reclen]
			buf = buf[reclen:]
			typ := direntType(rec)
			if skipFiles && typ.IsRegular() {
				continue
			}
			const namoff = uint64(unsafe.Offsetof(syscall.Dirent{}.Name))
			namlen, ok := direntNamlen(rec)
			if !ok || namoff+namlen > uint64(len(rec)) {
				break
			}
			name := rec[namoff : namoff+namlen]
			for i, c := range name {
				if c == 0 {
					name = name[:i]
					break
				}
			}
			if string(name) == "." || string(name) == ".." {
				continue
			}
			nm := string(name)
			if err := fn(dirName, nm, newUnixDirent(dirName, nm, typ)); err != nil {
				if err != ErrSkipFiles {
					return err
				}
				skipFiles = true
			}
		}
	}

	return nil
}

// readInt returns the size-bytes unsigned integer in native byte order at offset off.
func readInt(b []byte, off, size uintptr) (uint64, bool) {
	if len(b) >= int(off+size) {
		p := b[off:]
		_ = p[1] // bounds check hint to compiler; see golang.org/issue/14808
		return uint64(p[0]) | uint64(p[1])<<8, true
	}
	return 0, false
}

// Statically assert that the size of Reclen and Namlen is 2.
var _ = ([2]int{})[unsafe.Sizeof(syscall.Dirent{}.Reclen)-1]
var _ = ([2]int{})[unsafe.Sizeof(syscall.Dirent{}.Namlen)-1]

func direntReclen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(syscall.Dirent{}.Reclen), unsafe.Sizeof(syscall.Dirent{}.Reclen))
}

func direntNamlen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(syscall.Dirent{}.Namlen), unsafe.Sizeof(syscall.Dirent{}.Namlen))
}

func direntType(buf []byte) os.FileMode {
	off := unsafe.Offsetof(syscall.Dirent{}.Type)
	if off >= uintptr(len(buf)) {
		return ^os.FileMode(0) // unknown
	}
	typ := buf[off]
	switch typ {
	case syscall.DT_BLK:
		return os.ModeDevice
	case syscall.DT_CHR:
		return os.ModeDevice | os.ModeCharDevice
	case syscall.DT_DIR:
		return os.ModeDir
	case syscall.DT_FIFO:
		return os.ModeNamedPipe
	case syscall.DT_LNK:
		return os.ModeSymlink
	case syscall.DT_REG:
		return 0
	case syscall.DT_SOCK:
		return os.ModeSocket
	}
	return ^os.FileMode(0)
}
