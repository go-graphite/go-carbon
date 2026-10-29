//go:build linux && (amd64 || arm64)

package whisperio

import (
	"errors"
	"os"
	"slices"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	fiemapIOCTL  = 0xc020660b
	fiemapSync   = 1
	extentLast   = 1
	extentShared = 0x2000
)

type fileExtent struct{ logical, physical, length uint64 }
type fiemapExtent struct {
	Logical, Physical, Length uint64
	Reserved64                [2]uint64
	Flags                     uint32
	Reserved                  [3]uint32
}
type fiemapRequest struct {
	Start, Length                  uint64
	Flags, Mapped, Count, Reserved uint32
	Extents                        [64]fiemapExtent
}

// Both descriptors must stay immutable until the comparison finishes. Source
// is writer-locked and the private capture is owned by this converter.
func sameFileExtents(left, right *os.File) (bool, error) {
	a, err := left.Stat()
	if err != nil {
		return false, err
	}
	b, err := right.Stat()
	if err != nil {
		return false, err
	}
	if !a.Mode().IsRegular() || !b.Mode().IsRegular() || a.Size() != b.Size() {
		return false, nil
	}
	var ls, rs unix.Stat_t
	if err := unix.Fstat(int(left.Fd()), &ls); err != nil {
		return false, err
	}
	if err := unix.Fstat(int(right.Fd()), &rs); err != nil {
		return false, err
	}
	if ls.Dev != rs.Dev {
		return false, nil
	}
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(left.Fd()), &fs); err != nil {
		return false, err
	}
	if fs.Type != unix.XFS_SUPER_MAGIC {
		return false, nil
	}
	x, ok, err := synchronizedExtents(left, uint64(a.Size()))
	if err != nil || !ok {
		return false, err
	}
	y, ok, err := synchronizedExtents(right, uint64(b.Size()))
	return ok && slices.Equal(x, y), err
}

func synchronizedExtents(file *os.File, size uint64) ([]fileExtent, bool, error) {
	var result []fileExtent
	for start := uint64(0); start < size; {
		request := fiemapRequest{Start: start, Length: size - start, Flags: fiemapSync, Count: 64}
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), fiemapIOCTL, uintptr(unsafe.Pointer(&request)))
		if errno != 0 {
			if errors.Is(errno, unix.ENOTTY) || errors.Is(errno, unix.EOPNOTSUPP) || errors.Is(errno, unix.EBADR) {
				return nil, false, nil
			}
			return nil, false, errno
		}
		if request.Mapped > request.Count {
			return nil, false, nil
		}
		if request.Mapped == 0 {
			return result, true, nil
		}
		last := false
		for _, extent := range request.Extents[:request.Mapped] {
			// Unknown/delalloc, unwritten, inline, encoded and future flags cannot
			// prove equality. Only initialized plain XFS blocks qualify.
			if extent.Flags & ^uint32(extentLast|extentShared) != 0 || extent.Length == 0 || extent.Logical > size || extent.Logical+extent.Length < extent.Logical || extent.Physical+extent.Length < extent.Physical {
				return nil, false, nil
			}
			end := min(extent.Logical+extent.Length, size)
			if end <= start || extent.Logical < start {
				return nil, false, nil
			}
			item := fileExtent{extent.Logical, extent.Physical, end - extent.Logical}
			if len(result) > 0 {
				prev := &result[len(result)-1]
				if prev.logical+prev.length == item.logical && prev.physical+prev.length == item.physical {
					prev.length += item.length
				} else {
					result = append(result, item)
				}
			} else {
				result = append(result, item)
			}
			start = end
			last = extent.Flags&extentLast != 0
		}
		if len(result) > 4096 {
			return nil, false, nil
		}
		if last {
			return result, true, nil
		}
	}
	return result, true, nil
}
