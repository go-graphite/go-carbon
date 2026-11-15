//go:build linux

package main

import (
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sys/unix"
)

// Direct output applies only to this newly created staging descriptor. SST
// readers open independent buffered descriptors after ingestion. Alignment
// padding is removed before the caller's final fsync, so it never enters a
// published SST. Reuse the private allocation across this owner's files.
func coreWriteDirect(f vfs.File, data, scratch []byte) ([]byte, error) {
	if f.Fd() == vfs.InvalidFd {
		return scratch, fmt.Errorf("direct SST needs an OS file: %w", os.ErrInvalid)
	}
	const alignment = 4096
	padded := (len(data) + alignment - 1) &^ (alignment - 1)
	if padded == 0 {
		return scratch, nil
	}
	if cap(scratch) < padded+alignment-1 {
		scratch = make([]byte, padded+alignment-1)
	} else {
		scratch = scratch[:padded+alignment-1]
	}
	offset := int(-uintptr(unsafe.Pointer(&scratch[0])) & (alignment - 1))
	aligned := scratch[offset : offset+padded]
	copy(aligned, data)
	clear(aligned[len(data):])
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return scratch, err
	}
	if _, err = unix.FcntlInt(f.Fd(), unix.F_SETFL, flags|unix.O_DIRECT); err != nil {
		return scratch, err
	}
	n, err := f.Write(aligned)
	if err != nil {
		return scratch, err
	}
	if n != len(aligned) {
		return scratch, io.ErrShortWrite
	}
	return scratch, unix.Ftruncate(int(f.Fd()), int64(len(data)))
}
