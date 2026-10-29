//go:build linux
// +build linux

package whisper

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

func copySnapshotContents(out, in *os.File) error {
	// Linux FICLONE, _IOW(0x94, 9, int), also preserves sparse extents.
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, out.Fd(), 0x40049409, in.Fd()); errno == 0 {
		return nil
	}
	if err := out.Truncate(0); err != nil {
		return err
	}
	return copySnapshotSparse(out, in)
}

func copySnapshotSparse(out, in *os.File) error {
	info, err := in.Stat()
	if err != nil {
		return err
	}
	const seekData, seekHole = 3, 4
	for offset := int64(0); offset < info.Size(); {
		data, err := syscall.Seek(int(in.Fd()), offset, seekData)
		if err == syscall.ENXIO {
			break
		}
		if err == syscall.EINVAL { // Filesystem does not implement sparse seeking.
			if _, err := in.Seek(0, io.SeekStart); err != nil {
				return err
			}
			if _, err := out.Seek(0, io.SeekStart); err != nil {
				return err
			}
			_, err = io.Copy(out, in)
			return err
		}
		if err != nil {
			return err
		}
		hole, err := syscall.Seek(int(in.Fd()), data, seekHole)
		if err != nil {
			return err
		}
		if hole > info.Size() {
			hole = info.Size()
		}
		if hole <= data {
			return fmt.Errorf("snapshot sparse extent made no progress")
		}
		if _, err := out.Seek(data, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.CopyN(out, io.NewSectionReader(in, data, hole-data), hole-data); err != nil {
			return err
		}
		offset = hole
	}
	return out.Truncate(info.Size())
}
