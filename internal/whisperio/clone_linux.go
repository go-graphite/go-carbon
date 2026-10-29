//go:build linux

package whisperio

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func cloneFile(source *os.File, destination string, requireReflink bool) error {
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	cloneErr := unix.IoctlFileClone(int(out.Fd()), int(source.Fd()))
	if cloneErr != nil && !requireReflink {
		_, cloneErr = io.Copy(out, io.NewSectionReader(source, 0, 1<<63-1))
	}
	return errors.Join(cloneErr, out.Close())
}
