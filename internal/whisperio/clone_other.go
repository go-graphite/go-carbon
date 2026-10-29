//go:build !linux

package whisperio

import (
	"errors"
	"io"
	"os"
)

func cloneFile(source *os.File, destination string, requireReflink bool) error {
	if requireReflink {
		return errors.New("reflink capture is supported only on Linux")
	}
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, io.NewSectionReader(source, 0, 1<<63-1))
	return errors.Join(err, out.Close())
}
