//go:build !linux
// +build !linux

package whisper

import (
	"io"
	"os"
)

func copySnapshotContents(out, in *os.File) error { _, err := io.Copy(out, in); return err }
