package main

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func copyEnableCold(t *testing.T) {
	t.Helper()
	copyBeforeFileRead = func(f *os.File) error {
		return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	}
}
