// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build (linux || freebsd || openbsd || netbsd) && !appengine
// +build linux freebsd openbsd netbsd
// +build !appengine

package fastwalk

import (
	"io/fs"
	"os"
	"syscall"

	"github.com/charlievieth/fastwalk/internal/dirent"
)

// More than 5760 to work around https://golang.org/issue/24015.
const blockSize = 8192

// unknownFileMode is a sentinel (and bogus) os.FileMode
// value used to represent a syscall.DT_UNKNOWN Dirent.Type.
const unknownFileMode os.FileMode = os.ModeNamedPipe | os.ModeSocket | os.ModeDevice

func readDir(dirName string, fn func(dirName, entName string, de fs.DirEntry) error) error {
	fd, err := open(dirName, 0, 0)
	if err != nil {
		return &os.PathError{Op: "open", Path: dirName, Err: err}
	}
	defer syscall.Close(fd)

	// The buffer must be at least a block long.
	buf := make([]byte, blockSize) // stack-allocated; doesn't escape
	bufp := 0                      // starting read position in buf
	nbuf := 0                      // end valid data in buf
	skipFiles := false
	for {
		if bufp >= nbuf {
			bufp = 0
			nbuf, err = readDirent(fd, buf)
			if err != nil {
				return os.NewSyscallError("readdirent", err)
			}
			if nbuf <= 0 {
				return nil
			}
		}
		consumed, name, typ := dirent.Parse(buf[bufp:nbuf])
		bufp += consumed

		if name == "" || name == "." || name == ".." {
			continue
		}
		// Fallback for filesystems (like old XFS) that don't
		// support Dirent.Type and have DT_UNKNOWN (0) there
		// instead.
		if typ == unknownFileMode {
			fi, err := os.Lstat(dirName + "/" + name)
			if err != nil {
				// It got deleted in the meantime.
				if os.IsNotExist(err) {
					continue
				}
				return err
			}
			typ = fi.Mode() & os.ModeType
		}
		if skipFiles && typ.IsRegular() {
			continue
		}
		de := newUnixDirent(dirName, name, typ)
		if err := fn(dirName, name, de); err != nil {
			if err == ErrSkipFiles {
				skipFiles = true
				continue
			}
			return err
		}
	}
}

// According to https://golang.org/doc/go1.14#runtime
// A consequence of the implementation of preemption is that on Unix systems, including Linux and macOS
// systems, programs built with Go 1.14 will receive more signals than programs built with earlier releases.
//
// This causes syscall.Open and syscall.ReadDirent sometimes fail with EINTR errors.
// We need to retry in this case.
func open(path string, mode int, perm uint32) (fd int, err error) {
	for {
		fd, err := syscall.Open(path, mode, perm)
		if err != syscall.EINTR {
			return fd, err
		}
	}
}

func readDirent(fd int, buf []byte) (n int, err error) {
	for {
		nbuf, err := syscall.ReadDirent(fd, buf)
		if err != syscall.EINTR {
			return nbuf, err
		}
	}
}
