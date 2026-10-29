//go:build !linux

package inputlog

import "os"

func preallocate(f *os.File, size int64) error { return f.Truncate(size) }

func datasync(f *os.File) error { return f.Sync() }

func dropCache(*os.File, int64, int64) {}
