//go:build linux

package v2dict

import "syscall"

func releaseLevelPages(m []byte) error { return syscall.Madvise(m, syscall.MADV_DONTNEED) }
