//go:build !linux

package handoff

import (
	"errors"
	"syscall"
)

func kernelUnread(syscall.RawConn) (int, error) { return -1, errors.ErrUnsupported }
