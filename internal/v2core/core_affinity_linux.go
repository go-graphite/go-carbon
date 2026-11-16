//go:build linux

package v2core

import (
	"golang.org/x/sys/unix"
	"runtime"
)

func pinCoreThread(cpu int) (func(), error) {
	runtime.LockOSThread()
	var prior unix.CPUSet
	if err := unix.SchedGetaffinity(0, &prior); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	var mask unix.CPUSet
	mask.Set(cpu)
	if err := unix.SchedSetaffinity(0, &mask); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() { _ = unix.SchedSetaffinity(0, &prior); runtime.UnlockOSThread() }, nil
}
