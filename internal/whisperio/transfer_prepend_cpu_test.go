//go:build darwin || linux

package whisperio

import "syscall"

func prependProcessCPU() int64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	return u.Utime.Nano() + u.Stime.Nano()
}
