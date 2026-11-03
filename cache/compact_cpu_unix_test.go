//go:build linux || darwin

package cache

import "syscall"

// Process CPU includes background GC but excludes time descheduled by other
// work on a shared benchmark host. Sampling outside the loop adds no per-point
// overhead and makes the cores-at-a-given-ingest-rate estimate meaningful.
func compactCPUTime() int64 {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return (int64(usage.Utime.Sec)+int64(usage.Stime.Sec))*1e9 + (int64(usage.Utime.Usec)+int64(usage.Stime.Usec))*1e3
}
