package restartcpu

import "golang.org/x/sys/unix"

// CPU measures all threads in this process, in seconds. The process clock has
// nanosecond units; it is not the calling thread's clock or elapsed wall time.
func CPU() float64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &ts); err != nil {
		return rusageCPU()
	}
	return float64(ts.Sec) + float64(ts.Nsec)/1e9
}
