//go:build linux

package restartcpu

import (
	"math"
	"runtime"
	"sync"
	"testing"

	"github.com/prometheus/procfs"
	"golang.org/x/sys/unix"
)

// Burn a fixed amount of CPU on four other OS threads. A caller-thread clock
// or a wall-time/unit conversion cannot satisfy this independent procfs oracle.
func TestProcessCPUEqualsProcStatAcrossThreads(t *testing.T) {
	proc, err := procfs.Self()
	if err != nil {
		t.Fatal(err)
	}
	threadCPU := func() int64 {
		var ts unix.Timespec
		if err := unix.ClockGettime(unix.CLOCK_THREAD_CPUTIME_ID, &ts); err != nil {
			t.Error(err)
			return 0
		}
		return ts.Nano()
	}
	ready, start := make(chan struct{}, 4), make(chan struct{})
	var done sync.WaitGroup
	for range 4 {
		done.Go(func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			ready <- struct{}{}
			<-start
			at := threadCPU()
			var value uint64 = 1
			for threadCPU()-at < 75_000_000 {
				for range 4096 {
					value = value*6364136223846793005 + 1
				}
			}
			runtime.KeepAlive(value)
		})
	}
	for range 4 {
		<-ready
	}
	before, err := proc.Stat() // /proc/self/stat utime + stime, in USER_HZ ticks.
	if err != nil {
		close(start)
		done.Wait()
		t.Fatal(err)
	}
	used := CPU()
	close(start)
	done.Wait()
	used = CPU() - used
	after, err := proc.Stat()
	if err != nil {
		t.Fatal(err)
	}
	oracle := after.CPUTime() - before.CPUTime()
	if used < .28 || oracle < .28 || math.Abs(used-oracle) > max(.04, oracle*.05) {
		t.Fatalf("process CPU differs from procfs across four threads: CPU=%g proc_utime_stime=%g", used, oracle)
	}
	t.Logf("four other threads: CPU=%.6fs /proc/self/stat utime+stime=%.6fs", used, oracle)
}
