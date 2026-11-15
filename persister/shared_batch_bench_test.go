//go:build unix

package persister

import (
	"fmt"
	"syscall"
	"testing"
)

// Drive the worker's batch path: cache pop, continuation codec, shared commit,
// cache confirmation and timing, with the same reusable timer as a worker.
func BenchmarkSharedBatchTimings(b *testing.B) {
	for _, count := range []int{32, 210} {
		b.Run(fmt.Sprintf("points=%d", count), func(b *testing.B) {
			update := sharedTimingWorkload(b, count)
			for i := range 16 {
				update(i)
			}
			var before, after syscall.Rusage
			syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				update(i + 16)
			}
			b.StopTimer()
			syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			cpu := func(u syscall.Rusage) float64 {
				return float64(u.Utime.Sec+u.Stime.Sec)*1e6 + float64(u.Utime.Usec+u.Stime.Usec)
			}
			b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-us/batch")
			b.ReportMetric((cpu(after)-cpu(before))/float64(b.N*8*count), "CPU-s/Mpoints")
		})
	}
}
