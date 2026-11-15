package carbon

import (
	"math/bits"
	"os"
	"sync/atomic"
	"time"
)

const (
	captureEncode = iota
	captureWrite
	captureSync
	captureRotate
	captureAppend
	captureIOCount
)

var captureOperations = [captureIOCount]string{"encode", "write", "sync", "rotate", "append"}

type captureTiming struct {
	calls, total, maximum atomic.Int64
	buckets               [64]atomic.Int64
}
type captureTimingValue struct{ calls, total, maximum, p99 int64 }

func (t *captureTiming) observe(start time.Time) {
	t.observeDuration(time.Since(start))
}
func (t *captureTiming) observeDuration(duration time.Duration) {
	elapsed := duration.Nanoseconds()
	t.calls.Add(1)
	t.total.Add(elapsed)
	t.buckets[bits.Len64(uint64(max(elapsed, 0)))].Add(1)
	for old := t.maximum.Load(); elapsed > old; old = t.maximum.Load() {
		if t.maximum.CompareAndSwap(old, elapsed) {
			break
		}
	}
}
func (s *startupShard) syncFile(file *os.File) error {
	started := time.Now()
	err := file.Sync()
	s.timings[captureSync].observe(started)
	return err
}
func (s *startupInput) ioTimings() (out [captureIOCount]captureTimingValue) {
	var buckets [captureIOCount][64]int64
	add := func(i int, timing *captureTiming) {
		out[i].calls += timing.calls.Load()
		out[i].total += timing.total.Load()
		out[i].maximum = max(out[i].maximum, timing.maximum.Load())
		for b := range buckets[i] {
			buckets[i][b] += timing.buckets[b].Load()
		}
	}
	for _, sh := range s.shards {
		for i := range out {
			add(i, &sh.timings[i])
		}
	}
	if s.journal != nil {
		add(captureAppend, &s.journal.appendTiming)
	}
	for i := range out {
		var count int64
		for _, n := range buckets[i] {
			count += n
		}
		target, cumulative := (count*99+99)/100, int64(0)
		for b, n := range buckets[i] {
			cumulative += n
			if target > 0 && cumulative >= target {
				out[i].p99 = int64((uint64(1) << b) - 1)
				break
			}
		}
	}
	return out
}
