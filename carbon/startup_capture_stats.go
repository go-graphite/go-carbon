package carbon

import (
	"os"
	"sync/atomic"
	"time"
)

const (
	captureEncode = iota
	captureWrite
	captureSync
	captureRotate
	captureIOCount
)

var captureOperations = [captureIOCount]string{"encode", "write", "sync", "rotate"}

type captureTiming struct{ calls, total, maximum atomic.Int64 }
type captureTimingValue struct{ calls, total, maximum int64 }

func (t *captureTiming) observe(start time.Time) {
	elapsed := time.Since(start).Nanoseconds()
	t.calls.Add(1)
	t.total.Add(elapsed)
	for old := t.maximum.Load(); elapsed > old; old = t.maximum.Load() {
		if t.maximum.CompareAndSwap(old, elapsed) {
			break
		}
	}
}
func (s *startupShard) syncFile(file *os.File) error {
	started := time.Now()
	var err error
	if sync := s.owner.fileSync; sync != nil {
		err = sync(file)
	} else {
		err = file.Sync()
	}
	s.timings[captureSync].observe(started)
	return err
}
func (s *startupInput) ioTimings() (out [captureIOCount]captureTimingValue) {
	for _, sh := range s.shards {
		for i := range out {
			out[i].calls += sh.timings[i].calls.Load()
			out[i].total += sh.timings[i].total.Load()
			out[i].maximum = max(out[i].maximum, sh.timings[i].maximum.Load())
		}
	}
	return out
}
