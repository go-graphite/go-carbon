package recovery

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Opt-in: GO_CARBON_SINK_DIR=/var/lib/carbon/dump measures the sink on a real
// disk with 1, 2, 4 and 8 writers (GO_CARBON_SINK_GB, default 4).
func TestSinkHostThroughput(t *testing.T) {
	dir := os.Getenv("GO_CARBON_SINK_DIR")
	if dir == "" {
		t.Skip("set GO_CARBON_SINK_DIR")
	}
	gb := 4
	if v, err := strconv.Atoi(os.Getenv("GO_CARBON_SINK_GB")); err == nil && v > 0 {
		gb = v
	}
	chunk := make([]byte, segmentChunk+1234)
	for i := range chunk {
		chunk[i] = byte(i * 7)
	}
	saved := sinkWriters
	defer func() { sinkWriters = saved }()
	for _, writers := range []int{1, 2, 4, 8} {
		sinkWriters = writers
		path := filepath.Join(dir, ".sink-host-test.bin")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		s := newSink(f, 1<<20)
		start := time.Now()
		for n := 0; n < gb<<30; n += len(chunk) {
			if err = s.Submit(chunk, nil); err != nil {
				t.Fatal(err)
			}
		}
		err = s.Flush()
		flushed := time.Since(start)
		syncStart := time.Now()
		err = errors.Join(err, f.Sync(), f.Close(), os.Remove(path))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("writers=%d write %.2fs (%.2f GB/s) fsync %.2fs", writers, flushed.Seconds(), float64(gb)/flushed.Seconds(), time.Since(syncStart).Seconds())
		time.Sleep(3 * time.Second)
	}
}
