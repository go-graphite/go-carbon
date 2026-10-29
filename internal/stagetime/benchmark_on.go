//go:build phasecpubench

package stagetime

import (
	"sync/atomic"
	"time"
)

// A benchmark-only microsecond histogram avoids estimating p99 within the
// production histogram's doubling buckets. The final bucket is overflow.
var benchmarkUpdates [65536]atomic.Uint64

func observeBenchmarkUpdate(d time.Duration) {
	bucket := min(max(d.Microseconds(), 0), int64(len(benchmarkUpdates)-1))
	benchmarkUpdates[bucket].Add(1)
}

func BenchmarkUpdateHistogram() ([]uint64, uint64) {
	var count uint64
	out := make([]uint64, len(benchmarkUpdates))
	for i := range out {
		out[i] = benchmarkUpdates[i].Load()
		count += out[i]
	}
	return out, count
}
