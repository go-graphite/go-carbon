package v2daemon

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

// BenchmarkReceiverToOwner measures the producer path for receiver-shape input:
// one point per name per socket batch, 512 points per batch, 64k warm names.
// Each iteration admits 1M points through applicationOwner.Append, drains them
// into core owners and reports the resident heap per pending point.
func BenchmarkReceiverToOwner(b *testing.B) {
	const points1M, perPacket, nameCount = 1 << 20, 512, 1 << 16
	names := make([]string, nameCount)
	for i := range names {
		names[i] = fmt.Sprintf("bench.receiver.host%03d.metric%05d", i%997, i)
	}
	resolve := func(name string) ([]byte, error) {
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "bench", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
	}
	batchNames := make([]string, perPacket)
	batchData := make([]points.Point, perPacket)
	feed := func(o *applicationOwner, first, count int) error {
		for off := 0; off < count; off += perPacket {
			for j := range perPacket {
				k := first + off + j
				batchNames[j] = names[k%nameCount]
				batchData[j] = points.Point{Timestamp: int64(1_700_000_000 + k/nameCount), Value: float64(k)}
			}
			if err := o.Append(batchNames, nil, batchData, nil, int64(1_700_000_000+(first+off)/nameCount)); err != nil {
				return err
			}
		}
		return o.Drain()
	}
	var totalNS time.Duration
	var heap uint64
	var mallocs, bytes uint64
	for b.Loop() {
		b.StopTimer()
		s, err := Open(b.TempDir(), Config{Layer: 3, Producers: 1, Shards: 4, MaxPendingRows: 32768, MaxCoreRows: 1 << 21, GroupInterval: time.Hour, SealInterval: time.Hour, SealMaxRows: 1 << 22})
		if err != nil {
			b.Fatal(err)
		}
		o := &applicationOwner{p: s.Producers[0], resolve: resolve}
		if err = feed(o, 0, nameCount); err != nil { // warm: every name registered
			b.Fatal(err)
		}
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		b.StartTimer()
		start := time.Now()
		if err = feed(o, nameCount, points1M); err != nil {
			b.Fatal(err)
		}
		totalNS += time.Since(start)
		b.StopTimer()
		runtime.ReadMemStats(&after)
		mallocs += after.Mallocs - before.Mallocs
		bytes += after.TotalAlloc - before.TotalAlloc
		runtime.GC()
		runtime.ReadMemStats(&after)
		heap += after.HeapAlloc - before.HeapAlloc
		if err = s.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	n := float64(b.N) * points1M
	b.ReportMetric(float64(totalNS.Nanoseconds())/n, "ns/point")
	b.ReportMetric(float64(mallocs)/float64(b.N), "allocs/1Mpoints")
	b.ReportMetric(float64(bytes)/n, "allocB/point")
	b.ReportMetric(float64(heap)/n, "heapB/pending")
}
