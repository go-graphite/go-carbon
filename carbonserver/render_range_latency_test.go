package carbonserver

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"go.uber.org/zap"
)

// Real packed v9 blocks, eight-series protobuf renders and drifting 1s windows.
// The writeout mix changes a revision every 16 renders, as ingestion does even
// when it touches only recent data. Readers are closed-loop (no arrival queue).
// CPU includes those deterministic writes; p50/p99 time only the render.
func BenchmarkRenderRangeLatency(b *testing.B) {
	for _, mix := range []string{"moving", "writeout", "historical", "tailheavy"} {
		for _, readers := range []int{1, 8} {
			for _, enabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/readers%d/cache%v", mix, readers, enabled), func(b *testing.B) { benchmarkRenderRangeLatency(b, mix, readers, enabled) })
			}
		}
	}
}

// ABBA order controls temporal drift on a shared laptop. Five blocks provide
// ten independent samples of each mode; CPU and latency include the same mix.
func BenchmarkRenderRangeWriteoutParity(b *testing.B) {
	for pair := range 5 {
		for _, enabled := range []bool{false, true, true, false} {
			b.Run(fmt.Sprintf("pair%d/cache%v", pair, enabled), func(b *testing.B) { benchmarkRenderRangeLatency(b, "writeout", 1, enabled) })
		}
	}
}

func benchmarkRenderRangeLatency(b *testing.B, mix string, readers int, enabled bool) {
	l, db, _, first, count := newSealedRenderFixture(b)
	if err := l.SetQueryRangeCache(enabled, 64, 2*time.Hour); err != nil {
		b.Fatal(err)
	}
	if enabled {
		l.queryRangeCache.Load().now = func() time.Time { return time.Unix(1700100000, 0) }
	}
	render := func(i int) error {
		offset := i % 300
		until := first + count + offset
		if mix == "historical" {
			until = first + count/2 + offset
		}
		from := first + offset
		if mix == "tailheavy" {
			from, until = 1700100000-7500+offset, 1700100000-1
		}
		tr := timeRange{int32(from), int32(until)}
		targets := map[timeRange][]target{tr: {}}
		for m := range 8 {
			name := fmt.Sprintf("sealed.render.m%d", m)
			targets[tr] = append(targets[tr], target{name, name})
		}
		_, _, err := l.fetchWithCache(context.Background(), zap.NewNop(), protoV3Format, targets, &traceLogEntries{})
		return err
	}
	for i := range 32 {
		if err := render(i); err != nil {
			b.Fatal(err)
		}
	}
	runtime.GC()
	latencies := make([]int64, b.N)
	var next atomic.Int64
	var wg sync.WaitGroup
	cpu := processCPUTime(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range readers {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= b.N {
					return
				}
				if mix == "writeout" && i%16 == 0 {
					for m := range 8 {
						if err := db.UpdateMany(context.Background(), fmt.Sprintf("sealed.render.m%d", m), []store.Point{{Timestamp: 1700099990, Value: float64(i)}}); err != nil {
							b.Error(err)
							return
						}
					}
				}
				started := time.Now()
				if err := render(i); err != nil {
					b.Error(err)
					return
				}
				latencies[i] = time.Since(started).Nanoseconds()
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	cpu = processCPUTime(b) - cpu
	slices.Sort(latencies)
	b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
	b.ReportMetric(float64(latencies[len(latencies)/2]), "p50-ns")
	b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100]), "p99-ns")
}

// Count actual LSM point lookups and prefix walks, including validation, rather
// than counting Store.Fetch calls or in-memory continuation/state-cache hits.
func TestRenderRangeStorageReads(t *testing.T) {
	for _, memo := range []bool{true, false} {
		results := map[bool][]uint64{}
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("memo%v/cache%v", memo, enabled), func(t *testing.T) {
				l, db, _, first, count := newSealedRenderFixture(t)
				if !memo {
					restore := db.DisableMetadataCacheForTest()
					defer restore()
				}
				if err := l.SetQueryRangeCache(enabled, 64, 2*time.Hour); err != nil {
					t.Fatal(err)
				}
				if enabled {
					l.queryRangeCache.Load().now = func() time.Time { return time.Unix(1700100000, 0) }
				}
				fetch := func(from, until int) uint64 {
					done := store.CountStoreReadsForTest()
					defer done()
					for m := range 8 {
						if _, err := l.fetchMetricStoreSeries(context.Background(), db, fmt.Sprintf("sealed.render.m%d", m), from, until); err != nil {
							t.Fatal(err)
						}
					}
					return done()
				}
				counts := []uint64{fetch(first, first+count), fetch(first+1, first+count+1), fetch(first+2, first+count/2)}
				for m := range 8 {
					if err := db.UpdateMany(context.Background(), fmt.Sprintf("sealed.render.m%d", m), []store.Point{{Timestamp: 1700099990, Value: 9}}); err != nil {
						t.Fatal(err)
					}
				}
				counts = append(counts, fetch(first+3, first+count+3), fetch(first+4, first+count+4))
				results[enabled] = counts
				t.Logf("reads/render cold=%d moving_hit=%d historical_hit=%d revised=%d repeated=%d", counts[0], counts[1], counts[2], counts[3], counts[4])
			})
		}
		for i, label := range []string{"cold", "moving_hit", "historical_hit", "revised", "repeated"} {
			off, on := results[false][i], results[true][i]
			if on > off {
				t.Errorf("memo=%v %s adds reads: off=%d on=%d", memo, label, off, on)
			}
			if (i == 1 || i == 2) && on >= off {
				t.Errorf("memo=%v %s saved no reads: off=%d on=%d", memo, label, off, on)
			}
		}
	}
}
