package whisperio

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/klauspost/compress/zstd"
)

// Production-shaped archive selection and native block creation, followed by
// the real v9 cold packer. Values are deterministic, not a production capture.
func rangeFetchFixture(tb testing.TB) (*store.Store, *ContinuationCodec, int) {
	tb.Helper()
	ctx := context.Background()
	const now = 1793664000
	codec := &ContinuationCodec{}
	db, err := store.Open(filepath.Join(tb.TempDir(), "store"), store.Options{
		ExactBlocks: true, TimeBuckets: true, Continuation: codec,
		Now: func() time.Time { return time.Unix(now, 0) }, CacheSize: 8 << 20,
		MemTableSize: 2 << 20, MetadataCacheSize: 2 << 20, FetchSharedIterator: true,
	})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	db.StopBackground()
	cfg := store.MetricConfig{Name: "range.metric", Retentions: []store.Retention{{Step: 1, Count: 2 * 86400}, {Step: 60, Count: 28 * 1440}, {Step: 3600, Count: 2 * 8760}}, AggregationMethod: store.Average, XFilesFactor: .5}
	if _, err := db.Create(ctx, cfg); err != nil {
		tb.Fatal(err)
	}
	// Fill old archives directly, then fine points; all native headers and
	// rollups are produced by the existing writer rather than handmade metadata.
	for _, history := range []struct{ step, age, recent int }{{3600, 700 * 86400, 28 * 86400}, {60, 28 * 86400, 2 * 86400}, {1, 2*86400 - 60, 60}} {
		input := make([]store.Point, 0, (history.age-history.recent)/history.step)
		for ts := now - history.age; ts < now-history.recent; ts += history.step {
			value := 1000 + math.Sin(float64(ts)/7919)*23 + float64(ts%97)/100
			input = append(input, store.Point{Timestamp: int64(ts), Value: value})
		}
		if err := db.UpdateMany(ctx, cfg.Name, input); err != nil {
			tb.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		tb.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		tb.Fatal(err)
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: time.Second, CPUFraction: 1, BytesPerSecond: 1 << 40, MaxCompactionDebt: 1 << 40}); err != nil {
		tb.Fatal(err)
	}
	for n := 0; ; n++ {
		done, err := db.ColdPackStep(ctx)
		if err != nil {
			tb.Fatal(err)
		}
		if done {
			break
		}
		if n > 10000 {
			tb.Fatalf("cold pack did not finish: %+v", db.ColdPackStats())
		}
	}
	if db.ColdPackStats().Blocks == 0 {
		tb.Fatal("fixture has no v9 blocks")
	}
	if err := db.Flush(); err != nil {
		tb.Fatal(err)
	}
	tb.Logf("v9 blocks=%d points=%d bytes=%d", db.ColdPackStats().Blocks, db.ColdPackStats().Points, db.ColdPackStats().OutputBytes)
	return db, codec, now
}

func BenchmarkContinuationRangeFetch(b *testing.B) {
	db, codec, now := rangeFetchFixture(b)
	for _, hours := range []int{1, 6, 24, 7 * 24} {
		for _, enabled := range []bool{false, true, true, false} {
			b.Run(fmt.Sprintf("%dh/range=%v", hours, enabled), func(b *testing.B) {
				codec.RangeReadsEnabled = func() bool { return enabled }
				b.ReportAllocs()
				cpu := rangeCPUTime()
				for b.Loop() {
					result, err := db.Fetch(context.Background(), "range.metric", now-120-hours*3600, now-120)
					if err != nil || result == nil {
						b.Fatalf("Fetch: %v", err)
					}
				}
				b.ReportMetric(float64(rangeCPUTime()-cpu)/float64(b.N), "cpu-ns/op")
			})
		}
	}
}

// Process CPU includes allocation and GC work; ns/op is elapsed storage-fetch
// latency. The process is constrained to two Go CPUs on this shared machine.
func rangeCPUTime() int64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return usage.Utime.Nano() + usage.Stime.Nano()
}

func BenchmarkContinuationRangeBlock(b *testing.B) {
	for _, count := range []int{7200, 65536} {
		f, block, kv, _ := rangeBlockFixture(b, count, "decimal", true)
		desc, _ := f.descriptor(block.Archive, block.Index)
		for _, hours := range []int{1, 6, 24, 168} {
			// A fixed historical window with a partly filled final packed group.
			until := block.Last - 120
			from := until - hours*3600
			for _, enabled := range []bool{false, true, true, false} {
				b.Run(fmt.Sprintf("points=%d/%dh/range=%v", count, hours, enabled), func(b *testing.B) {
					f.fs.rangeReads = enabled
					decodedPoints, encodedBytes, bodyBytes, visitedBytes := 0, 0, 0, 0
					for i := 0; i < (count+4095)/4096; i++ {
						value, _ := kv.Get(desc.groupKey(42, 0, 1, i))
						visitedBytes += len(value)
						lo, hi, n, err := store.SealedTimeBounds(value)
						if err != nil {
							b.Fatal(err)
						}
						if !enabled || hi >= int64(from) && lo <= int64(until) {
							decodedPoints += n
							encodedBytes += len(value)
							expanded := len(value) - 16
							if value[1]&8 != 0 {
								var header zstd.Header
								if err := header.Decode(value[16:]); err != nil || !header.HasFCS {
									b.Fatal("fixture needs known decompressed size", err)
								}
								expanded = int(header.FrameContentSize)
							}
							bodyBytes += expanded
						}
					}
					b.ReportAllocs()
					cpu := rangeCPUTime()
					for b.Loop() {
						if _, err := f.ReadStorageBlockRange(block, from, until); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(rangeCPUTime()-cpu)/float64(b.N), "cpu-ns/op")
					b.ReportMetric(float64(decodedPoints), "decoded-pt/op")
					b.ReportMetric(float64(decodedPoints*16), "decoded-B/op")
					b.ReportMetric(float64(encodedBytes), "codec-input-B/op")
					b.ReportMetric(float64(bodyBytes), "codec-body-B/op")
					b.ReportMetric(float64(visitedBytes), "visited-value-B/op")
				})
			}
		}
	}
}
