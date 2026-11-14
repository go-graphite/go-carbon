package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Classic archives use a minutely primary and a thirty-minute rollup.
// A small UpdateMany can still rewrite the same rollup group many times.
func smallBatchClassicSeed(t testing.TB, method store.AggregationMethod, xff float32) (store.Metadata, mapContinuationIO, int64) {
	t.Helper()
	const base = int64(1794000000)
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "classic.small", Retentions: []store.Retention{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: method, XFilesFactor: xff}}
	kv := mapContinuationIO{}
	clock := func() time.Time { return time.Unix(base, 0) }
	fs := newContinuationFiles(kv, false).withClock(clock)
	options := continuationOptions(fs, clock)
	options.Compressed = false
	w, err := whisper.CreateWithOptions(fs.main(), whisper.MustParseRetentionDefs("1m:14d,30m:2y"), whisper.AggregationMethod(method), xff, options)
	if err != nil {
		t.Fatal(err)
	}
	var history []*whisper.TimeSeriesPoint
	for ts := base - 2*86400; ts < base; ts += 60 {
		history = append(history, &whisper.TimeSeriesPoint{Time: int(ts), Value: float64(ts%997) + .125})
	}
	if err := errors.Join(w.UpdateMany(history), w.Close(), fs.flush()); err != nil {
		t.Fatal(err)
	}
	return m, kv, base
}

func TestContinuationSmallBatchCoalescing(t *testing.T) {
	for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.Min, store.Max, store.Last, store.First} {
		for _, xff := range []float32{0, .5, 1} {
			for _, reuse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/%g/reuse=%t", method, xff, reuse), func(t *testing.T) {
					m, seed, base := smallBatchClassicSeed(t, method, xff)
					plain, buffered := copyIO(seed), copyIO(seed)
					codec := &ContinuationCodec{ReuseBlockKeys: reuse}
					bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000012, 0x3ff0000000000001}
					for round := range 20 {
						count := []int{2, 9, 34, 54, 78}[round%5]
						base += int64(count) * 60
						input := make([]store.Point, count)
						for j := range input {
							input[j] = store.Point{Timestamp: base - int64(count-j)*60, Value: math.Float64frombits(bits[(round+j)%len(bits)])}
						}
						if round%3 == 1 {
							input = append(input, store.Point{Timestamp: base - 5401, Value: .125}, input[0])
						}
						clock := func() time.Time { return time.Unix(base+1, 0) }
						old := coalescePointWrites
						func() {
							defer func() { coalescePointWrites = old }()
							coalescePointWrites = false
							if err := codec.Update(context.Background(), plain, m, input, -1, clock, nil); err != nil {
								t.Fatal(err)
							}
						}()
						bufferedSets := buffered.sets
						if err := codec.Update(context.Background(), buffered, m, input, -1, clock, nil); err != nil {
							t.Fatal(err)
						}
						if err := sameKV(plain.mapContinuationIO, buffered.mapContinuationIO); err != nil {
							t.Fatalf("round %d: %v", round, err)
						}
						// Below the old 2*rollup-ratio threshold (60 input points),
						// 34/54-point updates must still combine repeated group writes.
						if round%3 != 1 && (count == 34 || count == 54) {
							if buffered.sets-bufferedSets > 8 {
								t.Fatalf("%d input points issued %d sets; repeated groups were not coalesced", count, buffered.sets-bufferedSets)
							}
						}
					}
				})
			}
		}
	}
}

func BenchmarkClassicSmallBatch(b *testing.B) {
	for _, cadence := range []int64{6, 60} {
		for _, batch := range []int{34, 54, 78} {
			b.Run(fmt.Sprintf("cadence=%d/batch=%d", cadence, batch), func(b *testing.B) {
				m, seed, base := smallBatchClassicSeed(b, store.Average, .5)
				now := time.Unix(base, 0)
				db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true}, TimeBuckets: true,
					Now: func() time.Time { return now }, SyncInterval: time.Hour, MetadataCacheSize: 32 << 20, CacheSize: 128 << 10, MemTableSize: 4 << 20, BloomFilterBits: 10})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				db.StopBackground()
				var keys []store.RelayoutKV
				for key, value := range seed {
					keys = append(keys, store.RelayoutKV{Key: []byte(key), Value: value})
				}
				slices.SortFunc(keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
				const metrics = 32
				var names [metrics]string
				ctx := context.Background()
				for i := range names {
					cfg := m.MetricConfig
					cfg.Name = fmt.Sprintf("classic.small.%d", i)
					if _, err := db.ImportTransfer(ctx, []store.TransferMetric{{Config: cfg, Version: store.TransferVersion{Name: cfg.Name}, Continuation: true, Keys: keys}}); err != nil {
						b.Fatal(err)
					}
					names[i] = cfg.Name
				}
				if err := errors.Join(db.Flush(), db.Compact()); err != nil {
					b.Fatal(err)
				}
				input := make([]store.Point, batch)
				var before, after syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &before)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					end := base + int64(i/metrics+1)*int64(batch)*cadence
					now = time.Unix(end+1, 0)
					for j := range input {
						input[j] = store.Point{Timestamp: end - int64(batch-j)*cadence, Value: float64(i+j) + .125}
					}
					if err := db.UpdateMany(ctx, names[i%metrics], input); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				syscall.Getrusage(syscall.RUSAGE_SELF, &after)
				cpu := func(r syscall.Rusage) float64 {
					return float64(r.Utime.Sec+r.Stime.Sec)*1e6 + float64(r.Utime.Usec+r.Stime.Usec)
				}
				b.ReportMetric(float64(batch), "points/update")
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "updates/s")
				b.ReportMetric(float64(b.N*batch)*1e6/(cpu(after)-cpu(before)), "points/s/core")
			})
		}
	}
}
