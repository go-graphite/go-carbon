//go:build darwin || linux

package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Seed each metric with the same physical v6 file, including real descriptors,
// encoded point groups and manifests. Relayout uses the production codec.
type relayoutBenchmarkCodec struct {
	ContinuationCodec
	keys []store.RelayoutKV
}

func (c *relayoutBenchmarkCodec) Create(_ context.Context, io store.ContinuationIO, _ store.Metadata, _ func() time.Time) error {
	for _, kv := range c.keys {
		if err := io.Set(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

// Run with -benchtime=1x. Setup/close are excluded. Each trial uses a fresh
// on-disk v6 store, upgraded to v7, with many independently planned metrics.
// CPU includes every process thread (also background flushes/compactions).
// GO_CARBON_RELAYOUT_BENCH_MEMORY=1 uses the same Pebble tables on a memory
// filesystem to isolate CPU scaling from the local device's write overhead.
func BenchmarkRelayoutWorkers(b *testing.B) {
	ctx := context.Background()
	now := time.Unix(1793200000, 0)
	clock := func() time.Time { return now }
	memory := os.Getenv("GO_CARBON_RELAYOUT_BENCH_MEMORY") == "1"
	metrics := 4096
	if value := os.Getenv("GO_CARBON_RELAYOUT_BENCH_METRICS"); value != "" {
		var err error
		metrics, err = strconv.Atoi(value)
		if err != nil || metrics < 32 {
			b.Fatal("GO_CARBON_RELAYOUT_BENCH_METRICS must be at least 32")
		}
	}
	path := filepath.Join(b.TempDir(), "seed.wsp")
	w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("10s:2d,60s:14d"), whisper.Average, .5,
		// Reserve enough compressed bytes per point to keep the small-block
		// geometry: an automatic extension would rebuild with large blocks.
		&whisper.Options{Compressed: true, PointsPerBlock: 512, PointSize: 12, Now: clock})
	if err != nil {
		b.Fatal(err)
	}
	for archive, step := range []int{10, 60} {
		random := uint64(19 + archive)
		points := make([]whisper.TimeSeriesPoint, 12288)
		input := make([]*whisper.TimeSeriesPoint, len(points))
		for i := range points {
			ts := (int(now.Unix())/step - len(points) + i) * step
			random ^= random << 13
			random ^= random >> 7
			random ^= random << 17
			points[i] = whisper.TimeSeriesPoint{Time: ts, Value: 1000 + math.Sin(float64(ts)/977)*100 + float64(random%1024)*1e-5}
			input[i] = &points[i]
		}
		if err := w.UpdateManyForArchive(input, w.Retentions()[archive].MaxRetention()); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	fixture := mapContinuationIO{}
	codec := &relayoutBenchmarkCodec{}
	config := store.MetricConfig{Retentions: []store.Retention{{Step: 10, Count: 17280}, {Step: 60, Count: 20160}}, AggregationMethod: store.Average, XFilesFactor: .5}
	if err := codec.Import(ctx, fixture, path, store.Metadata{MetricConfig: config}, clock); err != nil {
		b.Fatal(err)
	}
	if err := fixture.Visit(nil, func(k, v []byte) error {
		codec.keys = append(codec.keys, store.RelayoutKV{Key: k, Value: v})
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	plan, err := codec.RelayoutPlan(ctx, fixture)
	if err != nil || len(plan.Blocks) < 8 || plan.Bytes < 64<<10 {
		b.Fatalf("fixture too small: blocks=%d bytes=%d err=%v", len(plan.Blocks), plan.Bytes, err)
	}
	b.Logf("fixture: %d metrics, %d blocks and %d relayout bytes per metric; memory filesystem=%v", metrics, len(plan.Blocks), plan.Bytes, memory)
	cpu := func() float64 {
		var r syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
			b.Fatal(err)
		}
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("K=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			var wallSeconds, cpuSeconds float64
			for range b.N {
				dir := b.TempDir()
				opts := store.Options{ExactBlocks: true, Continuation: codec, RelayoutWorkers: workers,
					Now: clock, SyncInterval: time.Hour, CacheSize: 128 << 20, MemTableSize: 32 << 20,
					CompactionWorkers: 4, BytesPerSync: 64 << 20}
				if memory {
					opts = opts.WithFS(vfs.NewMem())
				}
				db, err := store.Open(dir, opts)
				if err != nil {
					b.Fatal(err)
				}
				for i := range metrics {
					config.Name = fmt.Sprintf("bench.relayout.%08d", i)
					if _, err := db.Create(ctx, config); err != nil {
						b.Fatal(err)
					}
				}
				if err := db.Compact(); err != nil {
					b.Fatal(err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				opts.TimeBuckets = true
				db, err = store.Open(dir, opts)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				startCPU, start := cpu(), time.Now()
				err = db.RelayoutContinuation(ctx, 0)
				wallSeconds += time.Since(start).Seconds()
				cpuSeconds += cpu() - startCPU
				b.StopTimer()
				st := db.RelayoutStats()
				if err != nil || !st.Done || st.Metrics != uint64(metrics) || st.Blocks != uint64(metrics*len(plan.Blocks)) || st.Bytes != uint64(metrics*plan.Bytes) || st.Skipped != 0 {
					b.Fatalf("relayout: %+v %v", st, err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				if err := os.RemoveAll(dir); err != nil {
					b.Fatal(err)
				}
			}
			bytes := float64(b.N * metrics * plan.Bytes)
			b.SetBytes(int64(metrics * plan.Bytes))
			b.ReportMetric(float64(b.N*metrics)/wallSeconds, "metrics/s")
			b.ReportMetric(cpuSeconds*1e9/bytes, "cpu-ns/B")
			b.ReportMetric(cpuSeconds/wallSeconds, "cores")
			b.ReportMetric(float64(plan.Bytes), "B/metric")
			b.ReportMetric(float64(len(plan.Blocks)), "blocks/metric")
		})
	}
}
