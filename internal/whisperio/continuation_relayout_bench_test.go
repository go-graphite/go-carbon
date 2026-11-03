//go:build darwin || linux

package whisperio

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// Seed metrics from physical v6 files, including real descriptors,
// encoded point groups and manifests. Relayout uses the production codec.
type relayoutBenchmarkCodec struct {
	ContinuationCodec
	keys         []store.RelayoutKV
	lightKeys    []store.RelayoutKV
	heavyMetrics int
}

func (c *relayoutBenchmarkCodec) Create(_ context.Context, io store.ContinuationIO, m store.Metadata, _ func() time.Time) error {
	keys := c.keys
	if c.lightKeys != nil {
		var index int
		if _, err := fmt.Sscanf(m.Name, "bench.relayout.%d", &index); err != nil {
			return err
		}
		if index >= c.heavyMetrics {
			keys = c.lightKeys
		}
	}
	for _, kv := range keys {
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
// GO_CARBON_RELAYOUT_BENCH_SKEW=1 concentrates about 80% of history at the
// lowest 10% of IDs, rather than using identical files for every metric.
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
	retentions := "10s:2d,60s:14d"
	config := store.MetricConfig{Retentions: []store.Retention{{Step: 10, Count: 17280}, {Step: 60, Count: 20160}}, AggregationMethod: store.Average, XFilesFactor: .5}
	skew := os.Getenv("GO_CARBON_RELAYOUT_BENCH_SKEW") == "1"
	if skew {
		retentions = "10s:7d,60s:60d"
		config.Retentions = []store.Retention{{Step: 10, Count: 60480}, {Step: 60, Count: 86400}}
	}
	makeFixture := func(pointsPerArchive int) ([]store.RelayoutKV, store.RelayoutPlan) {
		path := filepath.Join(b.TempDir(), "seed.wsp")
		w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(retentions), whisper.Average, .5,
			// Reserve enough compressed bytes per point to keep the small-block
			// geometry: an automatic extension would rebuild with large blocks.
			&whisper.Options{Compressed: true, PointsPerBlock: 512, PointSize: 12, Now: clock})
		if err != nil {
			b.Fatal(err)
		}
		for archive, step := range []int{10, 60} {
			random := uint64(19 + archive)
			points := make([]whisper.TimeSeriesPoint, pointsPerArchive)
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
		codec := &ContinuationCodec{}
		if err := codec.Import(ctx, fixture, path, store.Metadata{MetricConfig: config}, clock); err != nil {
			b.Fatal(err)
		}
		var keys []store.RelayoutKV
		if err := fixture.Visit(nil, func(k, v []byte) error {
			keys = append(keys, store.RelayoutKV{Key: k, Value: v})
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		plan, err := codec.RelayoutPlan(ctx, fixture)
		if err != nil || len(plan.Blocks) < 1 || plan.Bytes < 1 {
			b.Fatalf("fixture too small: blocks=%d bytes=%d err=%v", len(plan.Blocks), plan.Bytes, err)
		}

		return keys, plan
	}
	pointsPerArchive := 12288
	if skew {
		pointsPerArchive = 28672
	}
	keys, plan := makeFixture(pointsPerArchive)
	if len(plan.Blocks) < 8 || plan.Bytes < 64<<10 {
		b.Fatalf("heavy fixture too small: blocks=%d bytes=%d", len(plan.Blocks), plan.Bytes)
	}
	codec := &relayoutBenchmarkCodec{keys: keys, heavyMetrics: metrics}
	totalBytes, totalBlocks := metrics*plan.Bytes, metrics*len(plan.Blocks)
	if skew {
		lightKeys, lightPlan := makeFixture(1024)
		codec.lightKeys = lightKeys
		codec.heavyMetrics = metrics / 10
		totalBytes = codec.heavyMetrics*plan.Bytes + (metrics-codec.heavyMetrics)*lightPlan.Bytes
		totalBlocks = codec.heavyMetrics*len(plan.Blocks) + (metrics-codec.heavyMetrics)*len(lightPlan.Blocks)
		b.Logf("skew: low %d IDs: %d blocks/%d B each; remaining %d IDs: %d blocks/%d B each; heavy shares blocks=%.1f%% bytes=%.1f%%", codec.heavyMetrics, len(plan.Blocks), plan.Bytes, metrics-codec.heavyMetrics, len(lightPlan.Blocks), lightPlan.Bytes, 100*float64(codec.heavyMetrics*len(plan.Blocks))/float64(totalBlocks), 100*float64(codec.heavyMetrics*plan.Bytes)/float64(totalBytes))
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
			var wallSeconds, cpuSeconds, workerSeconds, soloSeconds float64
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
				stopOccupancy := sampleRelayoutWorkers()
				b.StartTimer()
				startCPU, start := cpu(), time.Now()
				err = db.RelayoutContinuation(ctx, 0)
				wallSeconds += time.Since(start).Seconds()
				cpuSeconds += cpu() - startCPU
				b.StopTimer()
				active, solo := stopOccupancy()
				workerSeconds += active
				soloSeconds += solo
				st := db.RelayoutStats()
				if err != nil || !st.Done || st.Metrics != uint64(metrics) || st.Blocks != uint64(totalBlocks) || st.Bytes != uint64(totalBytes) || st.Skipped != 0 {
					b.Fatalf("relayout: %+v %v", st, err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				if err := os.RemoveAll(dir); err != nil {
					b.Fatal(err)
				}
			}
			bytes := float64(b.N * totalBytes)
			b.SetBytes(int64(totalBytes))
			b.ReportMetric(float64(b.N*metrics)/wallSeconds, "metrics/s")
			b.ReportMetric(cpuSeconds*1e9/bytes, "cpu-ns/B")
			b.ReportMetric(cpuSeconds/wallSeconds, "cores")
			b.ReportMetric(float64(totalBytes)/float64(metrics), "B/metric")
			b.ReportMetric(workerSeconds/wallSeconds, "active-workers")
			b.ReportMetric(100*workerSeconds/(wallSeconds*float64(workers)), "worker-%")
			b.ReportMetric(100*soloSeconds/wallSeconds, "solo-%")
			b.ReportMetric(wallSeconds/float64(b.N), "wall-s")
			b.ReportMetric(cpuSeconds/float64(b.N), "cpu-s")
			b.ReportMetric(float64(totalBlocks)/float64(metrics), "blocks/metric")
		})
	}
}

// Sample workers inside planning, ingestion or apply (including lock/IO waits).
// This is worker occupancy, separate from process CPU. Sample every 5 ms; use
// the same sampler on both schedulers. No production instrumentation is needed.
func sampleRelayoutWorkers() func() (float64, float64) {
	stop, done := make(chan struct{}), make(chan struct{})
	var activeSeconds, soloSeconds float64
	go func() {
		defer close(done)
		records := make([]runtime.StackRecord, 256)
		previous := time.Now()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				now := time.Now()
				elapsed := now.Sub(previous).Seconds()
				previous = now
				n, ok := runtime.GoroutineProfile(records)
				if !ok {
					records = make([]runtime.StackRecord, n+64)
					continue
				}
				active := 0
				for _, record := range records[:n] {
					frames := runtime.CallersFrames(record.Stack())
					for {
						frame, more := frames.Next()
						if strings.HasSuffix(frame.Function, "chunkstore.(*Store).relayoutRange") {
							active++
							break
						}
						if !more {
							break
						}
					}
				}
				activeSeconds += float64(active) * elapsed
				if active == 1 {
					soloSeconds += elapsed
				}
			}
		}
	}()
	return func() (float64, float64) { close(stop); <-done; return activeSeconds, soloSeconds }
}
