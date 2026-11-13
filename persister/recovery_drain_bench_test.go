package persister

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

// Disk-backed CWhisper history plus a restored cache, drained by 96 real
// persister calls. Each trial reopens the immutable seed with cold read caches.
// Cache sizes scale down the production data/cache ratio; this is synthetic,
// not a substitute for a native canary on a production-size catalog.
func BenchmarkRecoveryDrain(b *testing.B) {
	if os.Getenv("GO_CARBON_BENCH_RECOVERY_DRAIN") != "1" {
		b.Skip("opt-in disk benchmark")
	}
	const metrics, history, restored = 128, 8192, 2048
	const now = int64(1794596400)
	ctx := context.Background()
	storageBenchmarkLogs(b)
	root := b.TempDir()
	opts := store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true},
		Now: func() time.Time { return time.Unix(now, 0) }, SyncInterval: time.Hour, CacheSize: 8 << 20, MetadataCacheSize: 8 << 20,
		MemTableSize: 8 << 20, BloomFilterBits: 10, CompactionWorkers: 16, L0CompactionConcurrency: 8}
	names := make([]string, metrics)
	seed, err := store.Open(filepath.Join(root, "seed"), opts)
	if err != nil {
		b.Fatal(err)
	}
	seed.StopBackground()
	input := make([]store.Point, history)
	for i := range input {
		input[i] = store.Point{Timestamp: now - int64(history) + int64(i), Value: math.Sin(float64(i)/97)*100 + float64(i%31)}
	}
	for i := range names {
		names[i] = fmt.Sprintf("recovery.synthetic.metric.%08x", uint32(i)*2654435761)
		_, err = seed.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}, {Step: 3600, Count: 17520}}, AggregationMethod: store.Average, XFilesFactor: .5})
		if err != nil {
			b.Fatal(err)
		}
		if err = seed.UpdateMany(ctx, names[i], input); err != nil {
			b.Fatal(err)
		}
	}
	if err = seed.Flush(); err != nil {
		b.Fatal(err)
	}
	if err = seed.Compact(); err != nil {
		b.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		b.Fatal(err)
	}
	// The empty-history trial exercises the existing direct block builder.
	emptySeed, err := store.Open(filepath.Join(root, "empty"), opts)
	if err != nil {
		b.Fatal(err)
	}
	emptySeed.StopBackground()
	for _, name := range names {
		if _, err = emptySeed.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}, {Step: 3600, Count: 17520}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			b.Fatal(err)
		}
	}
	if err = emptySeed.Flush(); err != nil {
		b.Fatal(err)
	}
	if err = emptySeed.Close(); err != nil {
		b.Fatal(err)
	}
	restore := append([]store.Point(nil), input[history-restored:]...)
	// Corrections, duplicate timestamps, and a new suffix exercise real updates.
	for i := range restore {
		restore[i].Value += .125
		restore[i].Timestamp += 60
	}
	for _, trial := range []struct {
		name     string
		cache    int64
		lookups  bool
		fragment int
		shards   int
		empty    bool
	}{
		{name: "baseline", cache: 64 << 10, fragment: restored},
		{name: "oversharded", cache: 1 << 20, fragment: restored, shards: 64},
		{name: "cache-fit", cache: 1 << 20, fragment: restored, shards: 4},
		{name: "large-shards-old", cache: 16 << 20, fragment: restored, shards: 768},
		{name: "large-shards-new", cache: 16 << 20, fragment: restored, shards: 356},
		{name: "cache1MiB", cache: 1 << 20, fragment: restored},
		{name: "cache8MiB", cache: 8 << 20, fragment: restored},
		{name: "point-lookups", cache: 64 << 10, fragment: restored, lookups: true},
		{name: "fragments32", cache: 64 << 10, fragment: 32},
		{name: "empty-history", cache: 64 << 10, fragment: restored, empty: true},
	} {
		b.Run(trial.name, func(b *testing.B) {
			b.ReportAllocs()
			var totalElapsed time.Duration
			var totalCPU float64
			var misses uint64
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				path := filepath.Join(b.TempDir(), "store")
				seedName := "seed"
				if trial.empty {
					seedName = "empty"
				}
				if err = copyDrainFixture(filepath.Join(root, seedName), path); err != nil {
					b.Fatal(err)
				}
				trialOpts := opts
				trialOpts.CacheSize = trial.cache
				trialOpts.CacheShards = trial.shards
				trialOpts.UpdatePointLookups = trial.lookups
				db, e := store.Open(path, trialOpts)
				if e != nil {
					b.Fatal(e)
				}
				db.StopBackground()
				c := cache.New()
				c.SetMaxSize(10000000)
				c.SetRecoveryDrain(true)
				p := newOOOTestPersister(b, filepath.Join(path, "originals"), &fakeCache{})
				p.SetMetricStore(db)
				p.pop = c.PopNotConfirmed
				p.confirm = c.Confirm
				p.requeue = c.Requeue
				// Keep the pre-update ownership lookup, as an unfinished incremental scan does.
				p.incrementalStore = db
				var next atomic.Int64
				for _, name := range names {
					c.Add(&points.Points{Metric: name, Data: restore[:trial.fragment]})
				}
				before := db.Stats()
				var cpuBefore, cpuAfter syscall.Rusage
				_ = syscall.Getrusage(syscall.RUSAGE_SELF, &cpuBefore)
				start := time.Now()
				b.StartTimer()
				for offset := 0; offset < restored; offset += trial.fragment {
					if offset > 0 {
						for _, name := range names {
							c.Add(&points.Points{Metric: name, Data: restore[offset : offset+trial.fragment]})
						}
					}
					next.Store(-1)
					var wg sync.WaitGroup
					for range 96 {
						wg.Go(func() {
							for {
								i := int(next.Add(1))
								if i >= metrics {
									return
								}
								p.store(names[i])
							}
						})
					}
					wg.Wait()
				}
				b.StopTimer()
				elapsed := time.Since(start)
				_ = syscall.Getrusage(syscall.RUSAGE_SELF, &cpuAfter)
				after := db.Stats()
				cpuSeconds := func(r syscall.Rusage) float64 {
					return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
				}
				totalCPU += cpuSeconds(cpuAfter) - cpuSeconds(cpuBefore)
				totalElapsed += elapsed
				misses += after.CacheMisses - before.CacheMisses
				if !c.IsEmpty() {
					b.Fatal("drain left points cached")
				}
				if e = db.Close(); e != nil {
					b.Fatal(e)
				}
			}
			b.ReportMetric(1e9*totalCPU/float64(b.N*metrics*restored), "cpu-ns/point")
			b.ReportMetric(float64(b.N*metrics*restored)/totalElapsed.Seconds(), "points/s")
			b.ReportMetric(float64(misses)/float64(b.N*metrics), "block-misses/metric")
		})
	}
}

func copyDrainFixture(src, dst string) error {
	if err := os.MkdirAll(dst, 0750); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, e := os.ReadFile(filepath.Join(src, entry.Name()))
		if e != nil {
			return e
		}
		if e = os.WriteFile(filepath.Join(dst, entry.Name()), data, 0600); e != nil {
			return e
		}
	}
	return nil
}
