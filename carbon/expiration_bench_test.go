package carbon

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Measure the complete catalog path with a fixed page size. The high scan rate
// removes the production throttle from this measurement of cost per metric.
func BenchmarkExpirationScan(b *testing.B) {
	for _, count := range []int{1000, 100_000} {
		b.Run(fmt.Sprintf("metrics=%d", count), func(b *testing.B) {
			now := time.Unix(100_000, 0)
			db, err := store.Open(b.TempDir(), store.Options{
				Now: func() time.Time { return now }, SyncInterval: time.Hour,
				CacheSize: 8 << 20, MemTableSize: 1 << 20,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			})
			for i := 0; i < count; i++ {
				_, err := db.Create(context.Background(), store.MetricConfig{
					Name: fmt.Sprintf("jobs.%08d", i), Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Average,
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			if err := db.Compact(); err != nil {
				b.Fatal(err)
			}
			r := &metricExpirer{db: db, now: func() time.Time { return now }, expiration: time.Hour,
				rate: 1_000_000_000, stats: &expirationStats{},
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := r.sweep(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/metric")
			if r.stats.examined.Load() != uint64(b.N*count) || r.stats.deleted.Load() != 0 {
				b.Fatal("scan lost or deleted active metrics")
			}
		})
	}
}

// The fixture is a real 10M-name Pebble store prepared in bounded, opt-in steps
// by TestPrepareExpirationScaleStore. Preparation is outside all sweep timers.
func BenchmarkExpirationScaleScan(b *testing.B) {
	dir := os.Getenv("EXPIRATION_SCALE_DIR")
	if dir == "" {
		b.Skip("set EXPIRATION_SCALE_DIR to a prepared >=10M-name store")
	}
	db, err := store.Open(dir, store.Options{CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Hour, CompactionWorkers: 2})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	var count uint64
	err = store.EachPage(context.Background(), db.ListPage, "", 10000, func(page []store.Metadata) error { count += uint64(len(page)); return nil })
	if err != nil || count < 10_000_000 {
		b.Fatalf("need >=10M names: %d %v", count, err)
	}
	rate := 1_000_000_000
	if os.Getenv("EXPIRATION_SCALE_PACED") == "1" {
		rate = 100_000
	}
	now := time.Unix(1792000000, 0)
	if os.Getenv("EXPIRATION_SCALE_ACTIVE") == "1" {
		now = time.Unix(1790000000, 0)
	}
	r := &metricExpirer{db: db, now: func() time.Time { return now }, expiration: time.Hour, rate: rate, dryRun: true, stats: &expirationStats{}}
	b.ReportAllocs()
	var before, after syscall.Rusage
	b.ResetTimer()
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	for range b.N {
		if err := r.sweep(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	b.StopTimer()
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-seconds/sweep")
	b.ReportMetric(float64(count)*float64(b.N)/b.Elapsed().Seconds(), "names/s")
	b.ReportMetric(b.Elapsed().Seconds()/float64(b.N), "seconds/sweep")
	if r.stats.examined.Load() != count*uint64(b.N) || r.stats.deleted.Load() != 0 {
		b.Fatal("scan missed names or deleted data")
	}
}
