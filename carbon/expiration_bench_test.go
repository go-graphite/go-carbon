package carbon

import (
	"context"
	"fmt"
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
