package whisperio

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// BenchmarkContinuationMetadataCache measures a persister-shaped update and a
// one-hour fetch of hot metrics with and without the metadata cache, in the v6
// and v7 layouts, reporting the LSM reads (point lookups and walks) per op.
func BenchmarkContinuationMetadataCache(b *testing.B) {
	for _, buckets := range []bool{false, true} {
		for _, cache := range []int64{0, 64 << 20} {
			b.Run(fmt.Sprintf("buckets=%v/cache=%v", buckets, cache > 0), func(b *testing.B) {
				const metrics, batch = 256, 30
				ctx := context.Background()
				base := int64(1_790_000_000)
				now := time.Unix(base, 0)
				db, err := store.Open(filepath.Join(b.TempDir(), "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{},
					TimeBuckets: buckets, Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20,
					MetadataCacheSize: cache})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				if cache == 0 {
					defer db.DisableMetadataCacheForTest()()
				}
				names := make([]string, metrics)
				input := make([]store.Point, batch)
				round := int64(0)
				update := func(name string) {
					stamp := base + (round+1)*batch
					now = time.Unix(stamp+1, 0)
					for j := range input {
						input[j] = store.Point{Timestamp: stamp - int64(batch-j), Value: float64(j)}
					}
					if err := db.UpdateMany(ctx, name, input); err != nil {
						b.Fatal(err)
					}
				}
				for i := range names {
					names[i] = fmt.Sprintf("bench.cache.%d", i)
					if _, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}},
						AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
						b.Fatal(err)
					}
				}
				for ; round < 20; round++ {
					for _, name := range names {
						update(name)
					}
				}
				if err := db.Flush(); err != nil {
					b.Fatal(err)
				}
				b.Run("update", func(b *testing.B) {
					reads := store.CountStoreReadsForTest()
					b.ReportAllocs()
					for i := range b.N {
						if i%metrics == 0 {
							round++
						}
						update(names[i%metrics])
					}
					b.ReportMetric(float64(reads())/float64(b.N), "reads/op")
				})
				b.Run("fetch", func(b *testing.B) {
					reads := store.CountStoreReadsForTest()
					b.ReportAllocs()
					for i := range b.N {
						if _, err := db.Fetch(ctx, names[i%metrics], int(now.Unix())-3600, int(now.Unix())); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(reads())/float64(b.N), "reads/op")
				})
			})
		}
	}
}
