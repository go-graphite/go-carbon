package whisperio

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// BenchmarkContinuationUpdatePath measures one persister-shaped update through
// the store: metric lock, metadata, codec update and batch commit, appending a
// batch of 1-second points to one of many metrics in turn.
//
// timed=true wraps each update as the persister does: the update's total is
// recorded and one in stagetime.SampleEvery times its stages.
func BenchmarkContinuationUpdatePath(b *testing.B) {
	for _, run := range []struct{ buckets, timed bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		buckets, timed := run.buckets, run.timed
		b.Run(fmt.Sprintf("buckets=%v/timed=%v", buckets, timed), func(b *testing.B) {
			const metrics, batch = 256, 30
			ctx := context.Background()
			base := int64(1_700_000_000)
			now := time.Unix(base, 0)
			db, err := store.Open(filepath.Join(b.TempDir(), "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{},
				TimeBuckets: buckets, Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 16 << 20})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			names := make([]string, metrics)
			for i := range names {
				names[i] = fmt.Sprintf("bench.update.%d", i)
				if _, err := db.Create(ctx, store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}},
					AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
					b.Fatal(err)
				}
			}
			input := make([]store.Point, batch)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				round := int64(i / metrics)
				stamp := base + (round+1)*batch
				now = time.Unix(stamp+1, 0)
				for j := range input {
					input[j] = store.Point{Timestamp: stamp - int64(batch-j), Value: float64(i + j)}
				}
				if !timed {
					if err := db.UpdateMany(ctx, names[i%metrics], input); err != nil {
						b.Fatal(err)
					}
					continue
				}
				start := time.Now()
				update, timer := stagetime.Start(ctx)
				if err := db.UpdateMany(update, names[i%metrics], input); err != nil {
					b.Fatal(err)
				}
				timer.Finish()
				stagetime.ObserveUpdate(time.Since(start))
			}
		})
	}
}
