package whisperio

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// BenchmarkContinuationFetch reads one day of a 1s metric from a flushed store
// with and without the shared fetch iterator.
func BenchmarkContinuationFetch(b *testing.B) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	db, err := store.Open(filepath.Join(b.TempDir(), "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, CacheSize: 64 << 20, Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	const metrics = 200
	for i := range metrics {
		name := fmt.Sprintf("bench.metric%03d", i)
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 10080}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			b.Fatal(err)
		}
	}
	// Six hours of history per metric in 10-minute updates, flushed between
	// rounds so reads cross several tables.
	for round := range 36 {
		stamp := now.Unix() - 6*3600 + int64(round)*600
		for i := range metrics {
			input := make([]store.Point, 600)
			for j := range input {
				input[j] = store.Point{Timestamp: stamp + int64(j), Value: float64(j % 97)}
			}
			if err := db.UpdateMany(ctx, fmt.Sprintf("bench.metric%03d", i), input); err != nil {
				b.Fatal(err)
			}
		}
		if round%6 == 5 {
			if err := db.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, shared := range []bool{false, true} {
		b.Run(fmt.Sprintf("shared=%v", shared), func(b *testing.B) {
			defer db.SetFetchSharedIteratorForTest(shared)()
			for i := range b.N {
				if _, err := db.Fetch(ctx, fmt.Sprintf("bench.metric%03d", i%metrics), int(now.Unix())-3600, int(now.Unix())); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
