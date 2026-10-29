package whisperio

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A point whose timestamp does not fit Whisper's 32-bit field, such as
// milliseconds sent as seconds, is dropped; the update still writes the
// metric's valid points instead of failing and being requeued forever.
func TestContinuationUpdateDropsOutOfRangeTimestamps(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		ctx := context.Background()
		now := time.Unix(1_700_000_100, 0)
		db, err := openTestStore(filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: buckets, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Create(ctx, store.MetricConfig{Name: "ms.metric", Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			t.Fatal(err)
		}
		stamp := now.Unix() - 10
		before := InvalidContinuationPoints()
		input := []store.Point{{Timestamp: stamp * 1000, Value: 1}, {Timestamp: stamp, Value: 42}, {Timestamp: -5, Value: 2}}
		if err := db.UpdateMany(ctx, "ms.metric", input); err != nil {
			t.Fatalf("buckets=%v: update with out-of-range timestamps failed: %v", buckets, err)
		}
		if got := InvalidContinuationPoints() - before; got != 2 {
			t.Fatalf("buckets=%v: dropped %d points, want 2", buckets, got)
		}
		if err := db.UpdateMany(ctx, "ms.metric", []store.Point{{Timestamp: stamp * 1000, Value: 3}}); err != nil {
			t.Fatalf("buckets=%v: update with only out-of-range timestamps failed: %v", buckets, err)
		}
		got, err := db.Fetch(ctx, "ms.metric", int(stamp)-1, int(stamp)+1)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.Values[(int(stamp)-got.FromTime)/got.Step] != 42 {
			t.Fatalf("buckets=%v: valid point not written: %+v", buckets, got)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
