package whisperio

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// A sampled update through the store and codec records its stages.
func TestContinuationUpdateRecordsStages(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		ctx := context.Background()
		now := time.Unix(1_700_000_100, 0)
		db, err := openTestStore(filepath.Join(t.TempDir(), "shared"), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: buckets, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Create(ctx, store.MetricConfig{Name: "stage.metric", Retentions: []store.Retention{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			t.Fatal(err)
		}
		write := func(update context.Context, stamp int64, n int) {
			t.Helper()
			input := make([]store.Point, n)
			for i := range input {
				input[i] = store.Point{Timestamp: stamp - int64(len(input)-i), Value: float64(i)}
			}
			if err := db.UpdateMany(update, "stage.metric", input); err != nil {
				t.Fatal(err)
			}
		}
		// Enough history that blocks are sealed into storage and the timed
		// update must read stored groups.
		for range 6 {
			now = now.Add(1000 * time.Second)
			write(ctx, now.Unix(), 1000)
		}
		before := stagetime.Snapshot()
		var timed context.Context
		var timer *stagetime.Update
		for timer == nil {
			timed, timer = stagetime.Start(ctx)
		}
		now = now.Add(20 * time.Second)
		write(timed, now.Unix(), 20)
		timer.Finish()
		after := stagetime.Snapshot()
		for _, s := range []stagetime.Stage{stagetime.LockWait, stagetime.Route, stagetime.Load, stagetime.Decode, stagetime.Encode, stagetime.Commit} {
			if after[s].Calls == before[s].Calls {
				t.Errorf("buckets=%v: stage %s not recorded", buckets, s)
			}
		}
		if after[stagetime.TailRead].Calls == before[stagetime.TailRead].Calls {
			t.Errorf("buckets=%v: appending to an existing block recorded no tail read", buckets)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
