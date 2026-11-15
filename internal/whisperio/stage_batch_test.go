package whisperio

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

// Exercise actual eight-metric continuation batches, including stored tails,
// changing values and repeated commits, rather than a timer-only simulation.
func TestStageBatchPartition(t *testing.T) {
	now := time.Unix(1790000000, 0)
	db := stateCacheStore(t, t.TempDir(), true, 128<<20, func() time.Time { return now })
	defer db.Close()
	ctx := context.Background()
	updates := make([]store.MetricUpdate, 8)
	for i := range updates {
		name := fmt.Sprintf("timing.batch.%d", i)
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: percallSchemas[0].retentions, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			t.Fatal(err)
		}
		updates[i] = store.MetricUpdate{Name: name, Points: make([]store.Point, 210)}
	}
	before := stagetime.Snapshot()
	for round := range 128 {
		now = now.Add(210 * time.Second)
		for _, update := range updates {
			for j := range update.Points {
				update.Points[j] = store.Point{Timestamp: now.Unix() - int64(210-j), Value: float64(round+j) + .125}
			}
		}
		start := time.Now()
		timed, timer := stagetime.StartAt(ctx, start)
		err := db.UpdateBatch(timed, updates, nil)
		timer.FinishUpdate(start)
		if err != nil {
			t.Fatal(err)
		}
	}
	after := stagetime.Snapshot()
	total := after[stagetime.Total].Seconds - before[stagetime.Total].Seconds
	var sum float64
	for s := stagetime.Route; s < stagetime.NumStages; s++ {
		delta := after[s].Seconds - before[s].Seconds
		sum += delta
		t.Logf("%s %.6fs %.3f%%", s, delta, 100*delta/total)
	}
	if total <= 0 || math.Abs(sum-total) > total*.01 {
		t.Fatalf("partition %.9fs total %.9fs", sum, total)
	}
	other := after[stagetime.Other].Seconds - before[stagetime.Other].Seconds
	t.Logf("total %.6fs; other %.6fs (%.3f%%); named %.3f%%", total, other, 100*other/total, 100*(sum-other)/total)
	// The residual is visible even if future work is added outside named stages.
	if after[stagetime.Other].Calls == before[stagetime.Other].Calls {
		t.Fatal("other not exported")
	}
}
