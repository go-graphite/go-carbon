package persister

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

func TestStorageExactMigrationOOOAndBufferedRollups(t *testing.T) {
	now := storageTestClock(t)
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		t.Run(method.String(), func(t *testing.T) {
			now.Store(storageEpoch)
			source := newStorageBackend(t, "cwhisper-ooo", now)
			destination := newStorageBackend(t, "pebble-exact", now)
			config := storageConfig("metric", "1s:1m,10s:10m,60s:1h", method, .5)
			storageMust(t, source.create(config))
			values := []float64{.1, .2, -.3, 1e16, -1e16, math.SmallestNonzeroFloat64, math.Float64frombits(0x8000000000000000)}
			var initial []whisper.TimeSeriesPoint
			for i := 0; i < 58; i++ {
				initial = append(initial, whisper.TimeSeriesPoint{Time: storageEpoch - 59 + i, Value: values[i%len(values)]})
			}
			batches := [][]whisper.TimeSeriesPoint{
				initial,
				{{Time: storageEpoch - 53, Value: .7}, {Time: storageEpoch - 13, Value: .5}, {Time: storageEpoch - 13, Value: .25}},
				{{Time: storageEpoch + 10, Value: 19}},
				{{Time: storageEpoch - 50, Value: -7}},
			}
			ctx := context.Background()
			for i, batch := range batches {
				storageMust(t, source.update(config.Name, batch))
				snapshot, err := whisperio.ReadSnapshot(ctx, config.Name, source.path(config.Name))
				storageMust(t, err)
				if i == 0 {
					_, err = destination.db.CreateFromSnapshot(ctx, snapshot)
				} else {
					_, err = destination.db.Replace(ctx, snapshot)
				}
				storageMust(t, err)
				storageCompare(t, source, destination, config, fmt.Sprintf("migrated-batch-%d", i))
				storageMust(t, destination.compact([]string{config.Name}))
				storageMust(t, destination.reopen())
				storageCompare(t, source, destination, config, fmt.Sprintf("migrated-compacted-%d", i))
			}
			now.Add(20)
			storageCompare(t, source, destination, config, "future-visible-after-migration")
			for _, s := range []*storageBackend{source, destination} {
				storageMust(t, s.update(config.Name, []whisper.TimeSeriesPoint{{Time: storageEpoch - 12, Value: .125}, {Time: storageEpoch + 19, Value: 23}}))
			}
			storageCompare(t, source, destination, config, "post-migration-ooo-and-new-write")
		})
	}
}

func TestStorageExactPostMigrationCorrectionsMatchCWhisper(t *testing.T) {
	now := storageTestClock(t)
	source := newStorageBackend(t, "cwhisper-ooo", now)
	destination := newStorageBackend(t, "pebble-exact", now)
	config := storageConfig("metric", "1s:1h,60s:1d,3600s:30d", whisper.Average, .5)
	storageMust(t, source.create(config))
	var initial []whisper.TimeSeriesPoint
	for i := 0; i < 3000; i++ {
		initial = append(initial, whisper.TimeSeriesPoint{Time: storageEpoch - 3001 + i, Value: float64(i)})
	}
	storageMust(t, source.update(config.Name, initial))
	snapshot, err := whisperio.ReadSnapshot(context.Background(), config.Name, source.path(config.Name))
	storageMust(t, err)
	_, err = destination.db.CreateFromSnapshot(context.Background(), snapshot)
	storageMust(t, err)
	for _, s := range []*storageBackend{source, destination} {
		storageMust(t, s.update(config.Name, []whisper.TimeSeriesPoint{{Time: storageEpoch - 2000, Value: 999}}))
	}
	storageCompare(t, source, destination, config, "correction-of-existing-compressed-point")
}
