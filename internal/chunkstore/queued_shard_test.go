package chunkstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/helper"
)

// The noop writeout queue drains one cache shard at a time. An unrelated
// metric with a slow operation must not serialize that shard's entire batch.
func TestQueuedShardWritesProgressPastUnrelatedMetric(t *testing.T) {
	t.Parallel()
	db, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	names := []string{"queued.shard.held"}
	shard := helper.HashString(names[0]) & 1023
	for i := 0; len(names) < 17; i++ {
		name := fmt.Sprintf("queued.shard.metric.%d", i)
		if helper.HashString(name)&1023 == shard {
			names = append(names, name)
		}
	}
	for _, name := range names {
		if _, err := db.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 1, Count: 3600}}, AggregationMethod: Sum}); err != nil {
			t.Fatal(err)
		}
	}
	unlock := db.lockMetric(names[0])
	var release sync.Once
	releaseHeld := func() { release.Do(unlock) }
	var workers sync.WaitGroup
	defer func() { releaseHeld(); workers.Wait() }()
	completed := make(chan error, len(names)-1)
	for i, name := range names[1:] {
		workers.Add(1)
		go func(name string, value float64) {
			defer workers.Done()
			completed <- db.UpdateManyWithArchiveValues(ctx, name, nil, []ArchiveValue{{Archive: 0, Timestamp: int64(now - 1), Value: value, Present: true}})
		}(name, float64(i)+0.125)
	}
	select {
	case err := <-completed:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(2 * time.Second):
		t.Error("unrelated queued-shard writes all blocked behind one held metric")
	}
	releaseHeld()
	workers.Wait()
	close(completed)
	for err := range completed {
		if err != nil {
			t.Error(err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	for i, name := range names[1:] {
		snapshot, err := db.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		points := snapshot.Archives[0].Points
		if len(points) != 1 || points[0].Timestamp != int64(now-1) || points[0].Value != float64(i)+0.125 {
			t.Fatalf("queued-shard value changed after compaction: %s %+v", name, points)
		}
	}
}
