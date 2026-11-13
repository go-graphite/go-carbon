package persister

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

// The old and new cache partitioning must persist exactly the same timestamps,
// float bits and archive shapes through cold restored writes, flush and reopen.
// The last two metrics cover empty archives and creating a missing metric.
func TestRecoveryDrainCachePartitionParity(t *testing.T) {
	freezeWhisperNow(t)
	const now = int64(1794596400)
	ctx := context.Background()
	var stores [2]*store.Store
	var writers [2]*Whisper
	var caches [2]*cache.Cache
	var options [2]store.Options
	var dirs [2]string
	for side, shards := range []int{64, 4} {
		root := t.TempDir()
		dirs[side] = filepath.Join(root, "store")
		options[side] = store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true},
			Now: func() time.Time { return time.Unix(now, 0) }, IncrementalSource: filepath.Join(root, "originals"),
			SyncInterval: time.Hour, CacheSize: 64 << 10, CacheShards: shards, MetadataCacheSize: 8 << 20, MemTableSize: 1 << 20, BloomFilterBits: 10}
		if err := os.MkdirAll(options[side].IncrementalSource, 0750); err != nil {
			t.Fatal(err)
		}
		db, err := store.Open(dirs[side], options[side])
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		for metric := 0; metric < 5; metric++ {
			name := fmt.Sprintf("restore.metric.%d", metric)
			if _, err = db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 7200}, {Step: 60, Count: 1440}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
				t.Fatal(err)
			}
			if metric == 4 {
				continue
			}
			history := make([]store.Point, 4096)
			for i := range history {
				history[i] = store.Point{Timestamp: now - 4096 + int64(i), Value: float64(i%97) + .25}
			}
			if err = db.UpdateMany(ctx, name, history); err != nil {
				t.Fatal(err)
			}
		}
		if err = db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = store.Open(dirs[side], options[side])
		if err != nil {
			t.Fatal(err)
		}
		db.StopBackground()
		stores[side] = db
		t.Cleanup(func() { stores[side].Close() })
		c := cache.New()
		c.SetMaxSize(1000000)
		c.SetRecoveryDrain(true)
		caches[side] = c
		p := newOOOTestPersister(t, options[side].IncrementalSource, &fakeCache{})
		p.SetMetricStore(db)
		p.pop = c.PopNotConfirmed
		p.confirm = c.Confirm
		p.requeue = c.Requeue
		p.incrementalStore = db
		writers[side] = p
	}
	rng := rand.New(rand.NewSource(8317))
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000012, 0x7ff0000000000001, 0x3ff0000000000001}
	for round := 0; round < 5; round++ {
		for metric := 0; metric < 6; metric++ {
			batch := make([]points.Point, 1024)
			for i := range batch {
				batch[i] = points.Point{Timestamp: now - 3600 + int64(rng.Intn(3599)), Value: float64(rng.Intn(10000)) + .125}
			}
			// Duplicate corrections, special float bits, future and expired points.
			for i, v := range bits {
				batch = append(batch, points.Point{Timestamp: now - 100 - int64(i), Value: math.Float64frombits(v)})
			}
			batch = append(batch, points.Point{Timestamp: now - 100, Value: math.Copysign(0, -1)}, points.Point{Timestamp: now + 30, Value: 3.25}, points.Point{Timestamp: now - 200000, Value: 17})
			name := fmt.Sprintf("restore.metric.%d", metric)
			for _, c := range caches {
				c.Add(&points.Points{Metric: name, Data: append([]points.Point(nil), batch...)})
			}
		}
		for side, p := range writers {
			var wg sync.WaitGroup
			for metric := 0; metric < 6; metric++ {
				wg.Go(func() { p.store(fmt.Sprintf("restore.metric.%d", metric)) })
			}
			wg.Wait()
			if !caches[side].IsEmpty() {
				t.Fatalf("round %d side %d did not drain", round, side)
			}
		}
	}
	compare := func() {
		t.Helper()
		for metric := 0; metric < 6; metric++ {
			name := fmt.Sprintf("restore.metric.%d", metric)
			want, err := stores[0].Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := stores[1].Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.EqualSnapshots(want, got); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
	}
	compare()
	for side, db := range stores {
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := store.Open(dirs[side], options[side])
		if err != nil {
			t.Fatal(err)
		}
		reopened.StopBackground()
		stores[side] = reopened
	}
	compare()
}
