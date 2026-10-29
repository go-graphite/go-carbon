package whisperio

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func sameCachedSeries(a, b *store.Series) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	if !reflect.DeepEqual(a.Metadata, b.Metadata) || a.FromTime != b.FromTime || a.UntilTime != b.UntilTime || a.Step != b.Step || len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Values {
		if math.Float64bits(a.Values[i]) != math.Float64bits(b.Values[i]) && !(math.IsNaN(a.Values[i]) && math.IsNaN(b.Values[i])) {
			return false
		}
	}
	return true
}

func stateCacheStore(t testing.TB, dir string, buckets bool, cache int64, clock func() time.Time) *store.Store {
	t.Helper()
	db, err := openMemoryTestStore(t, dir, store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: buckets, Now: clock,
		SyncInterval: time.Hour, CacheSize: 8 << 20, MemTableSize: 1 << 20, MetadataCacheSize: cache})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// TestStateCacheMatchesUncachedStore drives two stores, with and without the
// metadata cache, through the same updates (in order, late, duplicate and
// sidecar-merging), flushes, compactions and a reopen, and compares every
// fetch and snapshot.
func TestStateCacheMatchesUncachedStore(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%v", buckets), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			base := int64(1_790_000_000)
			now := time.Unix(base, 0)
			clock := func() time.Time { return now }
			root := t.TempDir()
			plain := stateCacheStore(t, filepath.Join(root, "plain"), buckets, -0, clock)
			restorePlain := plain.DisableMetadataCacheForTest()
			defer restorePlain()
			cachedDir := filepath.Join(root, "cached")
			cached := stateCacheStore(t, cachedDir, buckets, 64<<20, clock)
			defer func() { plain.Close(); cached.Close() }()
			metrics := []string{"cache.a", "cache.b", "cache.c"}
			for _, db := range []*store.Store{plain, cached} {
				for _, name := range metrics {
					if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 600}, {Step: 10, Count: 360}, {Step: 60, Count: 240}},
						AggregationMethod: store.Average, XFilesFactor: .3}); err != nil {
						t.Fatal(err)
					}
				}
			}
			rng := rand.New(rand.NewSource(int64(len(metrics)) + map[bool]int64{false: 1, true: 2}[buckets]))
			for round := range 300 {
				now = time.Unix(base+int64(round)*7, 0)
				name := metrics[rng.Intn(len(metrics))]
				var input []store.Point
				for i := range 1 + rng.Intn(9) {
					input = append(input, store.Point{Timestamp: now.Unix() - int64(i), Value: float64(rng.Intn(1000))})
				}
				if round%4 == 3 { // late points into the sidecar and rollup corrections
					for range 1 + rng.Intn(4) {
						input = append(input, store.Point{Timestamp: now.Unix() - int64(30+rng.Intn(500)), Value: float64(rng.Intn(1000)) + .5})
					}
				}
				merge := round%17 == 16
				for _, db := range []*store.Store{plain, cached} {
					if err := db.UpdateManyMaintained(ctx, name, input, store.ContinuationMaintenance{Allow: func() bool { return merge }}); err != nil {
						t.Fatalf("round %d update: %v", round, err)
					}
				}
				if round%53 == 52 {
					for _, db := range []*store.Store{plain, cached} {
						if err := db.Compact(); err != nil {
							t.Fatal(err)
						}
					}
				}
				if round == 150 {
					if err := cached.Close(); err != nil {
						t.Fatal(err)
					}
					cached = stateCacheStore(t, cachedDir, buckets, 64<<20, clock)
				}
				for _, name := range metrics {
					for _, age := range []int{5, 60, 900, 4000, 14000} {
						from, until := int(now.Unix())-age, int(now.Unix())
						want, err := plain.Fetch(ctx, name, from, until)
						if err != nil {
							t.Fatal(err)
						}
						for range 2 { // the second read hits an entry the first may fill
							got, err := cached.Fetch(ctx, name, from, until)
							if err != nil {
								t.Fatal(err)
							}
							if !sameCachedSeries(want, got) {
								t.Fatalf("round %d %s age %d: cached fetch differs\nwant %+v\n got %+v", round, name, age, want, got)
							}
						}
					}
				}
				if round%25 == 0 {
					for _, name := range metrics {
						want, err := plain.Snapshot(ctx, name)
						if err != nil {
							t.Fatal(err)
						}
						got, err := cached.Snapshot(ctx, name)
						if err != nil || !reflect.DeepEqual(want.Archives, got.Archives) || !reflect.DeepEqual(want.Metadata, got.Metadata) {
							t.Fatalf("round %d %s: snapshots differ: %v", round, name, err)
						}
					}
				}
			}
			stats := cached.Stats().StateCache
			if stats.Installs == 0 || stats.Hits == 0 || stats.Fills == 0 {
				t.Fatalf("cache unused: %+v", stats)
			}
		})
	}
}

// TestStateCacheFollowsEveryMutation checks reads of a cached metric against
// uncached reads after each mutation path that changes cached keys.
func TestStateCacheFollowsEveryMutation(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%v", buckets), func(t *testing.T) {
			ctx := context.Background()
			base := int64(1_790_000_000)
			now := time.Unix(base, 0)
			db := stateCacheStore(t, t.TempDir(), buckets, 64<<20, func() time.Time { return now })
			defer db.Close()
			const name = "cache.mutated"
			config := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 600}, {Step: 60, Count: 240}}, AggregationMethod: store.Sum}
			check := func(step string) {
				t.Helper()
				from, until := int(now.Unix())-3000, int(now.Unix())
				var gotMeta, wantMeta store.Metadata
				var got, want *store.Series
				var gotErr, wantErr error
				for range 2 {
					got, gotErr = db.Fetch(ctx, name, from, until)
					gotMeta, _ = db.Metadata(ctx, name)
				}
				restore := db.DisableMetadataCacheForTest()
				want, wantErr = db.Fetch(ctx, name, from, until)
				wantMeta, _ = db.Metadata(ctx, name)
				restore()
				if !errors.Is(gotErr, wantErr) && (gotErr == nil) != (wantErr == nil) || !sameCachedSeries(want, got) || !reflect.DeepEqual(gotMeta, wantMeta) {
					t.Fatalf("after %s: cached read differs: %v/%v\nwant %+v\n got %+v", step, wantErr, gotErr, want, got)
				}
			}
			update := func(late bool) {
				t.Helper()
				now = now.Add(5 * time.Second)
				input := []store.Point{{Timestamp: now.Unix() - 1, Value: 3}, {Timestamp: now.Unix(), Value: 4}}
				if late {
					input = append(input, store.Point{Timestamp: now.Unix() - 200, Value: 9})
				}
				if err := db.UpdateManyMaintained(ctx, name, input, store.ContinuationMaintenance{Allow: func() bool { return late }}); err != nil {
					t.Fatal(err)
				}
			}
			created, err := db.Create(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			check("create")
			if err := db.InitializeActivity(ctx, name, created); err != nil && !errors.Is(err, store.ErrConflict) {
				t.Fatal(err)
			}
			check("activity initialization")
			for i := range 6 {
				update(i%2 == 1)
				check(fmt.Sprintf("update %d", i))
			}
			snapshot, err := db.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			// Continuation metrics refuse flattened fills and reconciles; the
			// refused mutation must still leave the cache right.
			if _, err := db.Fill(ctx, snapshot); err != nil && !errors.Is(err, store.ErrFormat) {
				t.Fatal(err)
			}
			check("fill")
			if _, err := db.ReconcileSnapshot(ctx, snapshot); err != nil && !errors.Is(err, store.ErrFormat) {
				t.Fatal(err)
			}
			check("reconcile")
			update(false)
			check("update after reconcile")
			current, err := db.Metadata(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.DeleteIfUnchanged(ctx, name, current); err != nil {
				t.Fatal(err)
			}
			check("delete")
			if _, err := db.Create(ctx, config); err != nil {
				t.Fatal(err)
			}
			check("recreate")
			update(true)
			check("update of the new generation")
			snapshot, err = db.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Metadata.AggregationMethod = store.Max
			if _, err := db.Replace(ctx, snapshot); err != nil && !errors.Is(err, store.ErrFormat) {
				t.Fatal(err)
			}
			check("replace")
			update(false)
			check("update after replace")
			if err := db.Delete(ctx, name); err != nil {
				t.Fatal(err)
			}
			check("unconditional delete")
		})
	}
}

// TestStateCacheConcurrentReadersSeeCommittedState races cached fetches with
// updates, and with delete/recreate cycles, under the race detector. A fetch
// must reflect every update and delete that finished before it began.
func TestStateCacheConcurrentReadersSeeCommittedState(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%v", buckets), func(t *testing.T) {
			ctx := context.Background()
			base := int64(1_790_000_000)
			var clockNanos atomic.Int64
			clockNanos.Store(time.Unix(base+3600, 0).UnixNano())
			clock := func() time.Time { return time.Unix(0, clockNanos.Load()) }
			db := stateCacheStore(t, t.TempDir(), buckets, 64<<20, clock)
			defer db.Close()
			config := func(name string) store.MetricConfig {
				return store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 7200}, {Step: 60, Count: 240}}, AggregationMethod: store.Last}
			}
			if _, err := db.Create(ctx, config("race.appended")); err != nil {
				t.Fatal(err)
			}
			const appends, cycles, readers = 2000, 300, 4
			var appended, generation, deleted, reads atomic.Int64
			var failure atomic.Value
			fail := func(format string, args ...any) { failure.CompareAndSwap(nil, fmt.Sprintf(format, args...)) }
			var writers sync.WaitGroup
			done := make(chan struct{})
			writers.Add(2)
			go func() { // appends point i = value i at base+i
				defer writers.Done()
				for i := int64(1); i <= appends && failure.Load() == nil; i++ {
					if err := db.UpdateMany(ctx, "race.appended", []store.Point{{Timestamp: base + i, Value: float64(i)}}); err != nil {
						fail("append %d: %v", i, err)
						return
					}
					appended.Store(i)
				}
			}()
			go func() { // generation k writes value k, then is deleted
				defer writers.Done()
				for k := int64(1); k <= cycles && failure.Load() == nil; k++ {
					if _, err := db.Create(ctx, config("race.cycled")); err != nil {
						fail("create %d: %v", k, err)
						return
					}
					if err := db.UpdateMany(ctx, "race.cycled", []store.Point{{Timestamp: base + 100, Value: float64(k)}}); err != nil {
						fail("cycle write %d: %v", k, err)
						return
					}
					generation.Store(k)
					if err := db.Delete(ctx, "race.cycled"); err != nil {
						fail("delete %d: %v", k, err)
						return
					}
					deleted.Store(k)
				}
			}()
			var readersDone sync.WaitGroup
			for range readers {
				readersDone.Add(1)
				go func() {
					defer readersDone.Done()
					for {
						select {
						case <-done:
							return
						default:
						}
						reads.Add(1)
						committed := appended.Load()
						series, err := db.Fetch(ctx, "race.appended", int(base), int(base)+appends+1)
						if err != nil {
							fail("fetch appended: %v", err)
							return
						}
						for i := int64(1); i <= committed; i++ {
							if v := series.Values[(base+i-int64(series.FromTime))/int64(series.Step)]; v != float64(i) {
								fail("append %d committed before the fetch began is missing: %v", i, v)
								return
							}
						}
						g, d := generation.Load(), deleted.Load()
						series, err = db.Fetch(ctx, "race.cycled", int(base)+100, int(base)+101)
						if errors.Is(err, store.ErrNotFound) {
							continue
						}
						if err != nil {
							fail("fetch cycled: %v", err)
							return
						}
						for _, v := range series.Values {
							if !math.IsNaN(v) && (int64(v) < g || int64(v) <= d) {
								fail("fetch returned generation %v after generation %d was written and %d deleted", v, g, d)
								return
							}
						}
					}
				}()
			}
			writers.Wait()
			close(done)
			readersDone.Wait()
			if f := failure.Load(); f != nil {
				t.Fatal(f)
			}
			if s := db.Stats().StateCache; s.Hits == 0 || s.Installs == 0 || reads.Load() < 100 {
				t.Fatalf("cache unused or too few reads (%d): %+v", reads.Load(), s)
			}
		})
	}
}
