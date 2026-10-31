package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func prewarmConfig(name string) store.MetricConfig {
	return store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 600}, {Step: 10, Count: 360}, {Step: 60, Count: 240}},
		AggregationMethod: store.Average, XFilesFactor: .3}
}

// checkCachedReads compares cached fetches and metadata of names with reads
// that bypass the cache.
func checkCachedReads(t *testing.T, db *store.Store, names []string, now time.Time, step string) {
	t.Helper()
	ctx := context.Background()
	for _, name := range names {
		from, until := int(now.Unix())-4000, int(now.Unix())
		var got, want *store.Series
		var gotMeta, wantMeta store.Metadata
		var gotErr, wantErr, gotMetaErr, wantMetaErr error
		for range 2 {
			got, gotErr = db.Fetch(ctx, name, from, until)
			gotMeta, gotMetaErr = db.Metadata(ctx, name)
		}
		restore := db.DisableMetadataCacheForTest()
		want, wantErr = db.Fetch(ctx, name, from, until)
		wantMeta, wantMetaErr = db.Metadata(ctx, name)
		restore()
		if (gotErr == nil) != (wantErr == nil) || (gotMetaErr == nil) != (wantMetaErr == nil) || !sameCachedSeries(want, got) || !reflect.DeepEqual(gotMeta, wantMeta) {
			t.Fatalf("%s: %s: cached read differs: %v/%v %v/%v\nwant %+v\n got %+v", step, name, wantErr, gotErr, wantMetaErr, gotMetaErr, want, got)
		}
	}
}

// TestPrewarmLoadsWhatUpdatesRead prewarms a reopened store and checks that
// updates then read no continuation state from the database, and that the
// store keeps matching one without the cache through further updates.
func TestPrewarmLoadsWhatUpdatesRead(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("buckets=%v", buckets), func(t *testing.T) {
			ctx := context.Background()
			base := int64(1_790_000_000)
			now := time.Unix(base, 0)
			clock := func() time.Time { return now }
			root := t.TempDir()
			plain := stateCacheStore(t, filepath.Join(root, "plain"), buckets, 0, clock)
			defer plain.DisableMetadataCacheForTest()()
			cachedDir := filepath.Join(root, "cached")
			cached := stateCacheStore(t, cachedDir, buckets, 64<<20, clock)
			defer func() { plain.Close(); cached.Close() }()
			var names []string
			for i := range 24 {
				names = append(names, fmt.Sprintf("prewarm.m%02d", i))
			}
			rng := rand.New(rand.NewSource(7))
			// update writes the same points to both stores and returns the
			// reads the cached store made, its route lookup included.
			update := func(name string, late bool) uint64 {
				t.Helper()
				input := []store.Point{{Timestamp: now.Unix(), Value: float64(rng.Intn(100))}}
				if late { // a sidecar, and a rollup correction
					input = append(input, store.Point{Timestamp: now.Unix() - int64(60+rng.Intn(400)), Value: .5})
				}
				if err := plain.UpdateMany(ctx, name, input); err != nil {
					t.Fatal(err)
				}
				stop := store.CountStoreReadsForTest()
				_, err := cached.Metadata(ctx, name)
				if err == nil {
					err = cached.UpdateMany(ctx, name, input)
				}
				reads := stop()
				if err != nil {
					t.Fatal(err)
				}
				return reads
			}
			for _, db := range []*store.Store{plain, cached} {
				for _, name := range names {
					if _, err := db.Create(ctx, prewarmConfig(name)); err != nil {
						t.Fatal(err)
					}
				}
			}
			for round := range 20 {
				now = time.Unix(base+int64(round)*3, 0)
				for i, name := range names {
					update(name, i%3 == 0 && round%5 == 4)
				}
			}
			if err := cached.Close(); err != nil {
				t.Fatal(err)
			}
			cached = stateCacheStore(t, cachedDir, buckets, 64<<20, clock)
			// The reads of one update per metric.
			roundReads := func() uint64 {
				reads := uint64(0)
				for _, name := range names {
					now = now.Add(time.Second)
					reads += update(name, false)
				}
				return reads
			}
			cold := roundReads()
			cached.ForgetMetadataCacheForTest()

			var priority iter.Seq[string]
			if !buckets {
				priority = slices.Values(names)
			}
			stats, err := cached.PrewarmMetadata(ctx, priority, store.PrewarmOptions{Workers: 2, Batch: 5})
			if err != nil || stats.Prewarmed != int64(len(names)) || stats.Failed != 0 {
				t.Fatalf("prewarm: %+v, %v", stats, err)
			}
			checkCachedReads(t, cached, names, now, "prewarm")
			misses := cached.Stats().StateCache.Misses
			prewarmed := roundReads()
			if s := cached.Stats().StateCache; s.Misses != misses {
				t.Fatalf("updates after the prewarm missed the cache: %+v", s)
			}
			// Prewarm loads file state. The first append also loads its block
			// descriptor and tail group; write-through keeps those two reads
			// out of the following update.
			installed := roundReads()
			t.Logf("reads per update: cold %.1f, prewarmed %.1f, installed %.1f", float64(cold)/float64(len(names)),
				float64(prewarmed)/float64(len(names)), float64(installed)/float64(len(names)))
			if prewarmed > installed+2*uint64(len(names)) || prewarmed >= cold {
				t.Fatalf("updates read %d keys after the prewarm, %d after their own updates, %d cold", prewarmed, installed, cold)
			}
			for round := range 10 {
				now = now.Add(5 * time.Second)
				for i, name := range names {
					update(name, (i+round)%4 == 0)
				}
				checkCachedReads(t, cached, names, now, fmt.Sprintf("round %d", round))
			}
			for _, name := range names {
				want, err := plain.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := cached.Snapshot(ctx, name)
				if err != nil || !reflect.DeepEqual(want.Archives, got.Archives) {
					t.Fatalf("%s: snapshots differ: %v", name, err)
				}
			}
		})
	}
}

// TestPrewarmSkipsStateChangedWhileRead runs each mutation of a metric
// between the prewarm reading its state and filling the cache: the fill must
// be dropped, and later reads and updates must see the mutation.
func TestPrewarmSkipsStateChangedWhileRead(t *testing.T) {
	type mutation struct {
		name   string
		mutate func(db *store.Store, now time.Time) error
	}
	const metric = "prewarm.raced"
	mutations := []mutation{
		{"append", func(db *store.Store, now time.Time) error {
			return db.UpdateMany(context.Background(), metric, []store.Point{{Timestamp: now.Unix(), Value: 77}})
		}},
		{"late point merge", func(db *store.Store, now time.Time) error {
			input := []store.Point{{Timestamp: now.Unix(), Value: 5}, {Timestamp: now.Unix() - 300, Value: 66}}
			return db.UpdateManyMaintained(context.Background(), metric, input, store.ContinuationMaintenance{Allow: func() bool { return true }})
		}},
		{"delete", func(db *store.Store, _ time.Time) error {
			return db.Delete(context.Background(), metric)
		}},
		{"delete and recreate", func(db *store.Store, now time.Time) error {
			ctx := context.Background()
			if err := db.Delete(ctx, metric); err != nil {
				return err
			}
			config := prewarmConfig(metric)
			config.Retentions = []store.Retention{{Step: 10, Count: 500}}
			if _, err := db.Create(ctx, config); err != nil {
				return err
			}
			return db.UpdateMany(ctx, metric, []store.Point{{Timestamp: now.Unix(), Value: 3}})
		}},
		{"other metric of the shard", nil},
	}
	for _, buckets := range []bool{false, true} {
		for _, m := range mutations {
			t.Run(fmt.Sprintf("buckets=%v/%s", buckets, m.name), func(t *testing.T) {
				ctx := context.Background()
				base := int64(1_790_000_000)
				now := time.Unix(base, 0)
				db := stateCacheStore(t, t.TempDir(), buckets, 64<<20, func() time.Time { return now })
				defer db.Close()
				names := []string{metric}
				mutate := m.mutate
				if mutate == nil {
					// A metric of the same metadata cache shard as metric.
					other := sameShardMetric(db, metric)
					names = append(names, other)
					mutate = func(db *store.Store, now time.Time) error {
						return db.UpdateMany(ctx, other, []store.Point{{Timestamp: now.Unix(), Value: 1}})
					}
				}
				for _, name := range names {
					if _, err := db.Create(ctx, prewarmConfig(name)); err != nil {
						t.Fatal(err)
					}
					for i := range 30 {
						points := []store.Point{{Timestamp: base - 30 + int64(i), Value: float64(i)}}
						if i%7 == 6 {
							points = append(points, store.Point{Timestamp: base - 200 - int64(i), Value: 2})
						}
						if err := db.UpdateMany(ctx, name, points); err != nil {
							t.Fatal(err)
						}
					}
				}
				db.ForgetMetadataCacheForTest()
				now = now.Add(10 * time.Second)
				fired := false
				var mutateErr error
				defer store.SetPrewarmBeforeFillForTest(func(name string) {
					if name == metric && !fired {
						fired = true
						mutateErr = mutate(db, now)
					}
				})()
				priority := slices.Values([]string{metric})
				if buckets && len(names) == 1 {
					priority = nil
				}
				stats, err := db.PrewarmMetadata(ctx, priority, store.PrewarmOptions{Workers: 1})
				if mutateErr != nil {
					t.Fatal(mutateErr)
				}
				if err != nil || !fired || stats.Prewarmed != 0 || stats.Raced+stats.Cached != 1 {
					t.Fatalf("prewarm across a mutation: %+v, %v (fired %v)", stats, err, fired)
				}
				checkCachedReads(t, db, names, now, "mutation during prewarm")
				// An update now starts from whatever the cache holds.
				now = now.Add(time.Second)
				if err := db.UpdateMany(ctx, metric, []store.Point{{Timestamp: now.Unix(), Value: 9}}); err != nil && !errors.Is(err, store.ErrNotFound) {
					t.Fatal(err)
				}
				checkCachedReads(t, db, names, now, "update after the prewarm")
			})
		}
	}
}

// sameShardMetric returns a metric name in the same metadata cache shard as
// name.
func sameShardMetric(db *store.Store, name string) string {
	for i := 0; ; i++ {
		other := fmt.Sprint("prewarm.other", i)
		if db.SameMetricStripeForTest(name, other) {
			return other
		}
	}
}

// TestPrewarmConcurrentWithWriters races repeated prewarms with appends, late
// points and delete/recreate cycles, with readers, and with forgetting every
// state as eviction would, under the race detector. Updates start from the
// cached state, so a stale prewarm would lose appended points: every append
// that finished before a read began must be visible to it.
func TestPrewarmConcurrentWithWriters(t *testing.T) {
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
			names := []string{"race.appended", "race.late", "race.cycled"}
			for _, name := range names[:2] {
				if _, err := db.Create(ctx, config(name)); err != nil {
					t.Fatal(err)
				}
			}
			const appends, cycles = 300, 40
			// Writers pause between mutations, so that many prewarm reads
			// finish unraced and their fills land among the mutations.
			pause := func(i int64) { time.Sleep(time.Duration(i%7) * 40 * time.Microsecond) }
			var appended, late, generation, deleted, prewarms atomic.Int64
			var failure atomic.Value
			fail := func(format string, args ...any) { failure.CompareAndSwap(nil, fmt.Sprintf(format, args...)) }
			var writers sync.WaitGroup
			done := make(chan struct{})
			writers.Add(3)
			go func() { // appends point i = value i at base+i
				defer writers.Done()
				for i := int64(1); i <= appends && failure.Load() == nil; i++ {
					if err := db.UpdateMany(ctx, "race.appended", []store.Point{{Timestamp: base + i, Value: float64(i)}}); err != nil {
						fail("append %d: %v", i, err)
						return
					}
					appended.Store(i)
					pause(i)
				}
			}()
			go func() { // a current point, then late point i = value i at base+i
				defer writers.Done()
				for i := int64(1); i <= appends/3 && failure.Load() == nil; i++ {
					input := []store.Point{{Timestamp: base + 3000 + i, Value: 1}, {Timestamp: base + i, Value: float64(i)}}
					if err := db.UpdateManyMaintained(ctx, "race.late", input, store.ContinuationMaintenance{Allow: func() bool { return i%25 == 0 }}); err != nil {
						fail("late %d: %v", i, err)
						return
					}
					late.Store(i)
					pause(i)
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
					pause(k)
				}
			}()
			var others sync.WaitGroup
			others.Add(3)
			go func() { // forget, as eviction would, so prewarms find no entry
				defer others.Done()
				for {
					select {
					case <-done:
						return
					case <-time.After(100 * time.Microsecond):
					}
					db.ForgetMetadataCacheForTest()
				}
			}()
			go func() { // prewarm everything, repeat
				defer others.Done()
				tick := time.NewTicker(100 * time.Microsecond)
				defer tick.Stop()
				for {
					select {
					case <-done:
						return
					case <-tick.C:
					}
					priority := slices.Values(names)
					if buckets {
						priority = nil
					}
					if _, err := db.PrewarmMetadata(ctx, priority, store.PrewarmOptions{Workers: 2, Batch: 2}); err != nil {
						fail("prewarm: %v", err)
						return
					}
					prewarms.Add(1)
				}
			}()
			go func() {
				defer others.Done()
				tick := time.NewTicker(100 * time.Microsecond)
				defer tick.Stop()
				for {
					select {
					case <-done:
						return
					case <-tick.C:
					}
					committed, committedLate := appended.Load(), late.Load()
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
					series, err = db.Fetch(ctx, "race.late", int(base), int(base)+appends/3+1)
					if err != nil {
						fail("fetch late: %v", err)
						return
					}
					for i := int64(1); i <= committedLate; i++ {
						if v := series.Values[(base+i-int64(series.FromTime))/int64(series.Step)]; v != float64(i) {
							fail("late point %d committed before the fetch began is missing: %v", i, v)
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
			writers.Wait()
			close(done)
			others.Wait()
			if f := failure.Load(); f != nil {
				t.Fatal(f)
			}
			s := db.Stats().StateCache
			t.Logf("prewarms %d: %+v", prewarms.Load(), s)
			if prewarms.Load() < 10 || s.Prewarmed == 0 || s.PrewarmSkipped == 0 {
				t.Fatalf("too few prewarms (%d): %+v", prewarms.Load(), s)
			}
			// Whatever the last prewarm and writes left must match the store.
			now := time.Unix(base+3600, 0)
			checkCachedReads(t, db, names, now, "after the race")
			// And every append and late point is there.
			series, err := db.Fetch(ctx, "race.appended", int(base), int(base)+appends+1)
			if err != nil {
				t.Fatal(err)
			}
			for i := int64(1); i <= appends; i++ {
				if v := series.Values[(base+i-int64(series.FromTime))/int64(series.Step)]; v != float64(i) {
					t.Fatalf("append %d lost: %v", i, v)
				}
			}
		})
	}
}

// Compare the preloaded raw keys and complete-prefix proofs with the original
// loader that opened Whisper files. No fixture body is copied into the cache.
type prewarmReadTrace struct {
	store.ContinuationIO
	buckets bool
	reads   map[string][]byte
}

func (t *prewarmReadTrace) TimeBuckets() bool { return t.buckets }
func (t *prewarmReadTrace) Get(key []byte) ([]byte, error) {
	value, err := t.ContinuationIO.Get(key)
	t.reads["get:"+string(key)] = bytes.Clone(value)
	return value, err
}
func (t *prewarmReadTrace) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	t.reads["walk:"+string(prefix)] = nil
	return t.ContinuationIO.Visit(prefix, func(key, value []byte) error {
		t.reads["visit:"+string(key)] = bytes.Clone(value)
		return visit(key, value)
	})
}

func TestPrewarmRawStateMatchesOpen(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, sidecar := range []bool{false, true} {
			t.Run(fmt.Sprintf("buckets=%v/sidecar=%v", buckets, sidecar), func(t *testing.T) {
				ctx := context.Background()
				clock := func() time.Time { return time.Unix(1_790_000_180, 0) }
				kv := &appendTailKV{data: make(map[string][]byte)}
				seed := &prewarmReadTrace{ContinuationIO: kv, buckets: buckets, reads: make(map[string][]byte)}
				codec := &ContinuationCodec{}
				meta := store.Metadata{MetricConfig: prewarmConfig("raw.state")}
				if err := codec.Create(ctx, seed, meta, clock); err != nil {
					t.Fatal(err)
				}
				input := make([]store.Point, 30)
				for i := range input {
					input[i] = store.Point{Timestamp: 1_790_000_120 + int64(i), Value: float64(i)}
				}
				if err := codec.Update(ctx, seed, meta, input, -1, clock, nil); err != nil {
					t.Fatal(err)
				}
				if sidecar {
					if err := codec.Update(ctx, seed, meta, []store.Point{{Timestamp: 1_790_000_005, Value: 1}}, -1, clock, nil); err != nil {
						t.Fatal(err)
					}
					if _, ok := kv.data[string(continuationNameKey(filepath.Base(whisper.OutOfOrderSidecarPath("main.wsp"))))]; !ok {
						t.Fatal("fixture lacks OOO sidecar")
					}
				}
				original := kv.clone()
				raw := &prewarmReadTrace{ContinuationIO: kv, buckets: buckets, reads: make(map[string][]byte)}
				opened := &prewarmReadTrace{ContinuationIO: original, buckets: buckets, reads: make(map[string][]byte)}
				if err := codec.LoadState(ctx, raw, meta, clock); err != nil {
					t.Fatal(err)
				}
				fs := newContinuationFiles(opened, true).withClock(clock)
				if err := fs.prefetchNames(); err != nil {
					t.Fatal(err)
				}
				w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
				if err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(raw.reads, opened.reads) {
					t.Fatalf("prewarm raw reads differ from open: %v / %v", raw.reads, opened.reads)
				}
				if !reflect.DeepEqual(kv.data, original.data) {
					t.Fatal("prewarm changed stored data")
				}
			})
		}
	}
}

func BenchmarkPrewarmStateDecode(b *testing.B) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1_790_000_180, 0) }
	codec := &ContinuationCodec{}
	kv := &appendTailKV{data: make(map[string][]byte)}
	meta := store.Metadata{MetricConfig: prewarmConfig("decode.bench")}
	if err := codec.Create(ctx, kv, meta, clock); err != nil {
		b.Fatal(err)
	}
	if err := codec.Update(ctx, kv, meta, []store.Point{{Timestamp: 1_790_000_120, Value: 1}}, -1, clock, nil); err != nil {
		b.Fatal(err)
	}
	b.Run("opened", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			fs := newContinuationFiles(kv, true).withClock(clock)
			if err := fs.prefetchNames(); err != nil {
				b.Fatal(err)
			}
			w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
			if err != nil {
				b.Fatal(err)
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := codec.LoadState(ctx, kv, meta, clock); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// A restart restores the same continuation names/manifests/headers as a live
// cache. The first read validates its provisional state before using it.
func TestMetadataCheckpointContinuation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Unix(1790000000, 0)
	opts := store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, TimeBuckets: true, MetadataCacheSize: 4 << 20, MetadataCacheCheckpoint: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour, Now: func() time.Time { return now }}
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const name = "checkpoint.continuation"
	if _, err := db.Create(ctx, prewarmConfig(name)); err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		if err := db.UpdateMany(ctx, name, []store.Point{{Timestamp: now.Unix() - int64(i*2), Value: float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, err := db.LoadMetadataCheckpoint(ctx, nil)
	if err != nil || !stats.Valid || stats.Entries != 1 || stats.Validated != 1 {
		t.Fatal(stats, err)
	}
	checkCachedReads(t, db, []string{name}, now, "provisional")
	if err := db.UpdateMany(ctx, name, []store.Point{{Timestamp: now.Unix(), Value: 42}, {Timestamp: now.Unix() - 80, Value: .5}}); err != nil {
		t.Fatal(err)
	}
	checkCachedReads(t, db, []string{name}, now, "after update")
	series, err := db.Fetch(ctx, name, int(now.Unix()-200), int(now.Unix()))
	if err != nil || series == nil {
		t.Fatal(series, err)
	}
}
