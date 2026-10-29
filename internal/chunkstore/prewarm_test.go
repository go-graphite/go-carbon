package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestStateCachePrewarmRules(t *testing.T) {
	st := testState("x", 1)
	c := newStateCache(2*st.size*metricStripes, nil)
	const stripe = 5

	epoch := c.epoch(stripe)
	if got := c.prewarm(stripe, "a", epoch, testState("a", 1)); got != prewarmFilled {
		t.Fatalf("fill into free room: %v", got)
	}
	if c.get(stripe, "a", c.epoch(stripe)) == nil || !c.has(stripe, "a") {
		t.Fatal("prewarmed state not served")
	}
	// An entry, installed, filled or pending, is never replaced.
	if got := c.prewarm(stripe, "a", c.epoch(stripe), testState("a", 2)); got != prewarmCached {
		t.Fatalf("prewarm over an entry: %v", got)
	}
	c.beginMutation(stripe, "p")
	if got := c.prewarm(stripe, "p", c.epoch(stripe), testState("p", 3)); got != prewarmCached {
		t.Fatalf("prewarm over a pending mutation: %v", got)
	}
	c.endMutation(stripe, "p")
	// A mutation of any name of the shard since the view opened drops it.
	stale := c.epoch(stripe)
	c.beginMutation(stripe, "other")
	c.endMutation(stripe, "other")
	if got := c.prewarm(stripe, "b", stale, testState("b", 4)); got != prewarmRaced || c.has(stripe, "b") {
		t.Fatalf("prewarm after the shard's epoch moved: %v", got)
	}
	// It fills free room only: the shard holds two states, and neither goes.
	if got := c.prewarm(stripe, "b", c.epoch(stripe), testState("b", 4)); got != prewarmFilled {
		t.Fatalf("second fill: %v", got)
	}
	if got := c.prewarm(stripe, "c", c.epoch(stripe), testState("c", 5)); got != prewarmFull {
		t.Fatalf("fill of a full shard: %v", got)
	}
	if s := c.stats(); s.Evictions != 0 || c.get(stripe, "a", c.epoch(stripe)) == nil || c.get(stripe, "b", c.epoch(stripe)) == nil || c.has(stripe, "c") {
		t.Fatalf("prewarm evicted: %+v", s)
	}
}

// names yields n metric names, then stops.
func prewarmNames(prefix string, n int) iter.Seq[string] {
	return func(yield func(string) bool) {
		for i := range n {
			if !yield(fmt.Sprintf("%s.%06d", prefix, i)) {
				return
			}
		}
	}
}

func TestPrewarmMetadataLoadsCatalog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(dir, Options{MetadataCacheSize: 64 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	const n = 300
	for name := range prewarmNames("warm", n) {
		if _, err := db.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 100}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, Options{MetadataCacheSize: 64 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Absent names, and one cached by a read meanwhile, are skipped.
	if _, err := db.Metadata(ctx, "warm.000007"); err != nil {
		t.Fatal(err)
	}
	names := slices.Collect(prewarmNames("warm", n+20))
	stats, err := db.PrewarmMetadata(ctx, slices.Values(names), PrewarmOptions{Workers: 3, Batch: 7})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Names != n+20 || stats.Prewarmed != n-1 || stats.Cached != 1 || stats.Absent != 20 || stats.Failed != 0 || stats.Raced != 0 || stats.Bytes <= 0 {
		t.Fatalf("prewarm stats %+v", stats)
	}
	cs := db.Stats().StateCache
	if cs.Prewarmed != n-1 || cs.PrewarmSkipped != 21 || cs.PrewarmFailed != 0 || cs.Evictions != 0 {
		t.Fatalf("cache counters %+v", cs)
	}
	// Prewarmed metadata equals what the database holds.
	for _, name := range names[:n] {
		cached, err := db.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		restore := db.DisableMetadataCacheForTest()
		want, err := db.Metadata(ctx, name)
		restore()
		if err != nil || !reflect.DeepEqual(cached, want) {
			t.Fatalf("%s: prewarmed %+v, database %+v (%v)", name, cached, want, err)
		}
	}
	if s := db.Stats().StateCache; s.Misses != 1 {
		t.Fatalf("reads after the prewarm missed the cache: %+v", s)
	}
}

func TestPrewarmMetadataStopsWhenFull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := testState("filler.00000000", 1)
	db, err := Open(t.TempDir(), Options{MetadataCacheSize: 2 * st.size * metricStripes, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Nearly full already: nothing is read, and nothing evicted.
	c := db.stateCache
	for i := 0; c.bytes.Load() < int64(float64(c.budget())*prewarmFullFraction); i++ {
		if i == 1<<20 {
			t.Fatalf("cache filled only to %d of %d bytes", c.bytes.Load(), c.budget())
		}
		name := fmt.Sprintf("filler.%08d", i)
		stripe := db.metricStripe(name)
		c.fill(stripe, name, c.epoch(stripe), testState(name, uint64(i)))
	}
	evictions := c.evictions.Load()
	// Model catalog records loaded before the cache was enabled.
	restore := db.DisableMetadataCacheForTest()
	for name := range prewarmNames("warm", 50) {
		if _, err := db.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 100}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
	}
	restore()
	stats, err := db.PrewarmMetadata(ctx, prewarmNames("warm", 50), PrewarmOptions{})
	if err != nil || !stats.BudgetFull || stats.Names != 0 || stats.Prewarmed != 0 {
		t.Fatalf("prewarm of a full cache: %+v, %v", stats, err)
	}
	if c.evictions.Load() != evictions {
		t.Fatal("prewarm evicted")
	}
}

func TestPrewarmMetadataSkipsFullShards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := testState("warm.000000", 1)
	// Room for about one metric per shard.
	db, err := Open(t.TempDir(), Options{MetadataCacheSize: (st.size + st.size/2) * metricStripes, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const n = 6000
	restore := db.DisableMetadataCacheForTest()
	for name := range prewarmNames("warm", n) {
		if _, err := db.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 100}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
	}
	restore()
	db.ForgetMetadataCacheForTest()
	stats, err := db.PrewarmMetadata(ctx, prewarmNames("warm", n), PrewarmOptions{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	cs := db.Stats().StateCache
	if stats.Full == 0 || stats.Prewarmed == 0 || stats.Prewarmed+stats.Full+stats.Cached != stats.Names || cs.Evictions != 0 || cs.Bytes > db.stateCache.budget() {
		t.Fatalf("prewarm into full shards: %+v, cache %+v", stats, cs)
	}
}

// endless yields names that are not in the catalog until the consumer stops.
func endless(yield func(string) bool) {
	for i := 0; ; i++ {
		if !yield(fmt.Sprint("absent.", i)) {
			return
		}
	}
}

func TestPrewarmMetadataStopsPromptly(t *testing.T) {
	for _, how := range []string{"context", "StopBackground", "Close"} {
		t.Run(how, func(t *testing.T) {
			db, err := Open(t.TempDir(), Options{MetadataCacheSize: 64 << 20, SyncInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					db.Close()
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				stats PrewarmStats
				err   error
			}
			done := make(chan result, 1)
			// Paced at 1000 names/s, an endless prewarm would run forever.
			go func() {
				stats, err := db.PrewarmMetadata(ctx, endless, PrewarmOptions{Workers: 2, Rate: 1000, Batch: 4})
				done <- result{stats, err}
			}()
			time.Sleep(50 * time.Millisecond)
			stopped := time.Now()
			switch how {
			case "context":
				cancel()
			case "StopBackground":
				db.StopBackground()
			case "Close":
				closed = true
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case r := <-done:
				if !errors.Is(r.err, context.Canceled) || r.stats.Names == 0 {
					t.Fatalf("stopped prewarm: %+v, %v", r.stats, r.err)
				}
				if waited := time.Since(stopped); waited > time.Second {
					t.Fatalf("prewarm took %v to stop", waited)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("prewarm did not stop")
			}
			if how != "context" {
				// A stopped store starts no prewarm.
				if _, err := db.PrewarmMetadata(context.Background(), prewarmNames("late", 3), PrewarmOptions{}); !errors.Is(err, context.Canceled) {
					t.Fatalf("prewarm after stop: %v", err)
				}
			}
		})
	}
}

func TestPrewarmMetadataPacesReads(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{MetadataCacheSize: 64 << 20, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	started := time.Now()
	stats, err := db.PrewarmMetadata(context.Background(), prewarmNames("absent", 100), PrewarmOptions{Rate: 400, Batch: 10})
	if err != nil || stats.Absent != 100 {
		t.Fatalf("paced prewarm: %+v, %v", stats, err)
	}
	// 100 names at 400/s, in batches of 10: the pause after the last batch
	// ends at 250 ms.
	if elapsed := time.Since(started); elapsed < 200*time.Millisecond {
		t.Fatalf("100 names at 400/s took %v", elapsed)
	}
}

func TestPrewarmMetadataNeedsCache(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.MetadataCacheEnabledForTest() {
		t.Skip("GO_CARBON_TEST_METADATA_CACHE enables the cache")
	}
	if _, err := db.PrewarmMetadata(context.Background(), prewarmNames("x", 1), PrewarmOptions{}); !errors.Is(err, ErrMetadataCacheDisabled) {
		t.Fatalf("prewarm without a cache: %v", err)
	}
}
