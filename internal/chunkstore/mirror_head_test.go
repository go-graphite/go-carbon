package chunkstore

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestSingleArchiveCorrectionsEligibility(t *testing.T) {
	m := Metadata{MetricConfig: MetricConfig{Retentions: []Retention{{Step: 2, Count: 256}}}}
	values := []ArchiveValue{{Timestamp: 800, Value: 0, Present: true}, {Timestamp: 802, Value: -1, Present: true}}
	for _, tt := range []struct {
		name   string
		input  []Point
		values []ArchiveValue
		want   bool
	}{
		{"complete", []Point{{Timestamp: 801}, {Timestamp: 803}}, values, true},
		{"corrections only", nil, values, true},
		{"missing input correction", []Point{{Timestamp: 804}}, values, false},
		{"empty correction", nil, nil, false},
		{"absence", nil, []ArchiveValue{{Timestamp: 800}}, false},
		{"two chunks", nil, []ArchiveValue{{Timestamp: 800, Present: true}, {Timestamp: 1024, Present: true}}, false},
		{"circular owner", []Point{{Timestamp: 288}}, values, false},
		{"epoch", []Point{{Timestamp: 0}}, values, false},
		{"negative", []Point{{Timestamp: -1}}, values, false},
		{"overflow", []Point{{Timestamp: math.MaxInt64}}, values, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, got := singleArchiveCorrections(m, tt.input, tt.values)
			if got != tt.want {
				t.Fatalf("eligible = %v, want %v", got, tt.want)
			}
		})
	}
	m.Retentions = append(m.Retentions, Retention{Step: 60, Count: 256})
	if _, ok := singleArchiveCorrections(m, nil, values); ok {
		t.Fatal("rollup policy accepted")
	}
}

func TestCanonicalChunkMatchesReferenceAcrossReopenAndOOO(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	opts := Options{ExactBlocks: true, fs: vfs.NewMem()}
	dirs := []string{filepath.Join(t.TempDir(), "fast"), filepath.Join(t.TempDir(), "reference")}
	fast, now := openTestStore(t, dirs[0], opts)
	reference, _ := openTestStore(t, dirs[1], opts)
	defer func() { closeTestStore(t, fast); closeTestStore(t, reference) }()
	config := MetricConfig{Name: "mirror", Retentions: []Retention{{Step: 1, Count: 256}}, AggregationMethod: Last}
	for _, db := range []*Store{fast, reference} {
		if _, err := db.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
		// Preserve an untouched owner when the optimized write emits a delta.
		if err := db.UpdateManyForArchive(ctx, config.Name, []Point{{Timestamp: int64(now - 5), Value: 123}}, 256); err != nil {
			t.Fatal(err)
		}
	}
	bits := []uint64{0, 1 << 63, math.Float64bits(17), math.Float64bits(1.25), 0x7ff8000000001234, math.Float64bits(math.Inf(-1))}
	base := int64(now / 128 * 128)
	for i := range 48 {
		ts := base + int64(10+i%6)
		value := math.Float64frombits(bits[i%len(bits)])
		input := []Point{{Timestamp: ts, Value: 999}, {Timestamp: ts - 1, Value: -999}}
		values := []ArchiveValue{{Timestamp: ts - 1, Value: -value, Present: true}, {Timestamp: ts, Value: value, Present: true}, {Timestamp: ts, Value: value, Present: true}}
		if err := fast.UpdateManyWithArchiveValues(ctx, config.Name, input, values); err != nil {
			t.Fatal(err)
		}
		unlock := reference.lockMetricMutation(config.Name)
		m, err := reference.metadataLocked(config.Name)
		if err != nil {
			unlock()
			t.Fatal(err)
		}
		err = reference.updateArchiveValues(m, input, values, now)
		unlock()
		if err != nil {
			t.Fatal(err)
		}
		assertMirrorStoresEqual(t, fast, reference, config.Name)
		got, err := fast.Snapshot(ctx, config.Name)
		if err != nil {
			t.Fatal(err)
		}
		slot := (int(ts) % config.Retentions[0].Count) / chunkSlots
		v, closer, err := fast.db.Get(chunkKey(got.Metadata, 0, slot))
		if err != nil {
			t.Fatal(err)
		}
		var c chunk
		err = decodeChunk(v, &c)
		closer.Close()
		if err != nil || c.Deltas >= deltasBeforeSet {
			t.Fatalf("unbounded canonical chain: %d, %v", c.Deltas, err)
		}

		if i%4 == 3 {
			for _, db := range []*Store{fast, reference} {
				if err := db.Compact(); err != nil {
					t.Fatal(err)
				}
				closeTestStore(t, db)
			}
			fast, _ = openTestStore(t, dirs[0], opts)
			reference, _ = openTestStore(t, dirs[1], opts)
			assertMirrorStoresEqual(t, fast, reference, config.Name)
		}
	}
}

func assertMirrorStoresEqual(t *testing.T, fast, reference *Store, name string) {
	t.Helper()
	want, err := reference.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fast.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err := EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	if got.Metadata.Revision != want.Metadata.Revision || !got.Metadata.LastUpdate.Equal(want.Metadata.LastUpdate) {
		t.Fatalf("optimized path changed revision/activity: got %d/%v want %d/%v", got.Metadata.Revision, got.Metadata.LastUpdate, want.Metadata.Revision, want.Metadata.LastUpdate)
	}
}

func TestCanonicalRunInvalidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, db)
	m := createTestMetric(t, db, "mirror")
	ts := int64(now/60*60 - 60)
	values := []ArchiveValue{{Timestamp: ts, Value: 1, Present: true}}
	put := func() {
		t.Helper()
		if err := db.UpdateManyWithArchiveValues(ctx, m.Name, nil, values); err != nil {
			t.Fatal(err)
		}
	}
	put()
	put()
	if !db.canonicalRun(m.ID).Contains(m.ID) {
		t.Fatal("canonical run was not recorded")
	}
	if err := db.UpdateMany(ctx, m.Name, []Point{{Timestamp: ts - 60, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	if db.canonicalRun(m.ID).Contains(m.ID) {
		t.Fatal("ordinary mutation retained permission")
	}
	before := db.Stats().Materializations
	put()
	if db.Stats().Materializations != before+1 {
		t.Fatal("invalidated run did not materialize")
	}
	db.canonicalRun(m.ID).Clear(m.ID)
	before = db.Stats().Materializations
	put()
	if db.Stats().Materializations != before+1 {
		t.Fatal("evicted run did not materialize")
	}
	snapshot, err := db.Snapshot(ctx, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 2 {
		t.Fatal("lost untouched point")
	}
	snapshot.Archives[0].Points[0].Value = 9
	if _, err = db.ReconcileSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if db.canonicalRun(m.ID).Contains(m.ID) {
		t.Fatal("reconciliation retained permission")
	}
	put()
	if _, err = db.Replace(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if db.canonicalRun(m.ID).Contains(m.ID) {
		t.Fatal("replacement retained permission")
	}
	put()
	if err = db.Delete(ctx, m.Name); err != nil {
		t.Fatal(err)
	}
	if db.canonicalRun(m.ID).Contains(m.ID) {
		t.Fatal("delete retained permission")
	}
}

func TestCanonicalRunColdLimitAndClockRollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := time.Unix(100_000, 0)
	db, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Now: func() time.Time { return clock }})
	defer closeTestStore(t, db)
	m, err := db.Create(ctx, MetricConfig{Name: "mirror", Retentions: []Retention{{Step: 1, Count: 256}}, AggregationMethod: Last})
	if err != nil {
		t.Fatal(err)
	}
	base := clock.Unix() / 128 * 128
	var cold chunk
	cold.Deltas = materializeAt - 1
	cold.set(20, Point{Timestamp: base + 20, Value: 17})
	index := int(base%256) / chunkSlots
	if err := db.db.Set(chunkKey(m, 0, index), db.encodeChunk(&cold), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	put := func() {
		t.Helper()
		ts := clock.Unix()/128*128 + 10
		if err := db.UpdateManyWithArchiveValues(ctx, m.Name, nil, []ArchiveValue{{Timestamp: ts, Value: 3, Present: true}}); err != nil {
			t.Fatal(err)
		}
	}
	put()
	put()
	snapshot, err := db.Snapshot(ctx, m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Archives[0].Points) != 2 {
		t.Fatal("cold materialization lost an untouched point")
	}
	clock = clock.Add(-129 * time.Second)
	for range 5 {
		put()
		if db.canonicalRun(m.ID).Contains(m.ID) {
			t.Fatal("future activity timestamp authorized blind writes")
		}
	}
	clock = time.Unix(100_001, 0)
	before := db.Stats().Materializations
	put()
	if db.Stats().Materializations != before+1 {
		t.Fatal("clock recovery skipped materialization")
	}
}

func TestCanonicalRunBitmapIsBounded(t *testing.T) {
	var set IDSet
	for i := uint64(1); i <= 20; i++ {
		id := i<<16 | 1
		set.markBounded(id, 16)
		if len(set.pages) > 16 || !set.Contains(id) {
			t.Fatal("bounded membership failed")
		}
		if set.Contains(id + 1) {
			t.Fatal("false positive membership")
		}
		set.Clear(id)
		if set.Contains(id) {
			t.Fatal("clear retained membership")
		}
	}
	var absent *IDSet
	absent.Clear(1)
}

func TestCanonicalChunkCrashAtomicity(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(fmt.Sprint(durable), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				strict := vfs.NewStrictMem()
				db, now := openTestStore(t, "/store", Options{ExactBlocks: true, fs: strict, SyncInterval: time.Second})
				m := createTestMetric(t, db, "mirror")
				time.Sleep(time.Second)
				synctest.Wait()
				for i := range 3 {
					if err := db.UpdateManyWithArchiveValues(context.Background(), m.Name, nil, []ArchiveValue{{Timestamp: int64(now/60*60 - 60), Value: float64(i), Present: true}}); err != nil {
						t.Fatal(err)
					}
				}
				if durable {
					time.Sleep(time.Second)
					synctest.Wait()
				}
				strict.SetIgnoreSyncs(true)
				closeTestStore(t, db)
				strict.ResetToSyncedState()
				strict.SetIgnoreSyncs(false)
				db, _ = openTestStore(t, "/store", Options{ExactBlocks: true, fs: strict})
				defer closeTestStore(t, db)
				snapshot, err := db.Snapshot(context.Background(), m.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := 0
				if durable {
					want = 1
				}
				if len(snapshot.Archives[0].Points) != want {
					t.Fatal("partial WAL recovery")
				}
				if durable && snapshot.Archives[0].Points[0].Value != 2 {
					t.Fatal("lost last durable value")
				}
				if !durable && snapshot.Metadata.Revision != m.Revision {
					t.Fatal("revision survived without points")
				}
			})
		})
	}
}

func TestCanonicalChunkFallbackPreservesCircularOwners(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fast, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	reference, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, fast)
	defer closeTestStore(t, reference)
	config := MetricConfig{Name: "mirror", Retentions: []Retention{{Step: 1, Count: 256}}, AggregationMethod: Last}
	for _, db := range []*Store{fast, reference} {
		if _, err := db.Create(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	base := int64(now / 128 * 128)
	batches := []struct {
		input  []Point
		values []ArchiveValue
	}{
		{nil, []ArchiveValue{{Timestamp: base + 10, Value: 1, Present: true}}},
		{nil, []ArchiveValue{{Timestamp: base + 11, Value: 2, Present: true}}},
		// A circular owner arrives out of order; its old timestamp's absence
		// must not remove the newer owner of that physical slot.
		{nil, []ArchiveValue{{Timestamp: base + 266, Value: 3, Present: true}, {Timestamp: base + 10, Present: false}}},
		// Removing a point after blind merges must not leave an old operand
		// that compaction can subsequently resurrect.
		{nil, []ArchiveValue{{Timestamp: base + 11, Present: false}}},
		// An input without a correction still takes the ordinary update path.
		{[]Point{{Timestamp: base + 12, Value: 4}}, []ArchiveValue{{Timestamp: base + 13, Value: 5, Present: true}}},
		{nil, []ArchiveValue{{Timestamp: base + 20, Value: 6, Present: true}, {Timestamp: base + 140, Value: 7, Present: true}}},
	}
	for _, batch := range batches {
		if err := fast.UpdateManyWithArchiveValues(ctx, config.Name, batch.input, batch.values); err != nil {
			t.Fatal(err)
		}
		unlock := reference.lockMetricMutation(config.Name)
		m, err := reference.metadataLocked(config.Name)
		if err == nil {
			err = reference.updateArchiveValues(m, batch.input, batch.values, now)
		}
		unlock()
		if err != nil {
			t.Fatal(err)
		}
		assertMirrorStoresEqual(t, fast, reference, config.Name)
	}
	for _, db := range []*Store{fast, reference} {
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	assertMirrorStoresEqual(t, fast, reference, config.Name)
}

func TestCanonicalChunkConcurrentAtomicReads(t *testing.T) {
	t.Parallel()
	db, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, SyncInterval: time.Second})
	defer closeTestStore(t, db)
	ctx := context.Background()
	_, err := db.Create(ctx, MetricConfig{Name: "mirror", Retentions: []Retention{{Step: 1, Count: 256}}, AggregationMethod: Last})
	if err != nil {
		t.Fatal(err)
	}
	base := int64(now/128*128 + 10)
	var group sync.WaitGroup
	for worker := range 4 {
		group.Go(func() {
			for i := range 100 {
				v := float64(worker*100 + i)
				values := []ArchiveValue{{Timestamp: base, Value: v, Present: true}, {Timestamp: base + 1, Value: -v, Present: true}}
				if err := db.UpdateManyWithArchiveValues(ctx, "mirror", nil, values); err != nil {
					t.Error(err)
					return
				}
				s, err := db.Snapshot(ctx, "mirror")
				if err != nil {
					t.Error(err)
					return
				}
				p := s.Archives[0].Points
				if len(p) != 2 || p[0].Value != -p[1].Value {
					t.Error("partial correction visible")
					return
				}
			}
		})
	}
	group.Wait()
}

// A cached single-metric diagnostic isolates lookup/workspace costs. Production
// corpus replay and the live canary still decide end-to-end performance.
func BenchmarkCanonicalChunk(b *testing.B) {
	for _, fast := range []bool{false, true} {
		b.Run(fmt.Sprint(fast), func(b *testing.B) {
			const now = 100_000
			db, err := Open("/bench", Options{ExactBlocks: true, fs: vfs.NewMem(), SyncInterval: time.Hour, Now: func() time.Time { return time.Unix(now, 0) }})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			if _, err := db.Create(ctx, MetricConfig{Name: "mirror", Retentions: []Retention{{Step: 1, Count: 256}}, AggregationMethod: Last}); err != nil {
				b.Fatal(err)
			}
			input := make([]Point, 8)
			values := make([]ArchiveValue, 8)
			for i := range input {
				ts := int64(now/128*128 + i)
				input[i] = Point{Timestamp: ts, Value: float64(i)}
				values[i] = ArchiveValue{Timestamp: ts, Value: float64(i), Present: true}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if fast {
					err = db.UpdateManyWithArchiveValues(ctx, "mirror", input, values)
				} else {
					unlock := db.lockMetricMutation("mirror")
					var m Metadata
					m, err = metadataFrom(db.db, "mirror")
					if err == nil {
						err = db.updateArchiveValues(m, input, values, now)
					}
					unlock()
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
