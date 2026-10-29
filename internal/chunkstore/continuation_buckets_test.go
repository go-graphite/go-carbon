package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// bucketCodec stands in for the whisperio codec: each metric gets one
// metric-prefixed key and keys in two time buckets, which Drop removes.
type bucketCodec struct{ ContinuationCodec }

func (bucketCodec) Create(_ context.Context, io ContinuationIO, _ Metadata, _ func() time.Time) error {
	for _, k := range [][]byte{[]byte("plain"), ContinuationBucketKey(7, []byte("g")), ContinuationBucketKey(9, []byte("g"))} {
		if err := io.Set(k, []byte("v")); err != nil {
			return err
		}
	}
	return nil
}

func (bucketCodec) Drop(_ context.Context, io ContinuationIO) error {
	for _, bucket := range []uint32{7, 9} {
		if err := io.DeletePrefix(ContinuationBucketKey(bucket, nil)); err != nil {
			return err
		}
	}
	return nil
}

func countStoreKeys(t *testing.T, s *Store) int {
	t.Helper()
	const first = 't'
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{first}, UpperBound: []byte{first + 1}})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		t.Fatal(err)
	}
	return n
}

// Bucketed suffixes live in the 't' space but stay scoped to their generation:
// reads, visits and prefix deletes never reach another metric's keys in the
// same bucket, and visits return suffix form.
func TestContinuationBucketKeysStayInTheirGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, s)
	m, other := Metadata{ID: 5, Generation: 2}, Metadata{ID: 6, Generation: 1}
	b := s.db.NewIndexedBatch()
	io, otherIO := newContinuationIO(ctx, b, b, m), newContinuationIO(ctx, b, b, other)
	for _, w := range []struct {
		io    *continuationIO
		key   []byte
		value string
	}{
		{io, ContinuationBucketKey(100, []byte("f1")), "a"},
		{io, ContinuationBucketKey(101, []byte("f1")), "b"},
		{io, []byte("plain"), "c"},
		{otherIO, ContinuationBucketKey(100, []byte("f1")), "d"},
	} {
		if err := w.io.Set(w.key, []byte(w.value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	raw := append(append([]byte{'t', 0, 0, 0, 100}, continuationPrefix(5, 2)...), "f1"...)
	if v, closer, err := s.db.Get(raw); err != nil || string(v) != "a" {
		t.Fatalf("bucketed key not stored at %x: %q %v", raw, v, err)
	} else {
		closer.Close()
	}
	if n := countStoreKeys(t, s); n != 3 {
		t.Fatalf("time-partitioned keys = %d, want 3", n)
	}
	visit := func(io *continuationIO, prefix []byte) map[string]string {
		t.Helper()
		got := map[string]string{}
		if err := io.Visit(prefix, func(k, v []byte) error { got[string(k)] = string(v); return nil }); err != nil {
			t.Fatal(err)
		}
		return got
	}
	committed := newContinuationIO(ctx, s.db, nil, m)
	if got, want := visit(committed, ContinuationBucketKey(100, nil)), map[string]string{string(ContinuationBucketKey(100, []byte("f1"))): "a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("visit bucket 100 = %q, want %q", got, want)
	}
	if v, err := committed.Get(ContinuationBucketKey(101, []byte("f1"))); err != nil || string(v) != "b" {
		t.Fatalf("get bucketed key: %q %v", v, err)
	}
	b = s.db.NewIndexedBatch()
	if err := newContinuationIO(ctx, b, b, m).DeletePrefix(ContinuationBucketKey(100, nil)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := committed.Get(ContinuationBucketKey(100, []byte("f1"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted bucketed key: %v", err)
	}
	if v, err := newContinuationIO(ctx, s.db, nil, other).Get(ContinuationBucketKey(100, []byte("f1"))); err != nil || string(v) != "d" {
		t.Fatalf("other generation's key in the same bucket: %q %v", v, err)
	}
	// A fresh stage keeps bucketed keys in its batch view, in suffix form.
	fresh := newFreshContinuationIO(ctx, s.db.NewIndexedBatch(), Metadata{ID: 9, Generation: 1})
	if err := fresh.Set(ContinuationBucketKey(3, []byte("g")), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if got := visit(fresh, nil); !reflect.DeepEqual(got, map[string]string{string(ContinuationBucketKey(3, []byte("g"))): "x"}) {
		t.Fatalf("fresh visit = %q", got)
	}
}

// Enabling time buckets upgrades a v6 marker in place and is one-way; the
// store then refuses modes without the continuation codec.
func TestTimeBucketsUpgradeMarker(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "store")
	options := Options{ExactBlocks: true, Continuation: bucketCodec{}}
	s, _ := openTestStore(t, dir, options)
	if s.timeBuckets {
		t.Fatal("new store without the option uses time buckets")
	}
	closeTestStore(t, s)
	if format, err := checkFormatMarker(vfs.Default, dir); err != nil || format != 6 {
		t.Fatalf("marker %d %v, want 6", format, err)
	}
	options.TimeBuckets = true
	s, _ = openTestStore(t, dir, options)
	if !s.timeBuckets {
		t.Fatal("option did not enable time buckets")
	}
	closeTestStore(t, s)
	if format, err := checkFormatMarker(vfs.Default, dir); err != nil || format != 7 {
		t.Fatalf("marker %d %v, want 7", format, err)
	}
	options.TimeBuckets = false
	s, _ = openTestStore(t, dir, options)
	if !s.timeBuckets {
		t.Fatal("a v7 store stopped writing time-bucketed blocks")
	}
	closeTestStore(t, s)
	if _, err := Open(dir, Options{ExactBlocks: true}); !errors.Is(err, ErrFormat) {
		t.Fatalf("v7 store opened without the continuation codec: %v", err)
	}
}

// Deleting a metric removes its time-partitioned keys and only its own.
func TestDeleteMetricDropsTimeBuckets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openTestStore(t, filepath.Join(t.TempDir(), "store"), Options{ExactBlocks: true, Continuation: bucketCodec{}, TimeBuckets: true})
	defer closeTestStore(t, s)
	createTestMetric(t, s, "a")
	createTestMetric(t, s, "b")
	if n := countStoreKeys(t, s); n != 4 {
		t.Fatalf("time-partitioned keys = %d, want 4", n)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if n := countStoreKeys(t, s); n != 2 {
		t.Fatalf("after deleting a: %d time-partitioned keys, want 2", n)
	}
	if err := s.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if n := countStoreKeys(t, s); n != 0 {
		t.Fatalf("after deleting both: %d time-partitioned keys", n)
	}
}

// relayoutCodec has one v6-style block per metric: descriptor "b" and group
// "c", which the plan copies to the history bucket.
type relayoutCodec struct{ bucketCodec }

func (relayoutCodec) Create(_ context.Context, io ContinuationIO, _ Metadata, _ func() time.Time) error {
	if err := io.Set([]byte("b"), []byte("v6")); err != nil {
		return err
	}
	return io.Set([]byte("c"), []byte("group"))
}

func (relayoutCodec) RelayoutPlan(_ context.Context, io ContinuationIO) (RelayoutPlan, error) {
	old, err := io.Get([]byte("b"))
	if err != nil || string(old) != "v6" {
		return RelayoutPlan{}, err
	}
	value, err := io.Get([]byte("c"))
	if err != nil {
		return RelayoutPlan{}, err
	}
	return RelayoutPlan{Bytes: len(value), Blocks: []RelayoutBlock{{Descriptor: []byte("b"), Old: old, New: []byte("v7"),
		Copies: []RelayoutKV{{Key: ContinuationBucketKey(0, []byte("c")), Value: value}}, Removed: [][]byte{[]byte("c")}}}}, nil
}

// A block that changes between planning and its switch keeps its new state,
// and the copies made for it are deleted; unchanged blocks switch.
func TestRelayoutSkipsChangedBlocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openTestStore(t, filepath.Join(t.TempDir(), "store"), Options{ExactBlocks: true, Continuation: relayoutCodec{}, TimeBuckets: true})
	defer closeTestStore(t, s)
	kept, changed := createTestMetric(t, s, "kept"), createTestMetric(t, s, "changed")
	var items []relayoutItem
	for _, m := range []Metadata{kept, changed} {
		plan, err := relayoutCodec{}.RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, m))
		if err != nil || len(plan.Blocks) != 1 {
			t.Fatalf("plan %s: %+v %v", m.Name, plan, err)
		}
		items = append(items, relayoutItem{m: m, plan: plan})
	}
	// An update rewrites the second block after planning.
	b := s.db.NewBatch()
	if err := s.newContinuationIO(ctx, s.db, b, changed).Set([]byte("b"), []byte("rewritten")); err != nil {
		t.Fatal(err)
	}
	if err := s.commit(b); err != nil {
		t.Fatal(err)
	}
	if err := s.relayoutGroup(ctx, items); err != nil {
		t.Fatal(err)
	}
	get := func(m Metadata, key []byte) string {
		v, err := s.newContinuationIO(ctx, s.db, nil, m).Get(key)
		if errors.Is(err, ErrNotFound) {
			return "<none>"
		}
		if err != nil {
			t.Fatal(err)
		}
		return string(v)
	}
	copyKey := ContinuationBucketKey(0, []byte("c"))
	if got := []string{get(kept, []byte("b")), get(kept, []byte("c")), get(kept, copyKey)}; !reflect.DeepEqual(got, []string{"v7", "<none>", "group"}) {
		t.Fatalf("unchanged block after relayout: %q", got)
	}
	if got := []string{get(changed, []byte("b")), get(changed, []byte("c")), get(changed, copyKey)}; !reflect.DeepEqual(got, []string{"rewritten", "group", "<none>"}) {
		t.Fatalf("changed block after relayout: %q", got)
	}
	if st := s.RelayoutStats(); st.Metrics != 1 || st.Blocks != 1 || st.Skipped != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// A rate raised while a relayout waits applies within the recheck interval,
// and time spent copying slowly earns no burst afterwards.
func TestRelayoutPacingFollowsRateChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Store{}
		if err := s.SetRelayoutRate(0); err == nil {
			t.Fatal("accepted a zero relayout rate")
		}
		s.relayout.rate.Store(1)
		s.relayout.bytes.Store(1 << 20)
		paced, pacedAt := uint64(0), time.Now()
		done := make(chan error, 1)
		go func() { done <- s.paceRelayout(context.Background(), &paced, &pacedAt) }()
		synctest.Wait()
		if err := s.SetRelayoutRate(1 << 40); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("raised rate did not end the wait")
		}
		if paced != 1<<20 {
			t.Fatalf("paced %d bytes, want %d", paced, 1<<20)
		}
		pacedAt = time.Now().Add(-time.Hour)
		s.relayout.rate.Store(1 << 20)
		s.relayout.bytes.Add(1 << 20)
		start := time.Now()
		if err := s.paceRelayout(context.Background(), &paced, &pacedAt); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > 100*time.Millisecond {
			t.Fatal("slow copying should not wait")
		}
		s.relayout.bytes.Add(1 << 20)
		start = time.Now()
		if err := s.paceRelayout(context.Background(), &paced, &pacedAt); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited < 900*time.Millisecond {
			t.Fatalf("waited %v after a slow interval, want about 1s", waited)
		}
	})
}

// A v6 store stays v6 when the upgrading open fails: another process holds
// the store, or Pebble rejects the options.
func TestTimeBucketsUpgradeNeedsSuccessfulOpen(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "store")
	options := Options{ExactBlocks: true, Continuation: bucketCodec{}}
	holder, _ := openTestStore(t, dir, options)
	upgrade := options
	upgrade.TimeBuckets = true
	if s, err := Open(dir, upgrade); err == nil {
		closeTestStore(t, s)
		t.Fatal("second opener acquired a held store")
	}
	if format, err := checkFormatMarker(vfs.Default, dir); err != nil || format != 6 {
		t.Fatalf("marker %d %v after a locked-out upgrade, want 6", format, err)
	}
	closeTestStore(t, holder)
	invalid := upgrade
	invalid.MemTableSize = 8 << 30
	if s, err := Open(dir, invalid); err == nil {
		closeTestStore(t, s)
		t.Fatal("invalid memtable size accepted")
	}
	if format, err := checkFormatMarker(vfs.Default, dir); err != nil || format != 6 {
		t.Fatalf("marker %d %v after a rejected upgrade, want 6", format, err)
	}
	s, _ := openTestStore(t, dir, upgrade)
	closeTestStore(t, s)
	if format, err := checkFormatMarker(vfs.Default, dir); err != nil || format != 7 {
		t.Fatalf("marker %d %v after a successful upgrade, want 7", format, err)
	}
}

// Only one relayout runs at a time.
func TestRelayoutRejectsConcurrentRun(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, filepath.Join(t.TempDir(), "store"), Options{ExactBlocks: true, Continuation: bucketCodec{}, TimeBuckets: true})
	defer closeTestStore(t, s)
	s.relayout.running.Store(true)
	if err := s.RelayoutContinuation(context.Background(), 0); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("concurrent relayout admitted: %v", err)
	}
	s.relayout.running.Store(false)
}

// Tables written with bloom filters carry them and point reads still work.
func TestBloomFilterTables(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, filepath.Join(t.TempDir(), "store"), Options{ExactBlocks: true, Continuation: bucketCodec{}, TimeBuckets: true, BloomFilterBits: 10})
	defer closeTestStore(t, s)
	createTestMetric(t, s, "bloom.a")
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	tables, err := s.db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, level := range tables {
		for _, table := range level {
			if table.Properties.FilterPolicyName != "rocksdb.BuiltinBloomFilter" {
				t.Fatalf("table %s filter %q", table.FileNum, table.Properties.FilterPolicyName)
			}
			found++
		}
	}
	if found == 0 {
		t.Fatal("no tables after flush")
	}
	if _, err := s.Metadata(context.Background(), "bloom.a"); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.TempDir(), Options{BloomFilterBits: 33}); err == nil {
		t.Fatal("accepted 33 bloom bits per key")
	}
}

// Deleting a prefix with more keys than the old 1024 limit uses point deletes,
// leaving no range tombstone in the memtable.
func TestContinuationLargePrefixDeleteUsesPointDeletes(t *testing.T) {
	s, _ := openTestStore(t, filepath.Join(t.TempDir(), "store"), Options{ExactBlocks: true, Continuation: bucketCodec{}, TimeBuckets: true})
	defer closeTestStore(t, s)
	m := createTestMetric(t, s, "large.prefix")
	batch := s.db.NewIndexedBatch()
	io := s.newContinuationIO(context.Background(), batch, batch, m)
	for i := range 3000 {
		if err := io.Set([]byte(fmt.Sprintf("f%05d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	before := continuationRangeDeletes.Load()
	batch = s.db.NewIndexedBatch()
	defer batch.Close()
	io = s.newContinuationIO(context.Background(), batch, batch, m)
	if err := io.DeletePrefix([]byte("f")); err != nil {
		t.Fatal(err)
	}
	if got := continuationRangeDeletes.Load() - before; got != 0 {
		t.Fatalf("range tombstones written: %d", got)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	left := 0
	reader := s.newContinuationIO(context.Background(), s.db, nil, m)
	if err := reader.Visit([]byte("f"), func([]byte, []byte) error { left++; return nil }); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("%d keys survived the prefix delete", left)
	}
}

// markerReadFailFS fails the second open of the format marker only.
type markerReadFailFS struct {
	vfs.FS
	opens atomic.Int32
}

func (fs *markerReadFailFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if filepath.Base(name) == "CHUNKSTORE" && fs.opens.Add(1) == 2 {
		return nil, errors.New("injected marker read failure")
	}
	return fs.FS.Open(name, opts...)
}

// A failed re-read of the marker during an upgrading open must never leave a
// store writing v7 keys under a v6 marker.
func TestTimeBucketsUpgradeNeverRunsV7UnderV6Marker(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "store")
	options := Options{ExactBlocks: true, Continuation: bucketCodec{}}
	s, _ := openTestStore(t, dir, options)
	closeTestStore(t, s)
	upgrade := options
	upgrade.TimeBuckets = true
	upgrade.fs = &markerReadFailFS{FS: vfs.Default}
	opened, err := Open(dir, upgrade)
	format, markerErr := checkFormatMarker(vfs.Default, dir)
	if markerErr != nil {
		t.Fatal(markerErr)
	}
	if err != nil {
		// No store runs. A failure after Pebble took the lock may leave the
		// upgraded marker; a failure before it must not.
		if format != 6 && format != 7 {
			t.Fatalf("failed open left marker %d", format)
		}
		return
	}
	defer closeTestStore(t, opened)
	if opened.timeBuckets && format != 7 {
		t.Fatalf("store writes v7 keys under marker %d", format)
	}
}
