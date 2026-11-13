package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// metricKeys returns the chunk, continuation-state and time-bucket keys of
// every generation of id.
func metricKeys(t *testing.T, s *Store, id uint64) [][]byte {
	t.Helper()
	it, err := s.db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		var owner []byte
		switch {
		case (key[0] == 'c' || key[0] == 'u') && len(key) >= 9:
			owner = key[1:9]
		case key[0] == continuationBucketKeyPrefix && len(key) >= 14 && key[5] == 'u':
			owner = key[6:14]
		default:
			continue
		}
		if binary.BigEndian.Uint64(owner) == id {
			keys = append(keys, append([]byte(nil), key...))
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		t.Fatal(err)
	}
	return keys
}

func flushedRangeDeletions(t *testing.T, s *Store) uint64 {
	t.Helper()
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	tables, err := s.db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, level := range tables {
		for _, table := range level {
			n += table.Properties.NumRangeDeletions
		}
	}
	return n
}

// buckyd DELETE (Store.Delete and DeleteIfUnchanged) must not write range
// tombstones: each one makes every reader of the memtable re-fragment all of
// its tombstones under one lock. The metric and all of its data still go,
// across generations and time buckets, and the neighbour is untouched.
func TestAdministrativeDeleteWritesNoRangeTombstones(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		for _, conditional := range []bool{false, true} {
			t.Run(fmt.Sprintf("buckets=%v/conditional=%v", buckets, conditional), func(t *testing.T) {
				ctx := context.Background()
				opts := Options{fs: vfs.NewMem(), ExactBlocks: true, SyncInterval: time.Hour}
				if buckets {
					opts.Continuation, opts.TimeBuckets = bucketCodec{}, true
				}
				s, now := openTestStore(t, "/store", opts)
				defer closeTestStore(t, s)
				gone, keep := createTestMetric(t, s, "gone"), createTestMetric(t, s, "keep")
				batch := s.db.NewBatch()
				for _, m := range []Metadata{gone, keep} {
					var c chunk
					c.set(0, Point{Timestamp: int64(now), Value: 42})
					for _, generation := range []uint64{m.Generation, m.Generation + 1} {
						m.Generation = generation
						for _, index := range []int{0, 7, 31} {
							if err := batch.Set(chunkKey(m, 0, index), s.encodeChunk(&c), nil); err != nil {
								t.Fatal(err)
							}
						}
						if buckets {
							if err := batch.Set(append(continuationPrefix(m.ID, generation), "stale"...), []byte("state"), nil); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				if err := batch.Commit(pebble.Sync); err != nil {
					t.Fatal(err)
				}
				batch.Close()
				if n := flushedRangeDeletions(t, s); n != 0 {
					t.Fatalf("fixture has %d range deletions", n)
				}
				if len(metricKeys(t, s, gone.ID)) == 0 {
					t.Fatal("fixture wrote no keys for the deleted metric")
				}
				keepBefore := metricKeys(t, s, keep.ID)
				// bucketCodec has no Snapshot; its neighbour check is by key.
				var keepSnap Snapshot
				if !buckets {
					var err error
					if keepSnap, err = s.Snapshot(ctx, keep.Name); err != nil {
						t.Fatal(err)
					}
				}

				before, beforeContinuation := rangeDeleteTotal(), continuationRangeDeletes.Load()
				if conditional {
					current, err := s.Metadata(ctx, gone.Name)
					if err != nil {
						t.Fatal(err)
					}
					err = s.DeleteIfUnchanged(ctx, gone.Name, current)
					if err != nil {
						t.Fatal(err)
					}
				} else if err := s.Delete(ctx, gone.Name); err != nil {
					t.Fatal(err)
				}
				if got := rangeDeleteTotal() - before; got != 0 {
					t.Fatalf("delete counted %d range tombstones: %+v", got, RangeDeleteCounts())
				}
				if got := continuationRangeDeletes.Load() - beforeContinuation; got != 0 {
					t.Fatalf("delete wrote %d continuation range tombstones", got)
				}
				if n := flushedRangeDeletions(t, s); n != 0 {
					t.Fatalf("delete flushed %d range deletions", n)
				}
				if left := metricKeys(t, s, gone.ID); len(left) != 0 {
					t.Fatalf("%d keys of the deleted metric remain, first %q", len(left), left[0])
				}
				if _, err := s.Snapshot(ctx, gone.Name); !errors.Is(err, ErrNotFound) {
					t.Fatalf("deleted metric still readable: %v", err)
				}
				if _, err := s.Metadata(ctx, gone.Name); !errors.Is(err, ErrNotFound) {
					t.Fatalf("deleted metric metadata: %v", err)
				}
				// Idempotent: the second delete finds nothing and writes nothing.
				if err := s.Delete(ctx, gone.Name); !errors.Is(err, ErrNotFound) {
					t.Fatalf("second delete: %v", err)
				}
				keepAfter := metricKeys(t, s, keep.ID)
				if len(keepAfter) != len(keepBefore) {
					t.Fatalf("neighbour keys %d -> %d", len(keepBefore), len(keepAfter))
				}
				for i := range keepAfter {
					if !bytes.Equal(keepAfter[i], keepBefore[i]) {
						t.Fatalf("neighbour key %d changed: %q -> %q", i, keepBefore[i], keepAfter[i])
					}
				}
				if !buckets {
					snap, err := s.Snapshot(ctx, keep.Name)
					if err != nil {
						t.Fatal(err)
					}
					if fmt.Sprint(snap) != fmt.Sprint(keepSnap) {
						t.Fatal("neighbour snapshot changed")
					}
				}
				// The name can be recreated and starts empty.
				again := createTestMetric(t, s, gone.Name)
				if again.ID == gone.ID {
					t.Fatal("recreated metric reused the deleted identity")
				}
			})
		}
	}
}

// Every DeleteRange in the store goes through deleteRange, so a new
// producer of range tombstones shows up in store_range_deletes_total{site}.
func TestRangeDeleteCountsEverySite(t *testing.T) {
	counts := RangeDeleteCounts()
	if len(counts) != int(rangeDeleteSites) {
		t.Fatalf("%d sites, want %d", len(counts), rangeDeleteSites)
	}
	seen := map[string]bool{}
	for _, c := range counts {
		if c.Site == "" || seen[c.Site] {
			t.Fatalf("site name %q empty or repeated", c.Site)
		}
		seen[c.Site] = true
	}
	s, _ := openTestStore(t, filepath.Join(t.TempDir(), "store"), Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	b := s.db.NewBatch()
	defer b.Close()
	before := rangeDeleteCounts[rangeDeleteTransferRanges].Load()
	if err := deleteRange(b, rangeDeleteTransferRanges, []byte("x"), []byte("y")); err != nil {
		t.Fatal(err)
	}
	if got := rangeDeleteCounts[rangeDeleteTransferRanges].Load() - before; got != 1 {
		t.Fatalf("counted %d", got)
	}
}

// Deletes with a live reader over a memtable already holding many range
// tombstones. A range-tombstone delete invalidates the memtable's fragment
// cache, so the reader's next iterator re-fragments all of them under one
// sync.Once; point deletes leave the cache intact. Reports the reader's mean
// iterator-open time.
func BenchmarkAdministrativeDeleteLiveReader(b *testing.B) {
	ctx := context.Background()
	s := openTestStoreB(b, Options{ExactBlocks: true, Continuation: bucketCodec{}, TimeBuckets: true, MemTableSize: 256 << 20, SyncInterval: time.Hour})
	defer func() { _ = s.Close() }()
	// Seed tombstones far from any real identity, as an earlier producer would.
	seed := s.db.NewBatch()
	for i := range 20000 {
		lower := chunkPrefix(1<<62+uint64(i)*2, 0)[:9]
		if err := seed.DeleteRange(lower, prefixEnd(lower), nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := seed.Commit(pebble.NoSync); err != nil {
		b.Fatal(err)
	}
	seed.Close()
	names := make([]string, b.N)
	for i := range names {
		names[i] = fmt.Sprintf("bench.delete.%d", i)
		if _, err := s.Create(ctx, MetricConfig{Name: names[i], Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}); err != nil {
			b.Fatal(err)
		}
	}
	var stop atomic.Bool
	var opens, openNanos atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			start := time.Now()
			it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{'c'}, UpperBound: []byte{'d'}})
			if err != nil {
				panic(err)
			}
			it.First()
			openNanos.Add(int64(time.Since(start)))
			opens.Add(1)
			if err := it.Close(); err != nil {
				panic(err)
			}
		}
	}()
	b.ResetTimer()
	for i := range b.N {
		if err := s.Delete(ctx, names[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	stop.Store(true)
	wg.Wait()
	if n := opens.Load(); n > 0 {
		b.ReportMetric(float64(openNanos.Load())/float64(n), "reader-ns/open")
	}
}

func openTestStoreB(b *testing.B, opts Options) *Store {
	b.Helper()
	if opts.RelayoutMinFreeBytes == 0 {
		opts.RelayoutMinFreeBytes = 1
	}
	if opts.fs == nil {
		opts.fs = noSyncFS{FS: vfs.Default}
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Unix(100_000, 0) }
	}
	s, err := Open(filepath.Join(b.TempDir(), "store"), opts)
	if err != nil {
		b.Fatal(err)
	}
	return s
}
