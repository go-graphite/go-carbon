package chunkstore_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// A merged import retires the target's live generation. It must do so with
// point deletes: every memtable range tombstone invalidates the fragment cache
// all readers share, and the next iterator of any reader re-fragments every
// tombstone while the others wait. The retired generation must still vanish
// completely and the merged points stay exact (target wins equal timestamps).
func TestTransferMergeRetiresWithPointDeletes(t *testing.T) {
	ctx := context.Background()
	source := transferStore(t, true, true)
	target := transferStore(t, true, true)
	names := make([]string, 8)
	for i := range names {
		names[i] = fmt.Sprintf("retire.points.n%02d", i)
		// Span several time buckets so the drop has several bucket prefixes.
		var history []store.Point
		for ts := int64(940000); ts < 999900; ts += 600 {
			history = append(history, store.Point{Timestamp: ts, Value: float64(ts)})
		}
		putTransferPoints(t, source, names[i], history)
		putTransferPoints(t, target, names[i], []store.Point{{Timestamp: 999880, Value: 3}, {Timestamp: 999910, Value: 4}})
	}
	old := make([]store.Metadata, len(names))
	for i, name := range names {
		m, err := target.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		old[i] = m
		if n, err := target.GenerationKeysForTest(m); err != nil || n == 0 {
			t.Fatalf("target generation %s keys=%d err=%v", name, n, err)
		}
	}
	records, err := source.ExportTransfer(ctx, names)
	if err != nil {
		t.Fatal(err)
	}
	before := target.Stats().ContinuationRangeDeletes
	proofs, err := target.ImportTransfer(ctx, records)
	if err != nil {
		t.Fatal(err)
	}
	if got := target.Stats().ContinuationRangeDeletes - before; got != 0 {
		t.Fatalf("merged import wrote %d continuation range tombstones", got)
	}
	if err := target.VerifyTransfer(ctx, proofs); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		if !proofs[i].Merged {
			t.Fatalf("%s was not merged", name)
		}
		if n, err := target.GenerationKeysForTest(old[i]); err != nil || n != 0 {
			t.Fatalf("retired generation of %s keeps %d keys (err %v)", name, n, err)
		}
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		want, err := source.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		// The target's own points win: 999880 keeps 3 and 999910 survives.
		found := map[int64]float64{}
		for _, p := range got.Archives[0].Points {
			found[p.Timestamp] = p.Value
		}
		if found[999880] != 3 || found[999910] != 4 {
			t.Fatalf("%s lost target points: 999880=%v 999910=%v", name, found[999880], found[999910])
		}
		for _, p := range want.Archives[0].Points {
			if p.Timestamp == 999880 {
				continue
			}
			if v, ok := found[p.Timestamp]; !ok || v != p.Value {
				t.Fatalf("%s point %d: got %v (present %v) want %v", name, p.Timestamp, v, ok, p.Value)
			}
		}
	}
}

// BenchmarkTransferMergeImportLiveTarget imports already-present (live) names
// from several streams while a reader keeps opening iterators, the shape of a
// survivor copy into a serving store. Reports merged names/s.
func BenchmarkTransferMergeImportLiveTarget(b *testing.B) {
	const streams, group = 8, 16
	ctx := context.Background()
	open := func() *store.Store {
		root := b.TempDir()
		now := time.Unix(1000000, 0)
		if err := os.MkdirAll(filepath.Join(root, "originals"), 0750); err != nil {
			b.Fatal(err)
		}
		s, err := store.Open(filepath.Join(root, "pool"), store.Options{ExactBlocks: true, CacheSize: 64 << 20, MemTableSize: 64 << 20, SyncInterval: time.Hour, Now: func() time.Time { return now }, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true}, IncrementalSource: filepath.Join(root, "originals"), TimeBuckets: true})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = s.Close() })
		return s
	}
	put := func(s *store.Store, name string, points []store.Point) {
		if _, err := s.Metadata(ctx, name); err != nil {
			if _, err := s.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 600}, {Step: 60, Count: 1000}}, AggregationMethod: store.Sum}); err != nil {
				b.Fatal(err)
			}
		}
		if err := s.UpdateMany(ctx, name, points); err != nil {
			b.Fatal(err)
		}
	}
	source, target := open(), open()
	total := max(b.N, 1) * streams * group
	names := make([]string, total)
	var history []store.Point
	for ts := int64(940000); ts < 999900; ts += 300 {
		history = append(history, store.Point{Timestamp: ts, Value: float64(ts)})
	}
	for i := range names {
		names[i] = fmt.Sprintf("bench.live.%06d", i)
		put(source, names[i], history)
		put(target, names[i], []store.Point{{Timestamp: 999880, Value: 3}})
	}
	batches := make([][]store.TransferMetric, 0, total/group)
	for start := 0; start < total; start += group {
		records, err := source.ExportTransfer(ctx, names[start:start+group])
		if err != nil {
			b.Fatal(err)
		}
		batches = append(batches, records)
	}
	// A serving memtable already holds the tombstones of earlier range deletes;
	// each further one makes the next iterator re-fragment all of them.
	if err := target.SeedMemtableRangeTombstonesForTest(200000); err != nil {
		b.Fatal(err)
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() { // live readers: each Snapshot opens fresh iterators
		defer readers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = target.Snapshot(ctx, names[i%len(names)])
		}
	}()
	rangeBefore := target.Stats().ContinuationRangeDeletes
	var next atomic.Int64
	b.ResetTimer()
	began := time.Now()
	var wg sync.WaitGroup
	for range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(batches) {
					return
				}
				if _, err := target.ImportTransfer(ctx, batches[i]); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(began)
	b.StopTimer()
	close(stop)
	readers.Wait()
	b.ReportMetric(float64(total)/elapsed.Seconds(), "names/s")
	b.ReportMetric(float64(target.Stats().ContinuationRangeDeletes-rangeBefore)/float64(total), "rangedels/name")
}
