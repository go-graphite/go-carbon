package chunkstore_test

import (
	"bytes"
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

// transferEpochFixture fills source with names, half of them also live on
// each target (merged imports) and half absent (new names), and returns the
// exported transfer groups.
func transferEpochFixture(t *testing.T, source *store.Store, targets []*store.Store, total, group int) ([]string, [][]store.TransferMetric) {
	t.Helper()
	ctx := context.Background()
	put := func(s *store.Store, name string, points []store.Point) {
		if _, err := s.Metadata(ctx, name); err != nil {
			if _, err := s.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 600}, {Step: 60, Count: 1000}}, AggregationMethod: store.Sum}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.UpdateMany(ctx, name, points); err != nil {
			t.Fatal(err)
		}
	}
	var history []store.Point
	for ts := int64(940000); ts < 999900; ts += 290 {
		history = append(history, store.Point{Timestamp: ts, Value: float64(ts)/7 + 0.1})
	}
	names := make([]string, total)
	for i := range names {
		names[i] = fmt.Sprintf("epoch.commit.%04d", i)
		put(source, names[i], history)
		if i%2 == 0 {
			for _, target := range targets {
				put(target, names[i], []store.Point{{Timestamp: 999880, Value: 3}, {Timestamp: 960010, Value: -1}})
			}
		}
	}
	var batches [][]store.TransferMetric
	for start := 0; start < total; start += group {
		records, err := source.ExportTransfer(ctx, names[start:min(start+group, total)])
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, records)
	}
	return names, batches
}

// The unchanged-epoch commit shortcut only skips a validation read: the
// committed bytes must be identical to commits that re-read every catalog
// entry under the stripes, for merged and new names alike.
func TestTransferEpochCommitIsBitIdentical(t *testing.T) {
	ctx := context.Background()
	source := transferStore(t, true, true)
	fast, reread := transferStore(t, true, true), transferStore(t, true, true)
	reread.SetTransferRereadMetadataForTest(true)
	_, batches := transferEpochFixture(t, source, []*store.Store{fast, reread}, 48, 8)
	for _, records := range batches {
		for _, target := range []*store.Store{fast, reread} {
			proofs, err := target.ImportTransfer(ctx, records)
			if err != nil {
				t.Fatal(err)
			}
			if err := target.VerifyTransfer(ctx, proofs); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := fast.TransferApplyStats().EpochValidatedNames; got != 48 {
		t.Fatalf("shortcut validated %d of 48 names", got)
	}
	if got := reread.TransferApplyStats().EpochValidatedNames; got != 0 {
		t.Fatalf("re-read store took the shortcut for %d names", got)
	}
	want, err := reread.RawKeyValuesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got, err := fast.RawKeyValuesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("%d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i][0], want[i][0]) || !bytes.Equal(got[i][1], want[i][1]) {
			t.Fatalf("key %d differs: %x=%x, want %x=%x", i, got[i][0], got[i][1], want[i][0], want[i][1])
		}
	}
}

// Concurrent imports with live writers on the same names: every name ends
// with the same points whichever path validated its commit.
func TestTransferEpochCommitConcurrentMatchesReread(t *testing.T) {
	ctx := context.Background()
	source := transferStore(t, true, true)
	fast, reread := transferStore(t, true, true), transferStore(t, true, true)
	reread.SetTransferRereadMetadataForTest(true)
	names, batches := transferEpochFixture(t, source, []*store.Store{fast, reread}, 96, 8)
	for _, target := range []*store.Store{fast, reread} {
		var wg sync.WaitGroup
		errs := make(chan error, len(batches)+len(names))
		for _, records := range batches {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := target.ImportTransfer(ctx, records)
				errs <- err
			}()
		}
		for i, name := range names {
			// Write only initially present names on both targets. For absent
			// names, Metadata racing the import could skip a write on just one
			// target, so the two snapshots would have different input histories.
			if i%6 != 0 {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := target.Metadata(ctx, name); err == nil {
					errs <- target.UpdateMany(ctx, name, []store.Point{{Timestamp: 999890, Value: float64(i)}})
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range names {
		a, err := fast.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		b, err := reread.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(b, a); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// BenchmarkTransferImportConcurrent merges groups of live names from
// 64 or 128 concurrent streams into a flushed target, the shape of several
// copies converging on one survivor. "reread" is the commit before the
// unchanged-epoch shortcut (two catalog Gets per name under the stripes).
// Reports names/s, stripe hold per name and split groups.
func BenchmarkTransferImportConcurrent(b *testing.B) {
	for _, streams := range []int{64, 128} {
		for _, mode := range []string{"reread", "epoch"} {
			b.Run(fmt.Sprintf("streams=%d/%s", streams, mode), func(b *testing.B) {
				benchmarkTransferImportConcurrent(b, streams, mode == "reread")
			})
		}
	}
}

func benchmarkTransferImportConcurrent(b *testing.B, streams int, reread bool) {
	const group, perStream = 16, 2
	ctx := context.Background()
	open := func() *store.Store {
		root := b.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "originals"), 0750); err != nil {
			b.Fatal(err)
		}
		now := time.Unix(1000000, 0)
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
	var namesPerSec, holdPerName, splits float64
	for range b.N {
		b.StopTimer()
		source, target := open(), open()
		target.SetTransferRereadMetadataForTest(reread)
		total := streams * perStream * group
		var history []store.Point
		for ts := int64(990000); ts < 999900; ts += 300 {
			history = append(history, store.Point{Timestamp: ts, Value: float64(ts)})
		}
		names := make([]string, total)
		for i := range names {
			names[i] = fmt.Sprintf("bench.concurrent.%06d", i)
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
		if err := target.FlushForTest(); err != nil {
			b.Fatal(err)
		}
		before := target.TransferApplyStats()
		var next atomic.Int64
		var wg sync.WaitGroup
		b.StartTimer()
		began := time.Now()
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
		after := target.TransferApplyStats()
		namesPerSec += float64(total) / elapsed.Seconds()
		holdPerName += (after.LockHoldSeconds - before.LockHoldSeconds) / float64(total)
		splits += float64(after.SplitGroups - before.SplitGroups)
	}
	n := float64(max(b.N, 1))
	b.ReportMetric(namesPerSec/n, "names/s")
	b.ReportMetric(holdPerName/n*1e6, "hold-us/name")
	b.ReportMetric(splits/n, "splits")
}
