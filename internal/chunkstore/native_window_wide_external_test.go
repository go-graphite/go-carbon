package chunkstore_test

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Both sides bypass the optional seek index, as older continuation images do.
// The oracle additionally forces the existing archive-preserving rebuild.
type wideWindowOracleCodec struct{ windowOracleCodec }

func (wideWindowOracleCodec) CombineNativeWindow(context.Context, store.ContinuationIO, store.ContinuationIO, store.Metadata, func() time.Time, store.NativeWindow) ([]store.RelayoutKV, store.NativeCombineClass, bool, error) {
	return nil, store.NativeCombineClass{}, false, nil
}

func wideWindowStore(t testing.TB, clock func() time.Time, oracle bool) *store.Store {
	t.Helper()
	var codec store.ContinuationCodec = windowOracleCodec{&whisperio.ContinuationCodec{}}
	if oracle {
		codec = wideWindowOracleCodec{windowOracleCodec{&whisperio.ContinuationCodec{}}}
	}
	s, err := store.Open("/wide", (store.Options{ExactBlocks: true, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20, Continuation: codec}).WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	s.StopBackground()
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func wideWindowFixture(t testing.TB, source, target *store.Store, count int, clock func() time.Time, special string) *store.NativeWindow {
	t.Helper()
	cfg := store.MetricConfig{Name: "wide.window", Retentions: []store.Retention{{Step: 1, Count: count}}, AggregationMethod: store.Sum}
	makeSnap := func() store.Snapshot {
		return store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0]}}}
	}
	donor, survivor := makeSnap(), makeSnap()
	base := int64(2 * count)
	window := &store.NativeWindow{From: base, Until: base + 130}
	for i := 0; i < count; i++ {
		p := store.Point{Timestamp: int64(count + i), Value: float64(i)}
		if i < 130 && i%3 == 0 {
			p.Timestamp = base + int64(i)
			p.Value = -float64(i)
			if i == 0 {
				p.Value = math.Copysign(0, -1)
			}
		}
		if special == "newer-outside" && i == 1 {
			p.Timestamp = base + int64(count) + 1
		}
		if special == "nan-outside" && i == 200 {
			p.Value = math.Float64frombits(0x7ff8000000000042)
		}
		survivor.Archives[0].Points = append(survivor.Archives[0].Points, p)
		if i < 130 {
			donor.Archives[0].Points = append(donor.Archives[0].Points, store.Point{Timestamp: base + int64(i), Value: float64(1000 + i)})
		}
	}
	// RestoreArchives accepts independently authoritative, sorted archives.
	slices.SortFunc(survivor.Archives[0].Points, func(a, b store.Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	seedWindowSnapshot(t, source, donor, true, clock)
	seedWindowSnapshot(t, target, survivor, true, clock)
	return window
}

func TestNativeWindowWideHistoryDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(10000, 0) }
	for _, special := range []string{"stale-owners", "newer-outside", "nan-outside"} {
		t.Run(special, func(t *testing.T) {
			source := windowStore(t, true, clock)
			target, oracle := wideWindowStore(t, clock, false), wideWindowStore(t, clock, true)
			window := wideWindowFixture(t, source, target, 1024, clock, special)
			wideWindowFixture(t, source, oracle, 1024, clock, special)
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, []string{"wide.window"}, &wire, window); err != nil {
				t.Fatal(err)
			}
			req := windowRange([]string{"wide.window"}, window)
			gotReceipt, gotErr := target.NativeImport(ctx, req, bytes.NewReader(wire.Bytes()))
			wantReceipt, wantErr := oracle.NativeImport(ctx, req, bytes.NewReader(wire.Bytes()))
			if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || gotReceipt.DeferredCount != wantReceipt.DeferredCount {
				t.Fatalf("receipts/errors: %+v %v / %+v %v", gotReceipt, gotErr, wantReceipt, wantErr)
			}
			got, err := target.Snapshot(ctx, "wide.window")
			if err != nil {
				t.Fatal(err)
			}
			want, err := oracle.Snapshot(ctx, "wide.window")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			if special == "stale-owners" {
				stats := target.NativeStats()
				if stats.CombineMaterializedArchives != 0 || stats.CombineStaleEvictedPoints != 86 {
					t.Fatal(stats)
				}
			}
		})
	}
}

func BenchmarkNativeWindowWideHistory(b *testing.B) {
	for _, oracle := range []bool{true, false} {
		b.Run(map[bool]string{true: "rebuild", false: "splice"}[oracle], func(b *testing.B) {
			ctx := context.Background()
			clock := func() time.Time { return time.Unix(400000, 0) }
			source, target := windowStore(b, true, clock), wideWindowStore(b, clock, oracle)
			window := wideWindowFixture(b, source, target, 100000, clock, "stale-owners")
			pin, err := source.PinNativeSource()
			if err != nil {
				b.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, []string{"wide.window"}, &wire, window); err != nil {
				b.Fatal(err)
			}
			original, err := target.ExportTransfer(ctx, []string{"wide.window"})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if err := target.Delete(ctx, "wide.window"); err != nil {
					b.Fatal(err)
				}
				if _, err := target.ImportTransfer(ctx, original); err != nil {
					b.Fatal(err)
				}
				beforeStats := target.NativeStats()
				req := windowRange([]string{"wide.window"}, window)
				req.Operation = fmt.Sprintf("%064x", i+1)
				b.StartTimer()
				receipt, err := target.NativeImport(ctx, req, bytes.NewReader(wire.Bytes()))
				if err != nil || receipt.CopiedCount != 1 {
					b.Fatal(receipt, err)
				}
				b.StopTimer()
				if target.NativeStats().CombineStaleEvictedPoints-beforeStats.CombineStaleEvictedPoints != 86 {
					b.Fatal("benchmark did not repair the original holes")
				}
			}
		})
	}
}
