package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func windowStore(t testing.TB, continuation bool, clock func() time.Time) *store.Store {
	s, _ := windowStoreCloser(t, continuation, clock)
	return s
}
func windowStoreCloser(t testing.TB, continuation bool, clock func() time.Time) (*store.Store, func()) {
	t.Helper()
	opts := store.Options{ExactBlocks: true, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20}
	if continuation {
		opts.Continuation = &whisperio.ContinuationCodec{}
	}
	s, err := store.Open("/db", opts.WithFS(vfs.NewMem()))
	if err != nil {
		t.Fatal(err)
	}
	s.StopBackground()
	var once sync.Once
	closeStore := func() {
		once.Do(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(closeStore)
	return s, closeStore
}
func seedWindowSnapshot(t testing.TB, s *store.Store, snap store.Snapshot, continuation bool, clock func() time.Time) {
	t.Helper()
	if !continuation {
		if _, err := s.CreateFromSnapshot(context.Background(), snap); err != nil {
			t.Fatal(err)
		}
		return
	}
	kv := largeContinuationMemory{}
	snap.Metadata.Revision = 1
	if err := (&whisperio.ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, clock); err != nil {
		t.Fatal(err)
	}
	record := store.TransferMetric{Config: snap.Metadata.MetricConfig, Version: store.TransferVersion{Name: snap.Metadata.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
	for k, v := range kv {
		record.Keys = append(record.Keys, store.RelayoutKV{Key: []byte(k), Value: v})
	}
	slices.SortFunc(record.Keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	if _, err := s.ImportTransfer(context.Background(), []store.TransferMetric{record}); err != nil {
		t.Fatal(err)
	}
}
func windowRange(names []string, w *store.NativeWindow) store.NativeRange {
	return store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: true, Window: w}
}
func pointBits(points []store.Point, w *store.NativeWindow, inside bool) map[int64]uint64 {
	m := map[int64]uint64{}
	for _, p := range points {
		if w.Contains(p.Timestamp) == inside {
			m[p.Timestamp] = math.Float64bits(p.Value)
		}
	}
	return m
}
func assertPointBits(t testing.TB, want, got map[int64]uint64) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("point count %d != %d", len(want), len(got))
	}
	for ts, b := range want {
		if g, ok := got[ts]; !ok || g != b {
			t.Fatalf("point %d: want %016x, got %016x (present %v)", ts, b, g, ok)
		}
	}
}

func TestNativeWindowDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1_100_000, 0) }
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprint(continuation), func(t *testing.T) {
			source, target, full := windowStore(t, continuation, clock), windowStore(t, continuation, clock), windowStore(t, continuation, clock)
			rng := rand.New(rand.NewPCG(42, 19))
			var names []string
			before := map[string]store.Snapshot{}
			for n := range 24 {
				name := fmt.Sprintf("window.random.%02d", n)
				names = append(names, name)
				cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 1024}, {Step: 60, Count: 1024}, {Step: 300, Count: 1024}}, AggregationMethod: store.Sum}
				makeSnap := func() store.Snapshot {
					snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: make([]store.Archive, 3)}
					for ai, r := range cfg.Retentions {
						snap.Archives[ai].Retention = r
						for i := range 80 {
							if rng.IntN(3) == 0 {
								continue
							}
							v := float64(rng.IntN(99))
							switch rng.IntN(12) {
							case 0:
								v = math.Copysign(0, -1)
							case 1:
								v = 0
							case 2:
								if n%2 == 0 {
									v = math.Float64frombits(0x7ff8000000000042)
								}
							}
							snap.Archives[ai].Points = append(snap.Archives[ai].Points, store.Point{Timestamp: 990000 + int64(i*r.Step), Value: v})
						}
					}
					return snap
				}
				seedWindowSnapshot(t, source, makeSnap(), continuation, clock)
				right := makeSnap()
				seedWindowSnapshot(t, target, right, continuation, clock)
				seedWindowSnapshot(t, full, right, continuation, clock)
				snap, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				before[name] = snap
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			// Aligned to all three archives; separate tests cover partial buckets.
			window := &store.NativeWindow{From: 990300, Until: 990600}
			var all, part bytes.Buffer
			if err := pin.Write(ctx, names, &all); err != nil {
				t.Fatal(err)
			}
			if err := pin.WriteWindow(ctx, names, &part, window); err != nil {
				t.Fatal(err)
			}
			if _, err := full.NativeImport(ctx, windowRange(names, nil), &all); err != nil {
				t.Fatal(err)
			}
			receipt, err := target.NativeImport(ctx, windowRange(names, window), &part)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.CopiedCount != len(names) {
				t.Fatal(receipt)
			}
			for _, name := range names {
				got, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				want, err := full.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				for ai := range got.Archives {
					assertPointBits(t, pointBits(want.Archives[ai].Points, window, true), pointBits(got.Archives[ai].Points, window, true))
					assertPointBits(t, pointBits(before[name].Archives[ai].Points, window, false), pointBits(got.Archives[ai].Points, window, false))
				}
			}
		})
	}
}

func TestNativeWindowBoundaryAndWireScope(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(2000, 0) }
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			source, target := windowStore(t, true, clock), windowStore(t, true, clock)
			cfg := store.MetricConfig{Name: "boundary", Retentions: []store.Retention{{Step: 10, Count: 600}, {Step: 60, Count: 600}}, AggregationMethod: store.Sum}
			snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0]}, {Retention: cfg.Retentions[1], Points: []store.Point{{Timestamp: 1200, Value: 42}, {Timestamp: 1260, Value: 43}}}}}
			for ts := int64(1200); ts < 1320; ts += 10 {
				if !complete && ts == 1300 {
					continue
				}
				snap.Archives[0].Points = append(snap.Archives[0].Points, store.Point{Timestamp: ts, Value: 1})
			}
			seedWindowSnapshot(t, source, snap, true, clock)
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			w := &store.NativeWindow{From: 1205, Until: 1281}
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, []string{cfg.Name}, &wire, w); err != nil {
				t.Fatal(err)
			}
			r := windowRange([]string{cfg.Name}, w)
			if _, err := target.NativeImport(ctx, windowRange(r.Names, nil), bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("full import accepted windowed payload")
			}
			wrong := *w
			wrong.Until++
			r.Window = &wrong
			if _, err := target.NativeImport(ctx, r, bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("different window accepted")
			}
			r.Window = w
			receipt, err := target.NativeImport(ctx, r, bytes.NewReader(wire.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			wantSkips := 3
			if complete {
				wantSkips = 2
			}
			if len(receipt.WindowSkipped) != wantSkips {
				t.Fatal(receipt.WindowSkipped)
			}
			got, err := target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			if (len(got.Archives[1].Points) == 1) != complete {
				t.Fatal(got.Archives[1])
			}
			for _, a := range got.Archives {
				for _, p := range a.Points {
					if !w.Contains(p.Timestamp) {
						t.Fatal("exported outside window", p)
					}
				}
			}
		})
	}
}

func TestNativeWindowPayloadScalesWithWindow(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(4_000_000, 0) }
	var sizes []int
	for _, count := range []int{128_000, 2_000_000} {
		s := windowStore(t, true, clock)
		cfg := store.MetricConfig{Name: "size", Retentions: []store.Retention{{Step: 1, Count: count}}, AggregationMethod: store.Sum}
		if err := seedLargeContinuation(s, cfg, clock); err != nil {
			t.Fatal(err)
		}
		pin, err := s.PinNativeSource()
		if err != nil {
			t.Fatal(err)
		}
		var part bytes.Buffer
		window := &store.NativeWindow{From: int64(count + 128), Until: int64(count + 128 + 3600)}
		if err := pin.WriteWindow(ctx, []string{cfg.Name}, &part, window); err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, part.Len())
		var full bytes.Buffer
		if err := pin.Write(ctx, []string{cfg.Name}, &full); err != nil {
			t.Fatal(err)
		}
		if part.Len()*20 >= full.Len() {
			t.Fatalf("window=%d full=%d", part.Len(), full.Len())
		}
		t.Logf("history=%d full=%d window=%d", count, full.Len(), part.Len())
		pin.Close()
	}
	if sizes[1] > sizes[0]*11/10 {
		t.Fatalf("payload grew with history: %v", sizes)
	}
}

// Same physical two-million-point fixture as BenchmarkNativeCombineWorkers.
// Timer includes export, framing, transfer, import and target-wins publication.
func BenchmarkNativeWindowHeavy(b *testing.B) {
	const count, nameCount = 2_000_000, 32
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(2*count, 0) }
	source := windowStore(b, true, clock)
	names := make([]string, nameCount)
	for i := range names {
		names[i] = fmt.Sprintf("heavy.history.%02d", i)
		cfg := store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: count}}, AggregationMethod: store.Sum}
		if err := seedLargeContinuation(source, cfg, clock); err != nil {
			b.Fatal(err)
		}
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		b.Fatal(err)
	}
	defer pin.Close()
	rates := make(map[string]float64)
	for _, mode := range []string{"full", "1h"} {
		b.Run(mode, func(b *testing.B) {
			b.StopTimer()
			var elapsed time.Duration
			var wireBytes int64
			for range b.N {
				target, closeTarget := windowStoreCloser(b, true, clock)
				for _, name := range names {
					cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: count}}, AggregationMethod: store.Sum}
					if err := seedLargeContinuation(target, cfg, clock); err != nil {
						b.Fatal(err)
					}
					if err := target.UpdateMany(ctx, name, []store.Point{{Timestamp: count, Value: 42}}); err != nil {
						b.Fatal(err)
					}
				}
				var window *store.NativeWindow
				if mode == "1h" {
					window = &store.NativeWindow{From: count, Until: count + 3600}
				}
				input, output := io.Pipe()
				done := make(chan error, 1)
				b.StartTimer()
				start := time.Now()
				var receipt store.NativeReceipt
				pprof.Do(ctx, pprof.Labels("copy-mode", mode), func(ctx context.Context) {
					go func() { err := pin.WriteWindow(ctx, names, output, window); output.CloseWithError(err); done <- err }()
					receipt, err = target.NativeImport(ctx, windowRange(names, window), input)
				})
				input.CloseWithError(err)
				exportErr := <-done
				elapsed += time.Since(start)
				b.StopTimer()
				if err != nil || exportErr != nil {
					b.Fatal(err, exportErr)
				}
				if receipt.CopiedCount != nameCount {
					b.Fatal(receipt)
				}
				wireBytes += receipt.Bytes
				closeTarget()
			}
			rates[mode] = float64(nameCount*b.N) / elapsed.Seconds()
			b.ReportMetric(rates[mode], "names/s")
			b.ReportMetric(float64(wireBytes)/float64(nameCount*b.N), "wire-B/name")
		})
	}
	if full, window := rates["full"], rates["1h"]; full > 0 && window > 0 {
		ratio := window / full
		b.ReportMetric(ratio, "window/full")
		if ratio < 10 {
			b.Fatalf("1h window %.2f names/s / full %.2f names/s = %.2fx; require >=10x", window, full, ratio)
		}
	}
}

func TestNativeWindowRejectsRingEviction(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(3000, 0) }
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprint(continuation), func(t *testing.T) {
			source, target := windowStore(t, continuation, clock), windowStore(t, continuation, clock)
			cfg := store.MetricConfig{Name: "ring.alias", Retentions: []store.Retention{{Step: 10, Count: 10}}, AggregationMethod: store.Sum}
			makeSnap := func(ts int64) store.Snapshot {
				return store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{{Timestamp: ts, Value: 42}}}}}
			}
			seedWindowSnapshot(t, source, makeSnap(1200), continuation, clock)
			seedWindowSnapshot(t, target, makeSnap(1300), continuation, clock)
			before, err := target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			w := &store.NativeWindow{From: 1200, Until: 1210}
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, []string{cfg.Name}, &wire, w); err != nil {
				t.Fatal(err)
			}
			if _, err := target.NativeImport(ctx, windowRange([]string{cfg.Name}, w), &wire); !errors.Is(err, store.ErrConflict) {
				t.Fatal("newer outside ring owner must conflict", err)
			}
			after, err := target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			assertPointBits(t, pointBits(before.Archives[0].Points, w, false), pointBits(after.Archives[0].Points, w, false))
			wire.Reset()
			expired := &store.NativeWindow{From: 1100, Until: 1110}
			if err := pin.WriteWindow(ctx, []string{cfg.Name}, &wire, expired); err == nil {
				t.Fatal("omitted source ring owner could change full-copy result")
			}
		})
	}
}
