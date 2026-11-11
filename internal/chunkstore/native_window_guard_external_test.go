package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestNativeWindowStaleEvictionPublication(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC) }
	old := time.Date(2026, 10, 24, 12, 52, 0, 0, time.UTC).Unix()
	incoming := time.Date(2026, 11, 7, 12, 52, 0, 0, time.UTC).Unix()
	w := &store.NativeWindow{From: time.Date(2026, 11, 6, 8, 58, 0, 0, time.UTC).Unix(), Until: time.Date(2026, 11, 10, 19, 15, 0, 0, time.UTC).Unix()}
	for _, mode := range []string{"chunks", "patch", "continuation-union"} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retry=%v", mode, retry), func(t *testing.T) {
				continuation := mode != "chunks"
				source := windowStore(t, continuation, clock)
				var target *store.Store
				if mode == "continuation-union" {
					var err error
					opts := store.Options{ExactBlocks: true, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20, Continuation: windowOracleCodec{&whisperio.ContinuationCodec{}}}
					target, err = store.Open("/union", opts.WithFS(vfs.NewMem()))
					if err != nil {
						t.Fatal(err)
					}
					target.StopBackground()
					t.Cleanup(func() {
						if err := target.Close(); err != nil {
							t.Error(err)
						}
					})
				} else {
					target = windowStore(t, continuation, clock)
				}
				cfg := store.MetricConfig{Name: "window.stale", Retentions: []store.Retention{{Step: 60, Count: 14 * 24 * 60}}, AggregationMethod: store.Sum}
				snap := func(ts int64, value float64) store.Snapshot {
					return store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{{Timestamp: ts, Value: value}}}}}
				}
				seedWindowSnapshot(t, source, snap(incoming, math.Copysign(0, -1)), continuation, clock)
				seedWindowSnapshot(t, target, snap(old, 42), continuation, clock)
				pin, err := source.PinNativeSource()
				if err != nil {
					t.Fatal(err)
				}
				defer pin.Close()
				var wire bytes.Buffer
				if err := pin.WriteWindow(ctx, []string{cfg.Name}, &wire, w); err != nil {
					t.Fatal(err)
				}
				changed := false
				if retry {
					target.NativeCombineHookForTest(func(stage string) error {
						if stage != "combine-before-publish" || changed {
							return nil
						}
						changed = true
						return target.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: clock().Unix() - 60, Value: 99}})
					})
				}
				r := windowRange([]string{cfg.Name}, w)
				for range 2 { // A lost-ack replay must neither republish nor recount.
					receipt, err := target.NativeImport(ctx, r, bytes.NewReader(wire.Bytes()))
					if err != nil || receipt.CopiedCount != 1 {
						t.Fatal(receipt, err)
					}
					stats := target.NativeStats()
					if stats.CombineStaleEvictedPoints != 1 || retry && stats.CombineRetries != 1 {
						t.Fatalf("stale evictions must count only publication: %+v", stats)
					}
				}
				got, err := target.Snapshot(ctx, cfg.Name)
				if err != nil {
					t.Fatal(err)
				}
				want := map[int64]uint64{incoming: math.Float64bits(math.Copysign(0, -1))}
				if retry {
					want[clock().Unix()-60] = math.Float64bits(99)
				}
				assertPointBits(t, want, pointBits(got.Archives[0].Points, nil, true))
			})
		}
	}
}

func TestNativeWindowConflictDoesNotPublishStaleEviction(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1400, 0) }
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprint(continuation), func(t *testing.T) {
			source, target := windowStore(t, continuation, clock), windowStore(t, continuation, clock)
			cfg := store.MetricConfig{Name: "window.atomic", Retentions: []store.Retention{{Step: 10, Count: 10}}, AggregationMethod: store.Sum}
			snap := func(points ...store.Point) store.Snapshot {
				return store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: points}}}
			}
			seedWindowSnapshot(t, source, snap(store.Point{Timestamp: 1200, Value: 1}, store.Point{Timestamp: 1210, Value: 2}), continuation, clock)
			seedWindowSnapshot(t, target, snap(store.Point{Timestamp: 1100, Value: 3}, store.Point{Timestamp: 1310, Value: 4}), continuation, clock)
			before, err := target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			w := &store.NativeWindow{From: 1200, Until: 1220}
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, []string{cfg.Name}, &wire, w); err != nil {
				t.Fatal(err)
			}
			if _, err := target.NativeImport(ctx, windowRange([]string{cfg.Name}, w), &wire); !errors.Is(err, store.ErrConflict) {
				t.Fatal("expected a newer outside owner conflict", err)
			}
			after, err := target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(before, after); err != nil {
				t.Fatal("conflict published a partial window", err)
			}
			if stats := target.NativeStats(); stats.CombineStaleEvictedPoints != 0 {
				t.Fatalf("conflict counted unpublished evictions: %+v", stats)
			}
		})
	}
}
