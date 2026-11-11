package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type windowOracleCodec struct{ *whisperio.ContinuationCodec }

func (c windowOracleCodec) PlanNativeWindowPatch(context.Context, store.ContinuationIO, store.Snapshot, func() time.Time, store.NativeWindow) ([]store.RelayoutKV, store.NativeCombineClass, bool, error) {
	return nil, store.NativeCombineClass{}, false, nil
}

func windowSeekFixture(t testing.TB, s *store.Store, clock func() time.Time, donor bool) {
	t.Helper()
	cfg := store.MetricConfig{Name: "seek.patch", Retentions: []store.Retention{{Step: 10, Count: 512}}, AggregationMethod: store.Sum}
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0]}}}
	for i := range 512 {
		if donor && i%3 == 0 || !donor && i%4 == 0 {
			continue
		}
		value := float64(i)
		if !donor {
			value = -value
			switch i % 13 {
			case 0:
				value = math.Float64frombits(0x7ff8000000000042)
			case 1:
				value = math.Copysign(0, -1)
			case 2:
				value = math.Inf(1)
			}
		}
		snap.Archives[0].Points = append(snap.Archives[0].Points, store.Point{Timestamp: 6000 + int64(i*10), Value: value})
	}
	seedWindowSnapshot(t, s, snap, true, clock)
}

func TestNativeWindowSeekPatchDifferential(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(12000, 0) }
	for _, concurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged-outside", true: "concurrent-writer"}[concurrent], func(t *testing.T) {
			source, target := windowStore(t, true, clock), windowStore(t, true, clock)
			opts := store.Options{ExactBlocks: true, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20, Continuation: windowOracleCodec{&whisperio.ContinuationCodec{}}}
			oracle, err := store.Open("/oracle", opts.WithFS(vfs.NewMem()))
			if err != nil {
				t.Fatal(err)
			}
			oracle.StopBackground()
			defer oracle.Close()
			windowSeekFixture(t, source, clock, true)
			windowSeekFixture(t, target, clock, false)
			windowSeekFixture(t, oracle, clock, false)
			before, err := target.ExportTransfer(ctx, []string{"seek.patch"})
			if err != nil {
				t.Fatal(err)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			w := &store.NativeWindow{From: 8005, Until: 8305}
			r := windowRange([]string{"seek.patch"}, w)
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, r.Names, &wire, w); err != nil {
				t.Fatal(err)
			}
			changed := false
			if concurrent {
				target.NativeCombineHookForTest(func(stage string) error {
					if stage != "combine-before-publish" || changed {
						return nil
					}
					changed = true
					for _, s := range []*store.Store{target, oracle} {
						if err := s.UpdateMany(ctx, "seek.patch", []store.Point{{Timestamp: 8100, Value: 42}, {Timestamp: 10000, Value: 99}}); err != nil {
							return err
						}
					}
					return nil
				})
			}
			for _, s := range []*store.Store{target, oracle} {
				if _, err := s.NativeImport(ctx, r, bytes.NewReader(wire.Bytes())); err != nil {
					t.Fatal(err)
				}
			}
			got, err := target.Snapshot(ctx, "seek.patch")
			if err != nil {
				t.Fatal(err)
			}
			want, err := oracle.Snapshot(ctx, "seek.patch")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			if concurrent {
				if !changed || target.NativeStats().CombineRetries != 1 {
					t.Fatal("missing revision retry", target.NativeStats())
				}
				return
			}
			after, err := target.ExportTransfer(ctx, r.Names)
			if err != nil {
				t.Fatal(err)
			}
			if before[0].Version.ID != after[0].Version.ID || before[0].Version.Generation != after[0].Version.Generation {
				t.Fatal("window rewrote the generation")
			}
			old := map[string][]byte{}
			for _, kv := range before[0].Keys {
				old[string(kv.Key)] = kv.Value
			}
			changedGroups := 0
			for _, kv := range after[0].Keys {
				if len(kv.Key) == 20 && kv.Key[0] == 'f' && kv.Key[9] == 'p' && !bytes.Equal(old[string(kv.Key)], kv.Value) {
					changedGroups++
				}
			}
			if changedGroups > 2 || changedGroups == 0 {
				t.Fatalf("small window changed %d groups", changedGroups)
			}
		})
	}
}

func TestNativeWindowSeekPatchCrashResume(t *testing.T) {
	for _, stage := range []string{"combine-prepared", "combine-before-publish", "combine-name-published", "combine-receipt", "combine-published"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			clock := func() time.Time { return time.Unix(12000, 0) }
			source := windowStore(t, true, clock)
			windowSeekFixture(t, source, clock, true)
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			w := &store.NativeWindow{From: 8000, Until: 8300}
			r := windowRange([]string{"seek.patch"}, w)
			var wire bytes.Buffer
			if err := pin.WriteWindow(ctx, r.Names, &wire, w); err != nil {
				t.Fatal(err)
			}
			fs := vfs.NewStrictMem()
			opts := store.Options{ExactBlocks: true, Now: clock, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20, Continuation: &whisperio.ContinuationCodec{}}
			open := func() *store.Store {
				s, err := store.Open("/target", opts.WithFS(fs))
				if err != nil {
					t.Fatal(err)
				}
				s.StopBackground()
				return s
			}
			target := open()
			windowSeekFixture(t, target, clock, false)
			before, err := target.Snapshot(ctx, "seek.patch")
			if err != nil {
				t.Fatal(err)
			}
			if err := target.NativeCombineSyncForTest(); err != nil {
				t.Fatal(err)
			}
			target.NativeCombineHookForTest(func(at string) error {
				if at != stage {
					return nil
				}
				if stage == "combine-name-published" {
					if err := target.NativeCombineSyncForTest(); err != nil {
						return err
					}
				}
				fs.SetIgnoreSyncs(true)
				return errors.New("simulated crash")
			})
			if _, err := target.NativeImport(ctx, r, bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("crash was not injected")
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			target = open()
			defer target.Close()
			receipt, err := target.NativeImport(ctx, r, bytes.NewReader(wire.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			if receipt.CopiedCount != 1 || receipt.DeferredCount != 0 {
				t.Fatal(receipt)
			}
			got, err := target.Snapshot(ctx, "seek.patch")
			if err != nil {
				t.Fatal(err)
			}
			assertPointBits(t, pointBits(before.Archives[0].Points, w, false), pointBits(got.Archives[0].Points, w, false))
			if got.Metadata.ID != before.Metadata.ID || len(pointBits(got.Archives[0].Points, w, true)) <= len(pointBits(before.Archives[0].Points, w, true)) {
				t.Fatal("patch lost or old generation retired")
			}
		})
	}
}

func TestNativeWindowStaleIndexImport(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(2000, 0) }
	cfg := store.MetricConfig{Name: "old.writer", Retentions: []store.Retention{{Step: 10, Count: 10}}, AggregationMethod: store.Sum}
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg, Revision: 1}, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{{Timestamp: 1000, Value: 1}}}}}
	memory := largeContinuationMemory{}
	if err := (&whisperio.ContinuationCodec{}).RestoreArchives(ctx, memory, snap, clock); err != nil {
		t.Fatal(err)
	}
	record := store.TransferMetric{Config: cfg, Version: store.TransferVersion{Name: cfg.Name, ID: 1, Generation: 1, Revision: 2}, Continuation: true}
	for key, value := range memory {
		// Model a writer that knows point groups and revisions, but not the index.
		if len(key) == 20 && key[9] == 'p' {
			var block store.PointBlock
			if err := store.DecodePointBlock(value, &block); err != nil {
				t.Fatal(err)
			}
			block.Points[0].Timestamp = 1200
			value = store.EncodePointBlock(&block)
		}
		record.Keys = append(record.Keys, store.RelayoutKV{Key: []byte(key), Value: value})
	}
	slices.SortFunc(record.Keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
	source := windowStore(t, true, clock)
	if _, err := source.ImportTransfer(ctx, []store.TransferMetric{record}); err != nil {
		t.Fatal(err)
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.WriteWindow(ctx, []string{cfg.Name}, &wire, &store.NativeWindow{From: 1000, Until: 1010}); !errors.Is(err, store.ErrConflict) {
		t.Fatal("stale bounds hid newer owner", err)
	}
}
