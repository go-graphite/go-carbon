package chunkstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestNativeCombineDifferential(t *testing.T) {
	for _, mode := range []struct {
		name                  string
		continuation, buckets bool
	}{{"exact", false, false}, {"v6", true, false}, {"v7", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			source := nativeMemoryStore(t, mode.continuation, mode.buckets)
			target := nativeMemoryStore(t, mode.continuation, mode.buckets)
			reference := nativeMemoryStore(t, mode.continuation, mode.buckets)
			var names []string
			for i := range 65 {
				name := fmt.Sprintf("native.combine.%03d", 64-i)
				names = append(names, name)
				putTransferPoints(t, source, name, []store.Point{{Timestamp: 999800, Value: math.Copysign(0, -1)}, {Timestamp: 999820, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 999880, Value: math.Inf(1)}, {Timestamp: 999890, Value: float64(i)}})
				if i%3 != 0 {
					for _, s := range []*store.Store{target, reference} {
						putTransferPoints(t, s, name, []store.Point{{Timestamp: 999880, Value: math.Inf(-1)}, {Timestamp: 999910, Value: 7}, {Timestamp: 999920, Value: math.Copysign(0, -1)}})
					}
				}
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.Write(ctx, names, &wire); err != nil {
				t.Fatal(err)
			}
			records, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reference.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names, Combine: true}
			receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			if receipt.CopiedCount != len(names) || receipt.DeferredCount != 0 {
				t.Fatal(receipt)
			}
			if len(receipt.Mappings) != len(names) {
				t.Fatal("durable ID lineage missing", receipt)
			}
			for i, mapping := range receipt.Mappings {
				donor, err := source.Metadata(ctx, names[i])
				if err != nil {
					t.Fatal(err)
				}
				survivor, err := target.Metadata(ctx, names[i])
				if err != nil {
					t.Fatal(err)
				}
				if mapping.Ordinal != i || mapping.SourceID != donor.ID || mapping.SourceGeneration != donor.Generation || mapping.SourceRevision != donor.Revision || mapping.TargetID != survivor.ID {
					t.Fatal("incorrect native identity mapping", mapping, donor, survivor)
				}
			}
			for _, name := range names {
				want, err := reference.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := target.Snapshot(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.EqualSnapshots(want, got); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			}
			// A lost acknowledgement cannot cause a second merge after newer
			// foreground points have arrived, even with a completely invalid body.
			putTransferPoints(t, target, names[0], []store.Point{{Timestamp: 999930, Value: 99}})
			before, _ := target.Metadata(ctx, names[0])
			if _, err := target.NativeImport(ctx, request, strings.NewReader("not a stream")); err != nil {
				t.Fatal(err)
			}
			after, _ := target.Metadata(ctx, names[0])
			if before.ID != after.ID || before.Generation != after.Generation || before.Revision != after.Revision {
				t.Fatal("durable retry changed foreground history", before, after)
			}
		})
	}
}

func TestNativeCombineV7CrashKeepsPublishedBucketsAndOriginalInput(t *testing.T) {
	for _, stage := range []string{"combine-ingested", "combine-prepared", "combine-name-published", "combine-published"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			source := nativeMemoryStore(t, true, true)
			names := []string{"combine.buckets.a", "combine.buckets.b"}
			for _, name := range names {
				var points []store.Point
				for ts := int64(995000); ts < 999900; ts += 10 {
					points = append(points, store.Point{Timestamp: ts, Value: float64(ts % 71)})
				}
				putTransferPoints(t, source, name, points)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.Write(ctx, names, &wire); err != nil {
				t.Fatal(err)
			}
			fs := vfs.NewStrictMem()
			if err := fs.MkdirAll("/originals", 0700); err != nil {
				t.Fatal(err)
			}
			if err := fs.MkdirAll("/db", 0700); err != nil {
				t.Fatal(err)
			}
			root, err := fs.OpenDir("/")
			if err != nil {
				t.Fatal(err)
			}
			if err := root.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			open := func() *store.Store {
				s, err := store.Open("/db", store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: "/originals", Now: func() time.Time { return time.Unix(1000000, 0) }, SyncInterval: time.Hour, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(fs))
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			target := open()
			// The first name stays absent, so its original bucketed ID is
			// published directly; the second name needs a new combined ID.
			putTransferPoints(t, target, names[1], []store.Point{{Timestamp: 999880, Value: 42}})
			if err := target.NativeCombineSyncForTest(); err != nil {
				t.Fatal(err)
			}
			var publishedID uint64
			target.NativeCombineHookForTest(func(got string) error {
				if got != stage {
					return nil
				}
				if stage == "combine-name-published" {
					m, err := target.Metadata(ctx, names[0])
					if err != nil {
						return err
					}
					publishedID = m.ID
					if err := target.UpdateMany(ctx, names[0], []store.Point{{Timestamp: 999930, Value: 99}}); err != nil {
						return err
					}
					if err := target.NativeCombineSyncForTest(); err != nil {
						return err
					}
				}
				fs.SetIgnoreSyncs(true)
				return errors.New("crash")
			})
			request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 2, Names: names, Combine: true}
			if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err == nil {
				t.Fatal("crash not injected")
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			target = open()
			defer target.Close()
			if stage == "combine-prepared" || stage == "combine-name-published" {
				if _, ready, err := target.ResumeNativeCombine(ctx, request); err != nil || !ready {
					t.Fatal("original donor stage not resumable without source", ready, err)
				}
			} else if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err != nil {
				t.Fatal(err)
			}
			for _, name := range names {
				snap, err := target.Snapshot(ctx, name)
				if err != nil || len(snap.Archives[0].Points) == 0 {
					t.Fatal("published history lost", name, err)
				}
			}
			if publishedID != 0 {
				m, _ := target.Metadata(ctx, names[0])
				if m.ID != publishedID {
					t.Fatal("committed bucketed ID imported twice")
				}
				snap, _ := target.Snapshot(ctx, names[0])
				last := snap.Archives[0].Points[len(snap.Archives[0].Points)-1]
				if last.Timestamp != 999930 || last.Value != 99 {
					t.Fatal("live point after partial publish lost", last)
				}
			}
		})
	}
}

func TestNativeCombineSidecarDifferential(t *testing.T) {
	for _, buckets := range []bool{false, true} {
		t.Run(fmt.Sprintf("v7=%v", buckets), func(t *testing.T) {
			ctx := context.Background()
			source, target, reference := nativeMemoryStore(t, true, buckets), nativeMemoryStore(t, true, buckets), nativeMemoryStore(t, true, buckets)
			name := "combine.sidecar"
			var points []store.Point
			for ts := int64(995000); ts < 999900; ts += 10 {
				points = append(points, store.Point{Timestamp: ts, Value: float64(ts % 91)})
			}
			putTransferPoints(t, source, name, points)
			putTransferPoints(t, source, name, []store.Point{{Timestamp: 996000, Value: 900}})
			if source.Sidecars().Count != 1 {
				t.Fatal("fixture has no sidecar", source.Sidecars())
			}
			for _, s := range []*store.Store{target, reference} {
				putTransferPoints(t, s, name, []store.Point{{Timestamp: 999880, Value: 42}, {Timestamp: 999910, Value: 7}})
			}
			records, err := source.ExportTransfer(ctx, []string{name})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reference.ImportTransfer(ctx, records); err != nil {
				t.Fatal(err)
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var wire bytes.Buffer
			if err := pin.Write(ctx, []string{name}, &wire); err != nil {
				t.Fatal(err)
			}
			r := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{name}, Combine: true}
			if _, err := target.NativeImport(ctx, r, &wire); err != nil {
				t.Fatal(err)
			}
			want, err := reference.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := target.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
			if target.Sidecars().Count != reference.Sidecars().Count {
				t.Fatal("sidecar inventory diverged")
			}
		})
	}
}
