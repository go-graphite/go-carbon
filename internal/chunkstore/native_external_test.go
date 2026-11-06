package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func nativeMemoryStore(t *testing.T, continuation, buckets bool) *store.Store {
	t.Helper()
	fs := vfs.NewMem()
	if err := fs.MkdirAll("/originals", 0700); err != nil {
		t.Fatal(err)
	}
	opts := store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, TimeBuckets: buckets, Now: func() time.Time { return time.Unix(1000000, 0) }}.WithFS(fs)
	if continuation {
		opts.Continuation = &whisperio.ContinuationCodec{}
		opts.IncrementalSource = "/originals"
	}
	s, err := store.Open("/db", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestNativeDifferentialPinnedAndDeferred(t *testing.T) {
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
			for i := range 129 {
				name := fmt.Sprintf("native.oracle.%03d", 128-i)
				names = append(names, name)
				putTransferPoints(t, source, name, []store.Point{{Timestamp: 999800, Value: math.Copysign(0, -1)}, {Timestamp: 999820, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 999880, Value: float64(i)}})
				if i%3 == 0 {
					for _, s := range []*store.Store{target, reference} {
						putTransferPoints(t, s, name, []store.Point{{Timestamp: 999880, Value: 42}, {Timestamp: 999910, Value: 7}})
					}
				}
			}
			pin, err := source.PinNativeSource()
			if err != nil {
				t.Fatal(err)
			}
			defer pin.Close()
			var frozen bytes.Buffer
			if err := pin.Write(ctx, names, &frozen); err != nil {
				t.Fatal(err)
			}
			// Native export skips logical-point sampling and retains raw stored keys.
			stream, err := store.NewTransferStream(bytes.NewReader(frozen.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			var records []store.TransferMetric
			for range stream.Count {
				r, err := stream.Next()
				if err != nil {
					t.Fatal(err)
				}
				r.Keys = cloneNativeKeys(r.Keys)
				records = append(records, r)
				if r.PointDigest != "" {
					t.Fatal("native export decoded logical snapshots")
				}
			}
			if _, _, err := stream.Finish(); err != nil {
				t.Fatal(err)
			}
			// Independently compare the new pinned exporter with the existing
			// donor exporter before using its capture as the import oracle.
			legacy, err := source.ExportTransfer(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			byName := make(map[string]store.TransferMetric, len(legacy))
			for _, r := range legacy {
				byName[r.Config.Name] = r
			}
			for _, r := range records {
				old := byName[r.Config.Name]
				if r.Version != old.Version || len(r.Keys) != len(old.Keys) {
					t.Fatal("native donor differs from reviewed export", r.Config.Name)
				}
				for i, kv := range r.Keys {
					if !bytes.Equal(kv.Key, old.Keys[i].Key) || !bytes.Equal(kv.Value, old.Keys[i].Value) {
						t.Fatal("native donor changed raw block bytes", r.Config.Name, i)
					}
				}
			}
			for _, r := range records {
				if _, err := reference.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
					t.Fatal(err)
				}
			}
			request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: int64(len(names)), Names: names}
			request.Candidates, err = target.NativeCandidates(ctx, names)
			if err != nil {
				t.Fatal(err)
			}
			var selected []string
			for i, name := range names {
				if request.Candidates[i/8]&(1<<uint(i%8)) != 0 {
					selected = append(selected, name)
				}
			}
			// Changes after the pin do not enter this export.
			putTransferPoints(t, source, names[1], []store.Point{{Timestamp: 999920, Value: 99}})
			var wire bytes.Buffer
			if err := pin.Write(ctx, selected, &wire); err != nil {
				t.Fatal(err)
			}
			receipt, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			if receipt.CopiedCount != len(selected) || receipt.DeferredCount != len(names)-len(selected) {
				t.Fatal(receipt)
			}
			for i := range names {
				if receipt.IsCopied(i) != (i%3 != 0) {
					t.Fatal("wrong original-plan bitmap", i)
				}
			}
			// Existing names remain unchanged by phase1, then use the reviewed
			// per-name combine as the controller's phase1 fallback does.
			for _, r := range records {
				if !containsNative(selected, r.Config.Name) {
					snapshot, err := target.Snapshot(ctx, r.Config.Name)
					if err != nil {
						t.Fatal(err)
					}
					if len(snapshot.Archives[0].Points) != 2 {
						t.Fatal("native phase1 changed an existing name")
					}
					if _, err := target.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
						t.Fatal(err)
					}
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
					t.Fatal(name, err)
				}
			}
			// A retry must return the original receipt without reading the body,
			// even after normal writes changed a successfully imported metric.
			putTransferPoints(t, target, names[1], []store.Point{{Timestamp: 999930, Value: 123}})
			before, err := target.Snapshot(ctx, names[1])
			if err != nil {
				t.Fatal(err)
			}
			again, err := target.NativeImport(ctx, request, bytes.NewReader([]byte("not a stream")))
			if err != nil || again.Digest != receipt.Digest {
				t.Fatal("retry did not recover receipt", err)
			}
			after, err := target.Snapshot(ctx, names[1])
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(before, after); err != nil {
				t.Fatal("retry modified live points", err)
			}
		})
	}
}
func cloneNativeKeys(keys []store.RelayoutKV) []store.RelayoutKV {
	out := make([]store.RelayoutKV, len(keys))
	for i, kv := range keys {
		out[i] = store.RelayoutKV{Key: bytes.Clone(kv.Key), Value: bytes.Clone(kv.Value)}
	}
	return out
}
func containsNative(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

func TestNativeRejectsCorruptionWithoutVisibility(t *testing.T) {
	ctx := context.Background()
	source, target := nativeMemoryStore(t, true, true), nativeMemoryStore(t, true, true)
	putTransferPoints(t, source, "native.corrupt", []store.Point{{Timestamp: 999880, Value: 42}})
	pin, err := source.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	var wire bytes.Buffer
	if err := pin.Write(ctx, []string{"native.corrupt"}, &wire); err != nil {
		t.Fatal(err)
	}
	request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: 1, Names: []string{"native.corrupt"}}
	data := bytes.Clone(wire.Bytes())
	data[len(data)-1] ^= 1
	if _, err := target.NativeImport(ctx, request, bytes.NewReader(data)); err == nil {
		t.Fatal("bad checksum accepted")
	}
	if _, err := target.Metadata(ctx, request.Names[0]); err == nil {
		t.Fatal("unverified history visible")
	}
	if _, err := target.NativeImport(ctx, request, bytes.NewReader(wire.Bytes())); err != nil {
		t.Fatal("clean retry failed", err)
	}
}
