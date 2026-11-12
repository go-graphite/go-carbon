package whisperio

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestClassicWriteConflictAbortsStoreTransaction(t *testing.T) {
	for _, cacheSize := range []int64{0, 1 << 20} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%d/batch=%t", cacheSize, batch), func(t *testing.T) {
				ctx := context.Background()
				clock := func() time.Time { return time.Unix(1000000, 0) }
				codec := &ContinuationCodec{}
				kv, snapshot := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999780, Value: 6}, {Timestamp: 999990, Value: 3}})
				if err := codec.Update(ctx, kv, snapshot.Metadata, []store.Point{{Timestamp: 999800, Value: 1}}, -1, clock, nil); err != nil {
					t.Fatal(err)
				}
				opts := coldOptions(codec)
				opts.Now, opts.MetadataCacheSize = clock, cacheSize
				db, err := store.Open(t.TempDir(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				db.StopBackground()
				record := store.TransferMetric{Config: snapshot.Metadata.MetricConfig, Version: store.TransferVersion{Name: snapshot.Metadata.Name}, Continuation: true}
				for key, value := range kv {
					record.Keys = append(record.Keys, store.RelayoutKV{Key: []byte(key), Value: value})
				}
				slices.SortFunc(record.Keys, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
				if _, err := db.ImportTransfer(ctx, []store.TransferMetric{record}); err != nil {
					t.Fatal(err)
				}
				name := snapshot.Metadata.Name
				before, err := db.ExportTransfer(ctx, []string{name})
				if err != nil {
					t.Fatal(err)
				}
				// Stage a harmless update before the conflicting update. Neither
				// its bytes nor the metadata revision may survive the error.
				first := []store.Point{{Timestamp: 999810, Value: 7}}
				conflict := []store.Point{{Timestamp: 999980, Value: 9}}
				if batch {
					err = db.UpdateBatch(ctx, []store.MetricUpdate{{Name: name, Points: first}, {Name: name, Points: conflict}}, nil)
				} else {
					err = db.UpdateMany(ctx, name, append(first, conflict...))
				}
				if err == nil || !strings.Contains(err.Error(), "unrelated physical history") {
					t.Fatalf("unsafe transaction succeeded: %v", err)
				}
				after, err := db.ExportTransfer(ctx, []string{name})
				if err != nil {
					t.Fatal(err)
				}
				if before[0].Version != after[0].Version || len(before[0].Keys) != len(after[0].Keys) {
					t.Fatal("failed update changed catalog or key count")
				}
				for i, pair := range before[0].Keys {
					if !bytes.Equal(pair.Key, after[0].Keys[i].Key) || !bytes.Equal(pair.Value, after[0].Keys[i].Value) {
						t.Fatal("failed update changed physical keys")
					}
				}
			})
		}
	}
}

// Old native images placed classic points in epoch-relative slots. Once a
// foreground write establishes a nonzero slot-zero anchor, Whisper addresses
// different slots and can overwrite unrelated, still-retained physical points.
func TestClassicUpdateRejectsUnrelatedPhysicalHistory(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000000, 0) }
	codec := &ContinuationCodec{DirectLateWrites: true}
	kv, snapshot := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999780, Value: 6}, {Timestamp: 999990, Value: 3}})
	if err := codec.Update(ctx, kv, snapshot.Metadata, []store.Point{{Timestamp: 999800, Value: 1}}, -1, clock, nil); err != nil {
		t.Fatal(err)
	}
	before, err := codec.Snapshot(ctx, kv, snapshot.Metadata, clock)
	if err != nil {
		t.Fatal(err)
	}
	// 999980 maps to slot 18 relative to anchor 999800, occupied by
	// 999780 in the old absolute layout. Their distance is not one ring.
	batch := newContinuationOverlay(kv)
	err = codec.Update(ctx, batch, snapshot.Metadata, []store.Point{{Timestamp: 999980, Value: 9}}, -1, clock, nil)
	if err == nil || !strings.Contains(err.Error(), "unrelated physical history") {
		after, _ := codec.Snapshot(ctx, batch, snapshot.Metadata, clock)
		t.Fatalf("unsafe update must fail before publication; err=%v before=%+v after=%+v", err, before.Archives, after.Archives)
	}
	// The store discards the indexed batch on an update error.
	after, err := codec.Snapshot(ctx, kv, snapshot.Metadata, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(before, after); err != nil {
		t.Fatal(err)
	}
}

func TestClassicUpdateAllowsCorrectionAndRingWrap(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{}
	kv, snapshot := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999600, Value: 1}, {Timestamp: 999780, Value: 2}})
	now := int64(1000000)
	clock := func() time.Time { return time.Unix(now, 0) }
	for _, point := range []store.Point{{Timestamp: 999780, Value: 4}, {Timestamp: 1000200, Value: 5}} {
		if point.Timestamp > now {
			now = 1000400
		}
		if err := codec.Update(ctx, kv, snapshot.Metadata, []store.Point{point}, -1, clock, nil); err != nil {
			t.Fatal(err)
		}
	}
	got, err := codec.Snapshot(ctx, kv, snapshot.Metadata, clock)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]float64{999780: 4, 1000200: 5}
	for _, p := range got.Archives[0].Points {
		if v, ok := want[p.Timestamp]; ok && v == p.Value {
			delete(want, p.Timestamp)
		}
	}
	if len(want) != 0 {
		t.Fatalf("lost legitimate updates: %v", want)
	}
}
