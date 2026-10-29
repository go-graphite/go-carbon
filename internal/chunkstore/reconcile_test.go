package chunkstore

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestReconcileSnapshotMatchesReplacementAndSkipsEqualChunks(t *testing.T) {
	t.Parallel()
	for _, exact := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "exact"}[exact], func(t *testing.T) {
			ctx := context.Background()
			opts := Options{fs: vfs.NewMem(), ExactBlocks: exact, Now: func() time.Time { return time.Unix(1800000000, 0) }}
			db, err := Open(filepath.Join(t.TempDir(), "actual"), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			oracle, err := Open(filepath.Join(t.TempDir(), "oracle"), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer oracle.Close()
			r := Retention{Step: 1, Count: 1024}
			source := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "reconcile.bits", Retentions: []Retention{r}, AggregationMethod: Last, XFilesFactor: .5}}, Archives: []Archive{{Retention: r}}}
			for i := int64(128); i < 144; i++ {
				source.Archives[0].Points = append(source.Archives[0].Points, Point{Timestamp: i, Value: float64(i)})
			}
			source.Archives[0].Points = append(source.Archives[0].Points, Point{Timestamp: 512, Value: 8}, Point{Timestamp: 768, Value: math.Copysign(0, -1)}, Point{Timestamp: 769, Value: math.Float64frombits(0x7ff8000000000042)}, Point{Timestamp: 770, Value: math.Inf(1)})
			if _, err = db.Replace(ctx, source); err != nil {
				t.Fatal(err)
			}
			// Exercise comparison through an existing merge chain.
			if err = db.UpdateManyForArchive(ctx, source.Metadata.Name, []Point{{Timestamp: 129, Value: 999}}, 1024); err != nil {
				t.Fatal(err)
			}
			old, err := db.Metadata(ctx, source.Metadata.Name)
			if err != nil {
				t.Fatal(err)
			}
			points := source.Archives[0].Points[:0]
			for _, p := range source.Archives[0].Points {
				if p.Timestamp == 130 || p.Timestamp == 512 {
					continue
				}
				if p.Timestamp == 129 {
					p.Value = math.Copysign(0, -1)
				}
				points = append(points, p)
			}
			source.Archives[0].Points = points
			// Out-of-order input and a newer circular owner share one slot.
			source.Archives[0].Points = append(source.Archives[0].Points, Point{Timestamp: 1280, Value: 11}, Point{Timestamp: 256, Value: math.Float64frombits(0x7ff8000000000077)})
			before := db.Stats()
			m, err := db.ReconcileSnapshot(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if m.ID != old.ID || m.Generation != old.Generation || m.Revision != old.Revision+1 {
				t.Fatal("compatible reconciliation changed identity or lost revision")
			}
			if _, err = oracle.Replace(ctx, source); err != nil {
				t.Fatal(err)
			}
			want, err := oracle.Snapshot(ctx, source.Metadata.Name)
			if err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				got, e := db.Snapshot(ctx, source.Metadata.Name)
				if e != nil {
					t.Fatal(e)
				}
				if e = EqualSnapshots(want, got); e != nil {
					t.Fatal(e)
				}
			}
			check()
			after := db.Stats()
			if after.ReconcileWrittenChunks-before.ReconcileWrittenChunks != 2 || after.ReconcileDeletedChunks-before.ReconcileDeletedChunks != 1 {
				t.Fatalf("unexpected differential writes: %+v", after)
			}
			m2, err := db.ReconcileSnapshot(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if m2.Revision != m.Revision || db.Stats().ReconcileWrittenChunks != after.ReconcileWrittenChunks || db.Stats().ReconcileUnchangedMetrics != after.ReconcileUnchangedMetrics+1 {
				t.Fatal("equal image caused a write")
			}
			if err = db.Flush(); err != nil {
				t.Fatal(err)
			}
			check()
			// Policy changes preserve unconditional replacement semantics.
			source.Metadata.Retentions[0].Count = 2048
			source.Archives[0].Retention.Count = 2048
			m3, err := db.ReconcileSnapshot(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if m3.Generation != m.Generation+1 {
				t.Fatal("policy change did not replace generation")
			}
			if _, err = oracle.Replace(ctx, source); err != nil {
				t.Fatal(err)
			}
			want, err = oracle.Snapshot(ctx, source.Metadata.Name)
			if err != nil {
				t.Fatal(err)
			}
			check()
		})
	}
}

func TestReconcileSnapshotCancellationPreservesImage(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	source := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "reconcile.cancel", Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Last}}, Archives: []Archive{{Retention: Retention{Step: 1, Count: 100}, Points: []Point{{Timestamp: 1, Value: 3}}}}}
	if _, err = db.ReconcileSnapshot(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	want, err := db.Snapshot(context.Background(), source.Metadata.Name)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source.Archives[0].Points[0].Value = 9
	if _, err = db.ReconcileSnapshot(ctx, source); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled reconciliation accepted")
	}
	got, err := db.Snapshot(context.Background(), source.Metadata.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err = EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileSnapshotRepairsUndecodableExistingChunk(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	source := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "reconcile.corrupt", Retentions: []Retention{{Step: 1, Count: 100}}, AggregationMethod: Last}}, Archives: []Archive{{Retention: Retention{Step: 1, Count: 100}, Points: []Point{{Timestamp: 1, Value: 3}}}}}
	m, err := db.Replace(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.db.Set(chunkKey(m, 0, 0), []byte("invalid"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	next, err := db.ReconcileSnapshot(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if next.Generation != m.Generation+1 {
		t.Fatal("unsafe comparison did not use full replacement")
	}
	got, err := db.Snapshot(context.Background(), source.Metadata.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err = EqualSnapshots(source, got); err != nil {
		t.Fatal(err)
	}
}
