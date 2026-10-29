package chunkstore

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestReconcileStatusMatchesIndependentSnapshotOracle(t *testing.T) {
	t.Parallel()
	for _, exact := range []bool{false, true} {
		db, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: exact})
		ctx := context.Background()
		r := Retention{Step: 2, Count: 512}
		base := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "status", Retentions: []Retention{r}, AggregationMethod: Last, XFilesFactor: .5}}, Archives: []Archive{{Retention: r, Points: []Point{{Timestamp: 256, Value: math.Copysign(0, -1)}, {Timestamp: 512, Value: math.Float64frombits(0x7ff8000000001234)}, {Timestamp: 1152, Value: 9}}}}}
		for kind := range 5 {
			source := base
			source.Archives = append([]Archive(nil), base.Archives...)
			source.Archives[0].Points = append([]Point(nil), base.Archives[0].Points...)
			switch kind {
			case 1:
				source.Archives[0].Points[0], source.Archives[0].Points[1] = source.Archives[0].Points[1], source.Archives[0].Points[0]
			case 2:
				source.Archives[0].Points = append(source.Archives[0].Points, source.Archives[0].Points[0])
			case 3:
				source.Archives[0].Points = append([]Point{{Timestamp: 128, Value: 7}}, source.Archives[0].Points...)
			case 4:
				source.Archives[0].Points[0].Timestamp++
			}
			if _, err := db.Replace(ctx, source); err != nil {
				t.Fatal(err)
			}
			actual, err := db.Snapshot(ctx, source.Metadata.Name)
			if err != nil {
				t.Fatal(err)
			}
			oracle := EqualSnapshots(source, actual) == nil
			before := actual.Metadata
			got, unchanged, err := db.ReconcileSnapshotStatus(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if unchanged != oracle {
				t.Fatalf("exact=%v case=%d certificate=%v oracle=%v", exact, kind, unchanged, oracle)
			}
			if got.ID != before.ID || got.Generation != before.Generation || got.Revision != before.Revision {
				t.Fatal("unchanged physical image was written")
			}
		}
		closeTestStore(t, db)
	}
}

func TestReconcileStatusRequiresReadbackAfterChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true})
	defer closeTestStore(t, db)
	r := Retention{Step: 1, Count: 256}
	source := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "status", Retentions: []Retention{r}, AggregationMethod: Last}}, Archives: []Archive{{Retention: r, Points: []Point{{Timestamp: 128, Value: 1}}}}}
	for _, value := range []float64{1, math.Copysign(0, -1), math.Float64frombits(0x7ff8000000004567)} {
		source.Archives[0].Points[0].Value = value
		if _, unchanged, err := db.ReconcileSnapshotStatus(ctx, source); err != nil || unchanged {
			t.Fatalf("changed image certified: %v %v", unchanged, err)
		}
		actual, err := db.Snapshot(ctx, source.Metadata.Name)
		if err != nil {
			t.Fatal(err)
		}
		if err := EqualSnapshots(source, actual); err != nil {
			t.Fatal(err)
		}
		if _, unchanged, err := db.ReconcileSnapshotStatus(ctx, source); err != nil || !unchanged {
			t.Fatalf("equal image not certified: %v %v", unchanged, err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, unchanged, err := db.ReconcileSnapshotStatus(ctx, source); err != nil || !unchanged {
		t.Fatal(unchanged, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, unchanged, err := db.ReconcileSnapshotStatus(cancelled, source); !errors.Is(err, context.Canceled) || unchanged {
		t.Fatal(unchanged, err)
	}
}
