package chunkstore

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestNativeWindowSparseRetentionCutoff(t *testing.T) {
	r := Retention{Step: 10, Count: 10}
	// Old non-aliased cells can survive on disk. A NaN is not a ring owner.
	snap := Snapshot{Archives: []Archive{{Retention: r, Points: []Point{
		{Timestamp: 1010, Value: 1}, {Timestamp: 1100, Value: 2}, {Timestamp: 1110, Value: math.Copysign(0, -1)},
		{Timestamp: 1190, Value: 3}, {Timestamp: 1200, Value: 4}, {Timestamp: 1300, Value: math.NaN()},
	}}}}
	for _, w := range []NativeWindow{{From: 1000, Until: 1100}, {From: 1000, Until: 1150}, {From: 1110, Until: 1201}} {
		got, _, err := SelectNativeWindow(snap, w, 1400)
		if err != nil {
			t.Fatal(err)
		}
		var want []Point
		for _, p := range snap.Archives[0].Points {
			if p.Timestamp > 1100 && p.Timestamp < 1300 && w.Contains(p.Timestamp) {
				want = append(want, p)
			}
		}
		if len(got.Archives[0].Points) != len(want) {
			t.Fatal(w, got, want)
		}
		for i, p := range want {
			g := got.Archives[0].Points[i]
			if p.Timestamp != g.Timestamp || math.Float64bits(p.Value) != math.Float64bits(g.Value) {
				t.Fatal(p, g)
			}
		}
	}
}

// An older importer sees exactly the same archive configuration and scoped
// record format. Empty fine archives do not change its target-wins union.
func TestNativeWindowHistoricalLegacyImporter(t *testing.T) {
	ctx := context.Background()
	s := &Store{exactBlocks: true}
	cfg := MetricConfig{Name: "mixed.versions", Retentions: []Retention{{Step: 10, Count: 10}, {Step: 100, Count: 100}}, AggregationMethod: Sum}
	snap := func(value float64) Snapshot {
		return Snapshot{Metadata: Metadata{MetricConfig: cfg}, Archives: []Archive{
			{Retention: cfg.Retentions[0], Points: []Point{{Timestamp: 2000, Value: 1}}},
			{Retention: cfg.Retentions[1], Points: []Point{{Timestamp: 1000, Value: value}, {Timestamp: 1100, Value: 3}, {Timestamp: 2000, Value: 4}}},
		}}
	}
	w := NativeWindow{From: 1000, Until: 1200}
	selected, _, err := SelectNativeWindow(snap(2), w, 2100)
	if err != nil {
		t.Fatal(err)
	}
	source, err := s.nativeSnapshotRecord(ctx, selected, TransferVersion{}, false)
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.nativeSnapshotRecord(ctx, snap(42), TransferVersion{}, false)
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := s.legacyNativeCombineWindowRecord(ctx, source, target, w)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := s.nativeCombineWindowRecord(ctx, source, target, w)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatal("legacy importer differs", err)
	}
	result, err := s.nativeRecordSnapshot(ctx, got)
	if err != nil {
		t.Fatal(err)
	}
	if result.Archives[1].Points[0].Value != 42 {
		t.Fatal("lost target point", result)
	}
}
