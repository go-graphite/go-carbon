package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNativeWindowGuardAliases(t *testing.T) {
	ctx := context.Background()
	s := &Store{exactBlocks: true}
	old := time.Date(2026, 10, 24, 12, 52, 0, 0, time.UTC).Unix()
	for _, tc := range []struct {
		name              string
		r                 Retention
		outside, incoming int64
		conflict          bool
	}{
		{"stale", Retention{Step: 10, Count: 10}, 1100, 1200, false},
		{"newer", Retention{Step: 10, Count: 10}, 1300, 1200, true},
		{"fourteen-days", Retention{Step: 60, Count: 14 * 24 * 60}, old, old + 14*24*60*60, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := MetricConfig{Name: "guard", Retentions: []Retention{tc.r}, AggregationMethod: Sum}
			record := func(ts int64, value float64) TransferMetric {
				t.Helper()
				r, err := s.nativeSnapshotRecord(ctx, Snapshot{Metadata: Metadata{MetricConfig: cfg}, Archives: []Archive{{tc.r, []Point{{Timestamp: ts, Value: value}}}}}, TransferVersion{}, false)
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			source, target := record(tc.incoming, math.Copysign(0, -1)), record(tc.outside, 42)
			w := NativeWindow{From: tc.incoming, Until: tc.incoming + int64(tc.r.Step)}
			if tc.name == "fourteen-days" {
				w = NativeWindow{From: time.Date(2026, 11, 6, 8, 58, 0, 0, time.UTC).Unix(), Until: time.Date(2026, 11, 10, 19, 15, 0, 0, time.UTC).Unix()}
			}
			_, _, beforeErr := s.legacyNativeCombineWindowRecord(ctx, source, target, w)
			if errors.Is(beforeErr, ErrConflict) == tc.conflict {
				t.Fatalf("fixture did not exercise the old guard: %v", beforeErr)
			}
			got, class, err := s.nativeCombineWindowRecord(ctx, source, target, w)
			if tc.conflict {
				if !errors.Is(err, ErrConflict) || class.StaleEvictedPoints != 0 {
					t.Fatal(class, err)
				}
				return
			}
			if err != nil || class.StaleEvictedPoints != 1 {
				t.Fatal(class, err)
			}
			// Full-mode ring selection is the independent oracle for stale owners.
			want, _, err := s.nativeCombineRecord(ctx, source, target)
			if err != nil || !reflect.DeepEqual(want, got) {
				t.Fatalf("window differs from full ring union: %v", err)
			}
			snap, err := s.nativeRecordSnapshot(ctx, got)
			if err != nil || len(snap.Archives[0].Points) != 1 || snap.Archives[0].Points[0].Timestamp != tc.incoming || math.Float64bits(snap.Archives[0].Points[0].Value) != math.Float64bits(math.Copysign(0, -1)) {
				t.Fatal(snap, err)
			}
		})
	}
}

func TestNativeWindowGuardRejectsOutsideRingAliases(t *testing.T) {
	ctx := context.Background()
	s := &Store{exactBlocks: true}
	r := Retention{Step: 10, Count: 10}
	cfg := MetricConfig{Name: "outside.aliases", Retentions: []Retention{r}, AggregationMethod: Sum}
	record := func(ts int64) TransferMetric {
		t.Helper()
		v, err := s.nativeSnapshotRecord(ctx, Snapshot{Metadata: Metadata{MetricConfig: cfg}, Archives: []Archive{{Retention: r, Points: []Point{{Timestamp: ts, Value: 42}}}}}, TransferVersion{}, false)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	// Present two decoded outside owners to the reference union. A canonical
	// ring cannot hold both; do not silently choose one even if both are stale.
	target := record(1000)
	target.Keys = append(target.Keys, record(1100).Keys...)
	for _, source := range []TransferMetric{record(1200), record(1210)} {
		_, _, err := s.nativeCombineWindowRecord(ctx, source, target, NativeWindow{From: 1200, Until: 1220})
		if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "has ring aliases") {
			t.Fatal("outside/outside alias guard changed", err)
		}
	}
}

func TestNativeWindowGuardLegacyDifferential(t *testing.T) {
	ctx := context.Background()
	s := &Store{exactBlocks: true}
	rng := rand.New(rand.NewPCG(8144, 283285))
	for n := range 80 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			cfg := MetricConfig{Name: "differential", Retentions: []Retention{{Step: 10, Count: 257}, {Step: 60, Count: 129}}, AggregationMethod: Sum}
			w := NativeWindow{From: 30000 + int64(rng.IntN(600)), Until: 30601 + int64(rng.IntN(600))}
			makeRecord := func(source bool) TransferMetric {
				t.Helper()
				snap := Snapshot{Metadata: Metadata{MetricConfig: cfg}}
				for _, r := range cfg.Retentions {
					a := Archive{Retention: r}
					for i := range 80 {
						ts := int64(30000 + i*r.Step)
						if rng.IntN(3) == 0 || source && !w.Contains(ts) {
							continue
						}
						v := []float64{math.Copysign(0, -1), 0, math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000042), float64(i)}[rng.IntN(6)]
						a.Points = append(a.Points, Point{Timestamp: ts, Value: v})
					}
					snap.Archives = append(snap.Archives, a)
				}
				record, err := s.nativeSnapshotRecord(ctx, snap, TransferVersion{}, false)
				if err != nil {
					t.Fatal(err)
				}
				return record
			}
			source, target := makeRecord(true), makeRecord(false)
			want, wc, we := s.legacyNativeCombineWindowRecord(ctx, source, target, w)
			got, gc, ge := s.nativeCombineWindowRecord(ctx, source, target, w)
			if we != nil || ge != nil || !reflect.DeepEqual(want, got) || wc != gc {
				t.Fatalf("legacy bytes/classes differ: errors %v/%v, classes %+v/%+v", we, ge, wc, gc)
			}
		})
	}
}
