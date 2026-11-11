package whisperio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeWindowPatchGuardAliases(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{}
	clock := func() time.Time { return time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC) }
	old := time.Date(2026, 10, 24, 12, 52, 0, 0, time.UTC).Unix()
	for _, tc := range []struct {
		name              string
		r                 store.Retention
		outside, incoming int64
		conflict          bool
	}{
		{"stale", store.Retention{Step: 10, Count: 10}, 1100, 1200, false},
		{"newer", store.Retention{Step: 10, Count: 10}, 1300, 1200, true},
		{"fourteen-days", store.Retention{Step: 60, Count: 14 * 24 * 60}, old, old + 14*24*60*60, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := store.MetricConfig{Name: "guard", Retentions: []store.Retention{tc.r}, AggregationMethod: store.Sum}
			snap := func(ts int64, value float64) store.Snapshot {
				return store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: tc.r, Points: []store.Point{{Timestamp: ts, Value: value}}}}}
			}
			kv := mapContinuationIO{}
			if err := codec.RestoreArchives(ctx, kv, snap(tc.outside, 42), clock); err != nil {
				t.Fatal(err)
			}
			before := maps.Clone(kv)
			source := snap(tc.incoming, math.Copysign(0, -1))
			w := store.NativeWindow{From: tc.incoming, Until: tc.incoming + int64(tc.r.Step)}
			if tc.name == "fourteen-days" {
				w = store.NativeWindow{From: time.Date(2026, 11, 6, 8, 58, 0, 0, time.UTC).Unix(), Until: time.Date(2026, 11, 10, 19, 15, 0, 0, time.UTC).Unix()}
			}
			_, _, beforeErr := codec.legacyPlanNativeWindowPatch(ctx, kv, source, clock, w)
			if errors.Is(beforeErr, store.ErrConflict) == tc.conflict {
				t.Fatalf("fixture did not exercise the old guard: %v", beforeErr)
			}
			writes, class, handled, err := codec.PlanNativeWindowPatch(ctx, kv, source, clock, w)
			if !reflect.DeepEqual(before, kv) {
				t.Fatal("planning mutated the target")
			}
			if tc.conflict {
				if !errors.Is(err, store.ErrConflict) || handled || len(writes) != 0 || class.StaleEvictedPoints != 0 {
					t.Fatal(class, handled, err)
				}
				return
			}
			if err != nil || !handled || class.StaleEvictedPoints != 1 {
				t.Fatal(class, handled, err)
			}
			applyWindowGuardWrites(kv, writes)
			got, err := codec.Snapshot(ctx, kv, source.Metadata, clock)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EqualSnapshots(source, got); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func applyWindowGuardWrites(kv mapContinuationIO, writes []store.RelayoutKV) {
	for _, entry := range writes {
		if entry.Value == nil {
			delete(kv, string(entry.Key))
		} else {
			kv[string(entry.Key)] = entry.Value
		}
	}
}

func TestNativeWindowPatchGuardLegacyDifferential(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{}
	clock := func() time.Time { return time.Unix(100000, 0) }
	rng := rand.New(rand.NewPCG(8144, 283285))
	for n := range 80 {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			cfg := store.MetricConfig{Name: "differential", Retentions: []store.Retention{{Step: 10, Count: 257}, {Step: 60, Count: 129}}, AggregationMethod: store.Sum}
			w := store.NativeWindow{From: 30000 + int64(rng.IntN(600)), Until: 30601 + int64(rng.IntN(600))}
			makeSnapshot := func(source bool) store.Snapshot {
				snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}}
				for _, r := range cfg.Retentions {
					a := store.Archive{Retention: r}
					for i := range 80 {
						ts := int64(30000 + i*r.Step)
						if rng.IntN(3) == 0 || source && !w.Contains(ts) {
							continue
						}
						v := []float64{math.Copysign(0, -1), 0, math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000042), float64(i)}[rng.IntN(6)]
						if source && math.IsNaN(v) {
							continue // The patch receives the normalized donor window.
						}
						a.Points = append(a.Points, store.Point{Timestamp: ts, Value: v})
					}
					snap.Archives = append(snap.Archives, a)
				}
				return snap
			}
			source, target := makeSnapshot(true), makeSnapshot(false)
			kv := mapContinuationIO{}
			if err := codec.RestoreArchives(ctx, kv, target, clock); err != nil {
				t.Fatal(err)
			}
			want, wh, we := codec.legacyPlanNativeWindowPatch(ctx, kv, source, clock, w)
			got, class, gh, ge := codec.PlanNativeWindowPatch(ctx, kv, source, clock, w)
			sortWrites := func(writes []store.RelayoutKV) {
				slices.SortFunc(writes, func(a, b store.RelayoutKV) int { return bytes.Compare(a.Key, b.Key) })
			}
			sortWrites(want)
			sortWrites(got)
			if we != nil || ge != nil || !wh || !gh || class.StaleEvictedPoints != 0 || !reflect.DeepEqual(want, got) {
				t.Fatalf("legacy patch bytes differ: errors %v/%v, handled %v/%v, class %+v", we, ge, wh, gh, class)
			}
		})
	}
}
