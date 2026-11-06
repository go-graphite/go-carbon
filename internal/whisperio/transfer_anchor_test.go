package whisperio

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestRestoreArchiveAnchorExactImage(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000080, 0) }
	r := []store.Retention{{Step: 10, Count: 6}, {Step: 60, Count: 10}, {Step: 600, Count: 10}}
	// More than one ring of timestamp span, no physical aliases, unordered input,
	// distinct archive grids, an empty archive, and every exceptional float bit.
	fine := []store.Point{{Timestamp: 1000070, Value: math.Inf(1)}, {Timestamp: 999900, Value: math.Copysign(0, -1)}, {Timestamp: 1000060, Value: math.Float64frombits(0x7ff8000000000042)}}
	coarse := []store.Point{{Timestamp: 999960, Value: math.Inf(-1)}, {Timestamp: 999480, Value: 0}}
	want := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "anchor", Retentions: r, AggregationMethod: store.Sum}}, Archives: []store.Archive{{Retention: r[0], Points: fine}, {Retention: r[1], Points: coarse}, {Retention: r[2]}}}
	kv := mapContinuationIO{}
	codec := &ContinuationCodec{}
	if err := codec.RestoreArchives(ctx, kv, want, clock); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Snapshot(ctx, kv, want.Metadata, clock)
	if err != nil {
		t.Fatal(err)
	}
	for i := range want.Archives {
		slices.SortFunc(want.Archives[i].Points, func(a, b store.Point) int {
			if a.Timestamp < b.Timestamp {
				return -1
			}
			if a.Timestamp > b.Timestamp {
				return 1
			}
			return 0
		})
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	series, err := codec.Fetch(ctx, kv, want.Metadata, 1000020, 1000079, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range fine {
		if p.Timestamp < int64(series.FromTime) {
			continue
		}
		i := (int(p.Timestamp) - series.FromTime) / series.Step
		if i >= len(series.Values) || math.Float64bits(series.Values[i]) != math.Float64bits(p.Value) {
			t.Fatalf("fetch changed %+v: %+v", p, series)
		}
	}
}

func TestRestoreArchiveRejectsUnrepresentableImages(t *testing.T) {
	r := store.Retention{Step: 10, Count: 6}
	for _, points := range [][]store.Point{
		{{Timestamp: 999901, Value: 1}},
		{{Timestamp: 999900, Value: 1}, {Timestamp: 999960, Value: 2}},
		{{Timestamp: 0, Value: 1}},
		{{Timestamp: 1 << 32, Value: 1}},
	} {
		kv := mapContinuationIO{}
		snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "anchor", Retentions: []store.Retention{r}, AggregationMethod: store.Sum}}, Archives: []store.Archive{{Retention: r, Points: points}}}
		if err := (&ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, time.Now); err == nil {
			t.Fatalf("accepted %+v", points)
		}
		if len(kv) != 0 {
			t.Fatal("invalid restore wrote keys")
		}
	}
}
