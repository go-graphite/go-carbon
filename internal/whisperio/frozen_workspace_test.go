package whisperio

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestFrozenWorkspaceMatchesFetchAcrossQueriesAndStores(t *testing.T) {
	ctx := context.Background()
	const now = 1794700800
	var workspace store.FrozenReadWorkspace
	defer workspace.Reset()
	for _, method := range []store.AggregationMethod{store.Average, store.Sum, store.Last, store.Min, store.Max} {
		fs := vfs.NewMem()
		var rangeReads bool
		var rangeSamples int
		opts := store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true, RangeReadsEnabled: func() bool { rangeSamples++; return rangeReads }}, Now: func() time.Time { return time.Unix(now, 0) }}
		s, err := store.Open("legacy", opts.WithFS(fs))
		if err != nil {
			t.Fatal(err)
		}
		cfg := store.MetricConfig{Name: "m", Retentions: []store.Retention{{Step: 1, Count: 1000}, {Step: 10, Count: 1000}}, AggregationMethod: method, XFilesFactor: .5}
		if _, err := s.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		var points []store.Point
		for ts := int64(now - 3000); ts < now; ts++ {
			value := float64(ts%19) - 9
			if ts%137 == 0 {
				value = math.Copysign(0, -1)
			}
			points = append(points, store.Point{Timestamp: ts, Value: value})
		}
		if err := s.UpdateMany(ctx, cfg.Name, points); err != nil {
			t.Fatal(err)
		}
		if err := s.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: now - 1234, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: now - 107, Value: -17}}); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		opts.ReadOnly = true
		s, err = store.Open("legacy", opts.WithFS(fs))
		if err != nil {
			t.Fatal(err)
		}
		m, err := s.ResolveFrozen(ctx, cfg.Name)
		if err != nil {
			t.Fatal(err)
		}
		for repeat := range 3 {
			rangeReads = repeat%2 != 0
			for _, bounds := range [][2]int{{now - 1000, now}, {now - 60, now}, {now - 10000, now}, {now - 3001, now - 1231}, {now, now}, {now + 1, now + 2}, {0, 1}, {now - 91, now - 91}} {
				want, err := s.Fetch(ctx, cfg.Name, bounds[0], bounds[1])
				if err != nil {
					t.Fatal(err)
				}
				called := false
				samplesBefore := rangeSamples
				err = s.FetchFrozenValuesWithWorkspace(ctx, m, bounds[0], bounds[1], &workspace, func(from, until, step int, values []float64) error {
					called = true
					if want == nil || from != want.FromTime || until != want.UntilTime || step != want.Step || len(values) != len(want.Values) {
						t.Fatalf("shape method=%v bounds=%v repeat=%d", method, bounds, repeat)
					}
					for i, value := range values {
						if math.Float64bits(value) != math.Float64bits(want.Values[i]) {
							t.Fatalf("bits method=%v bounds=%v repeat=%d slot=%d", method, bounds, repeat, i)
						}
						values[i] = 123456 // Consumer ownership must not corrupt decoder scratch.
					}
					return nil
				})
				if rangeSamples != samplesBefore+1 {
					t.Fatal("range-read switch must be sampled once per workspace Fetch")
				}
				if err != nil || called != (want != nil) {
					t.Fatalf("callback/nil mismatch: %v", err)
				}
			}
		}
		// The next method reuses the same IDs in a distinct immutable store.
		// Closing without resetting the workspace must not pin a read lifetime.
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
