package whisperio

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func TestFrozenWindowMaskedDifferential(t *testing.T) {
	ctx := context.Background()
	for _, classic := range []bool{false, true} {
		t.Run(fmt.Sprint("classic=", classic), func(t *testing.T) {
			codec := &ContinuationCodec{DirectLateWrites: true}
			var kv mapContinuationIO
			var m store.Metadata
			var clock func() time.Time
			if classic {
				kv, m, clock = classicFetchFixture(t)
			} else {
				clock = func() time.Time { return time.Unix(1000, 0) }
				m = store.Metadata{MetricConfig: store.MetricConfig{Name: "masked", Retentions: []store.Retention{{Step: 1, Count: 1200}, {Step: 10, Count: 240}}, AggregationMethod: store.Average, XFilesFactor: .5}}
				kv = mapContinuationIO{}
				if err := codec.Create(ctx, kv, m, clock); err != nil {
					t.Fatal(err)
				}
				var points []store.Point
				for ts := 1; ts <= 999; ts++ {
					points = append(points, store.Point{Timestamp: int64(ts), Value: float64(ts % 19)})
				}
				if err := codec.Update(ctx, kv, m, points, -1, clock, nil); err != nil {
					t.Fatal(err)
				}
				// Real late writes exercise sidecar priority and noncanonical bits.
				points = []store.Point{{Timestamp: 100, Value: math.Copysign(0, -1)}, {Timestamp: 200, Value: math.Float64frombits(0x7ff8000000000042)}}
				if err := codec.Update(ctx, kv, m, points, -1, clock, nil); err != nil {
					t.Fatal(err)
				}
			}
			for ai, a := range m.Retentions {
				from, until := max(a.Step, 1000-a.Count*a.Step+a.Step), 1000/a.Step*a.Step
				fs := newContinuationFiles(kv, true)
				w, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, clock))
				if err != nil {
					t.Fatal(err)
				}
				want, err := w.FetchArchiveWindow(whisper.ArchiveWindow{Archive: ai, From: from, Until: until})
				w.Close()
				if err != nil {
					t.Fatal(err)
				}
				for _, stride := range []int{0, 1, 2, 17, 128} {
					mask := make([]bool, len(want.Values()))
					dst := make([]float64, len(mask))
					for i := range dst {
						mask[i] = stride > 0 && i%stride == 0
						dst[i] = math.Float64frombits(0x7ff800000000beef)
					}
					if err := codec.FetchWindowInto(ctx, kv, m, ai, from, until, dst, mask, clock); err != nil {
						t.Fatal(err)
					}
					for i, value := range dst {
						expected := math.Float64bits(want.Values()[i])
						if mask[i] {
							expected = 0x7ff800000000beef
						}
						if math.Float64bits(value) != expected {
							t.Fatalf("archive%d stride%d slot%d got%x want%x", ai, stride, i, math.Float64bits(value), expected)
						}
					}
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := codec.FetchWindowInto(canceled, kv, m, 0, 1, 2, make([]float64, 1), nil, clock); err != context.Canceled {
				t.Fatal(err)
			}
		})
	}
}
