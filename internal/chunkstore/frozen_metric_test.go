package chunkstore_test

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func TestFrozenMetricStoreBindingAndBits(t *testing.T) {
	ctx := context.Background()
	const clock = 1794700800
	for _, cont := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "continuation"}[cont], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store")
			opts := chunkstore.Options{ExactBlocks: true, MetadataCacheSize: 1 << 20, Now: func() time.Time { return time.Unix(clock, 0) }}
			if cont {
				opts.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
				opts.TimeBuckets = true
			}
			s, err := chunkstore.Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			cfg := chunkstore.MetricConfig{Name: "m", Retentions: []chunkstore.Retention{{Step: 1, Count: 1000}}, AggregationMethod: chunkstore.Last, XFilesFactor: .5}
			if _, err = s.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			if _, err = s.ResolveFrozen(ctx, "m"); !errors.Is(err, chunkstore.ErrFrozenMetric) {
				t.Fatalf("writable=%v", err)
			}
			if err = s.UpdateMany(ctx, "m", []chunkstore.Point{{Timestamp: clock - 100, Value: math.Copysign(0, -1)}, {Timestamp: clock - 99, Value: math.Float64frombits(0x7ff8000000000042)}}); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			opts.ReadOnly = true
			s, err = chunkstore.Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			m, err := s.ResolveFrozen(ctx, "m")
			if err != nil {
				t.Fatal(err)
			}
			want, err := s.Fetch(ctx, "m", clock-101, clock-98)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.FetchFrozen(ctx, m, clock-101, clock-98)
			if err != nil {
				t.Fatal(err)
			}
			if got.FromTime != want.FromTime || got.Step != want.Step || len(got.Values) != len(want.Values) {
				t.Fatal("shape mismatch")
			}
			for i := range got.Values {
				if math.Float64bits(got.Values[i]) != math.Float64bits(want.Values[i]) {
					t.Fatalf("bits at%d", i)
				}
			}
			got.Metadata.Retentions[0].Step = 999
			m.Config().Retentions[0].Step = 998
			got, err = s.FetchFrozen(ctx, m, clock-101, clock-98)
			if err != nil || got.Step != 1 {
				t.Fatalf("metadata aliased: %v", err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err = s.FetchFrozen(canceled, m, clock-101, clock-98); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			next, err := chunkstore.Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			if _, err = next.FetchFrozen(ctx, m, clock-101, clock-98); !errors.Is(err, chunkstore.ErrFrozenMetric) {
				t.Fatalf("wrongstore=%v", err)
			}
			if _, err = s.FetchFrozen(ctx, m, clock-101, clock-98); err == nil {
				t.Fatal("closed store accepted")
			}
		})
	}
}
