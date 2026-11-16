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

func TestFrozenWindowStoreLifetimeAndMask(t *testing.T) {
	ctx := context.Background()
	const now = 1794700800
	path := filepath.Join(t.TempDir(), "legacy")
	opts := chunkstore.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{DirectLateWrites: true}, Now: func() time.Time { return time.Unix(now, 0) }}
	s, err := chunkstore.Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	cfg := chunkstore.MetricConfig{Name: "m", Retentions: []chunkstore.Retention{{Step: 1, Count: 1000}, {Step: 10, Count: 1000}}, AggregationMethod: chunkstore.Last, XFilesFactor: .5}
	if _, err := s.Create(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	points := []chunkstore.Point{{Timestamp: now - 100, Value: math.Copysign(0, -1)}, {Timestamp: now - 99, Value: math.Float64frombits(0x7ff8000000000042)}}
	if err := s.UpdateMany(ctx, "m", points); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opts.ReadOnly = true
	s, err = chunkstore.Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()
	frozen, err := s.ResolveFrozen(ctx, "m")
	if err != nil {
		t.Fatal(err)
	}
	want, err := s.Fetch(ctx, "m", now-105, now-95)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]float64, len(want.Values))
	mask := make([]bool, len(dst))
	for i := range dst {
		dst[i] = math.Float64frombits(0x7ff800000000beef)
		mask[i] = i%3 == 0
	}
	for run := 0; run < 3; run++ {
		if err := s.FetchFrozenWindowInto(ctx, frozen, 0, want.FromTime, want.UntilTime, dst, mask); err != nil {
			t.Fatal(err)
		}
		for i, value := range dst {
			expected := math.Float64bits(want.Values[i])
			if mask[i] {
				expected = 0x7ff800000000beef
			}
			if math.Float64bits(value) != expected {
				t.Fatalf("slot%d got%x want%x", i, math.Float64bits(value), expected)
			}
		}
	}
	if err := s.FetchFrozenWindowInto(ctx, nil, 0, want.FromTime, want.UntilTime, dst, mask); !errors.Is(err, chunkstore.ErrFrozenMetric) {
		t.Fatal(err)
	}
	if err := s.FetchFrozenWindowInto(ctx, frozen, 0, want.FromTime, want.UntilTime, dst, mask[:1]); !errors.Is(err, chunkstore.ErrFormat) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.FetchFrozenWindowInto(canceled, frozen, 0, want.FromTime, want.UntilTime, dst, mask); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if err := s.FetchFrozenWindowInto(ctx, frozen, 0, want.FromTime, want.UntilTime, dst, mask); !errors.Is(err, chunkstore.ErrClosed) {
		t.Fatal(err)
	}
}
