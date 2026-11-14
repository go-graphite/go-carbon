package chunkstore

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestDigestBatchSnapshotAndMissing(t *testing.T) {
	ctx := context.Background()
	s, err := Open(t.TempDir(), Options{Now: func() time.Time { return time.Unix(10000, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	names := []string{"z", "a", "missing"}
	for _, name := range names[:2] {
		if _, err = s.Create(ctx, MetricConfig{Name: name, Retentions: []Retention{{Step: 10, Count: 100}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
		if err = s.UpdateMany(ctx, name, []Point{{Timestamp: 9900, Value: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := s.Fetch(ctx, "z", 9899, 9910)
	if err != nil {
		t.Fatal(err)
	}
	visited := make([]bool, len(names))
	changed := false
	err = s.DigestBatch(ctx, names, func(ctx context.Context, i int) error {
		visited[i] = true
		if !changed {
			changed = true
			if err := s.UpdateMany(ctx, "z", []Point{{Timestamp: 9900, Value: 99}}); err != nil {
				return err
			}
		}
		series, err := s.Fetch(ctx, names[i], 9899, 9910)
		if names[i] == "missing" {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing: %v", err)
			}
			return nil
		}
		if err != nil {
			return err
		}
		if names[i] == "z" {
			if series.FromTime != before.FromTime || series.UntilTime != before.UntilTime || series.Step != before.Step || len(series.Values) != len(before.Values) {
				t.Fatal("batch changed series bounds")
			}
			for j, v := range series.Values {
				if math.Float64bits(v) != math.Float64bits(before.Values[j]) {
					t.Fatal("batch snapshot changed")
				}
			}
		}
		_, err = s.SumDigest(ctx, names[i], 9899, 9910)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range visited {
		if !v {
			t.Fatalf("not visited %d", i)
		}
	}
	after, err := s.Fetch(ctx, "z", 9899, 9910)
	if err != nil {
		t.Fatal(err)
	}
	if after.Values[0] != 99 {
		t.Fatal("batch polluted live metadata/state cache")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.DigestBatch(canceled, names, func(context.Context, int) error { t.Fatal("callback after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
