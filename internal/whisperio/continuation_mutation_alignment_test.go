package whisperio

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestContinuationMutationProofUnalignedRollupScope(t *testing.T) {
	ctx := context.Background()
	now := int64(1700000000)
	clock := func() time.Time { return time.Unix(now, 0) }
	open := func() *store.Store {
		t.Helper()
		s, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, Now: clock})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	}
	tracked, plain := open(), open()
	c := store.MetricConfig{Name: "proof", Retentions: []store.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}, AggregationMethod: store.Average, XFilesFactor: .5}
	for _, s := range []*store.Store{tracked, plain} {
		if _, err := s.Create(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.UpdateMany(ctx, c.Name, []store.Point{{Timestamp: now - 100, Value: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	proof := func() string {
		t.Helper()
		p, _, err := tracked.MutationProof(ctx, c.Name, now-101, now-1)
		if err != nil || p == "" {
			t.Fatalf("proof=%q err=%v", p, err)
		}
		return p
	}
	before := proof()
	for i, batch := range [][]store.Point{
		{{Timestamp: now, Value: 3}}, // A lower-archive buffer crosses the scope boundary.
		{{Timestamp: now - 100, Value: 2}},
		{{Timestamp: now - 60, Value: math.Copysign(0, -1)}, {Timestamp: now - 59, Value: math.Float64frombits(1)}},
		{{Timestamp: now - 60, Value: math.Float64frombits(0x7ff8000000001234)}},
	} {
		for _, s := range []*store.Store{tracked, plain} {
			if err := s.UpdateMany(ctx, c.Name, batch); err != nil {
				t.Fatalf("batch %d: %v", i, err)
			}
		}
		after := proof()
		if i == 0 && after != before {
			t.Fatal("out-of-window append invalidated proof")
		}
		if i == 1 && after == before {
			t.Fatal("in-window correction retained stale proof")
		}
		want, err := plain.SumDigest(ctx, c.Name, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		got, err := tracked.SumDigest(ctx, c.Name, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("batch %d: digests differ\ngot %+v\nwant %+v", i, got, want)
		}
		for _, age := range []int64{100, 90000, 2700000, 32000000} {
			a, err := tracked.Fetch(ctx, c.Name, int(now-age), int(now))
			if err != nil {
				t.Fatal(err)
			}
			b, err := plain.Fetch(ctx, c.Name, int(now-age), int(now))
			if err != nil {
				t.Fatal(err)
			}
			if a.FromTime != b.FromTime || a.Step != b.Step || len(a.Values) != len(b.Values) {
				t.Fatal("series bounds differ")
			}
			for j, v := range a.Values {
				if math.Float64bits(v) != math.Float64bits(b.Values[j]) {
					t.Fatalf("batch %d age %d slot %d: value bits differ", i, age, j)
				}
			}
		}
	}
}

func TestContinuationMutationProofWideClassicScope(t *testing.T) {
	ctx := context.Background()
	now := int64(1700000040) // A minute boundary leaves the unrounded upper fence off grid.
	clock := func() time.Time { return time.Unix(now, 0) }
	r := store.Retention{Step: 60, Count: 120}
	kv, m := absoluteClassicImage(t, []store.Retention{r}, [][]store.Point{{{Timestamp: now - 120, Value: 1}}})
	s, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{}, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.ImportTransfer(ctx, []store.TransferMetric{classicTransfer(kv, store.Snapshot{Metadata: m})}); err != nil {
		t.Fatal(err)
	}
	proof := func() (string, string) {
		t.Helper()
		p, reason, err := s.MutationProof(ctx, m.Name, now-2*int64(r.MaxRetention()), now+2*int64(r.MaxRetention()))
		if err != nil || p == "" {
			t.Fatalf("proof=%q err=%v", p, err)
		}
		return p, reason
	}
	before, _ := proof()
	if err = s.UpdateMany(ctx, m.Name, []store.Point{{Timestamp: now - 60, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	after, reason := proof()
	if after == before || reason != "unknown mutation path" {
		t.Fatalf("oversized comparison must invalidate the proof: %s", reason)
	}
	series, err := s.Fetch(ctx, m.Name, int(now-120), int(now))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i, v := range series.Values {
		if int64(series.FromTime+i*series.Step) == now-60 && v == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("write was not persisted")
	}
}
