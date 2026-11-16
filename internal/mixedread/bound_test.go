package mixedread

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partsread"
)

type catalogEntry struct {
	metric partsread.Metric
	legacy *LegacyBinding
}
type entryCatalog struct {
	partsread.Catalog
	entries map[string]catalogEntry
	lookups *int
}

func (c entryCatalog) LookupLegacy(n string) (partsread.Metric, any, bool) {
	if c.lookups != nil {
		*c.lookups++
	}
	e, ok := c.entries[n]
	if e.legacy == nil {
		return e.metric, nil, ok
	}
	return e.metric, e.legacy, ok
}
func (c entryCatalog) Lookup(n string) (partsread.Metric, bool) {
	m, _, ok := c.LookupLegacy(n)
	return m, ok
}

func TestBoundSnapshotOneLookupAndImmutableLifetime(t *testing.T) {
	ctx := context.Background()
	const now = 1794700800
	r, p, m := fixture(t, []partsread.Record{{ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 100, Bits: math.Float64bits(9), Sequence: 1}}})
	frozen, e := r.opts.Legacy.ResolveFrozen(ctx, "metric")
	if e != nil {
		t.Fatal(e)
	}
	binding, e := BindLegacy(frozen, m)
	if e != nil {
		t.Fatal(e)
	}
	lookups := 0
	cat := entryCatalog{Catalog: r.opts.LegacyCatalog, entries: map[string]catalogEntry{"metric": {m, binding}}, lookups: &lookups}
	p.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{partsread.Freeze([]partsread.Record{{ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 100, Bits: math.Float64bits(9), Sequence: 1}}})}})
	snap, e := p.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	defer snap.Close()
	opts := r.opts
	opts.Snapshot = snap
	opts.Parts.Budget = partsread.NewBudget(8 << 20)
	opts.UnifiedCatalog = true
	bound, e := New(opts)
	if e != nil {
		t.Fatal(e)
	}
	// Replacing/closing the publication cannot change this bound reader.
	p.Publish(partsread.View{Catalog: catalog(map[string]partsread.Metric{})})
	p.Close()
	for range 128 {
		before := lookups
		if e := bound.Fetch(ctx, "metric", now-101, now-98, func(s *partsread.Series) error {
			if s.Values[0] != 9 || s.Values[1] != 7 {
				t.Fatalf("wrong overlay: %v", s.Values)
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
		if lookups != before+1 {
			t.Fatalf("metadata lookups=%d", lookups-before)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if e := bound.Fetch(canceled, "metric", now-101, now-98, func(*partsread.Series) error { return nil }); !errors.Is(e, context.Canceled) {
		t.Fatalf("canceled=%v", e)
	}
	if bound.opts.Parts.Budget.Used() != 0 {
		t.Fatal("admission leaked")
	}
}

func TestBoundV2OnlyZeroAdapterAllocations(t *testing.T) {
	ctx := context.Background()
	const now = 1794700800
	r, p, m := fixture(t, nil)
	cat := entryCatalog{Catalog: r.opts.LegacyCatalog, entries: map[string]catalogEntry{"metric": {metric: m}}}
	p.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{partsread.Freeze([]partsread.Record{{ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 100, Bits: math.Float64bits(9), Sequence: 1}}})}})
	snap, e := p.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	defer snap.Close()
	opts := r.opts
	opts.Snapshot = snap
	opts.Parts.Budget = partsread.NewBudget(8 << 20)
	opts.UnifiedCatalog = true
	bound, e := New(opts)
	if e != nil {
		t.Fatal(e)
	}
	run := func() {
		if e := bound.Fetch(ctx, "metric", now-101, now-98, func(s *partsread.Series) error {
			if s.Values[0] != 9 {
				t.Fatal("wrong value")
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	run()
	if n := testing.AllocsPerRun(1000, run); n != 0 {
		t.Fatalf("adapter allocations=%g", n)
	}
	// Reader callers finish before the publisher drains its snapshot.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 32 {
				run()
			}
		}()
	}
	wg.Wait()
	if bound.opts.Parts.Budget.Used() != 0 {
		t.Fatal("admission leaked")
	}
}

func TestBoundLegacyOnlyAndSuppression(t *testing.T) {
	ctx := context.Background()
	const now = 1794700800
	r, p, m := fixture(t, nil)
	frozen, err := r.opts.Legacy.ResolveFrozen(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindLegacy(frozen, m)
	if err != nil {
		t.Fatal(err)
	}
	cat := entryCatalog{Catalog: r.opts.LegacyCatalog, entries: map[string]catalogEntry{"metric": {metric: m, legacy: binding}}}
	p.Publish(partsread.View{Catalog: cat})
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	for _, suppress := range []bool{false, true} {
		opts := r.opts
		opts.Snapshot = snap
		opts.UnifiedCatalog = true
		opts.SuppressLegacy = func(string) bool { return suppress }
		bound, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		if err = bound.Fetch(ctx, "metric", now-101, now-98, func(s *partsread.Series) error {
			if suppress {
				for _, v := range s.Values {
					if !math.IsNaN(v) {
						t.Fatal("suppressed history leaked")
					}
				}
			} else if s.Values[0] != 5 || s.Values[1] != 7 {
				t.Fatal("legacy-only values changed")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBoundDisjointOverlayUsesLegacyResult(t *testing.T) {
	ctx := context.Background()
	const now = 1794700800
	r, p, m := fixture(t, nil)
	frozen, err := r.opts.Legacy.ResolveFrozen(ctx, "metric")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindLegacy(frozen, m)
	if err != nil {
		t.Fatal(err)
	}
	cat := entryCatalog{Catalog: r.opts.LegacyCatalog, entries: map[string]catalogEntry{"metric": {m, binding}}}
	index, err := partsread.NewSlotIndex([]partsread.SlotBlock{{ID: m.ID, Archive: m.Archives[0], Slots: partsread.SlotRange{From: 1, Until: 2}, Key: []byte("disjoint")}}, func(context.Context, []byte, []partsread.SlotRange, func(partsread.Sample) error) error {
		t.Fatal("disjoint file read")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{index}})
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	opts := r.opts
	opts.Snapshot = snap
	opts.UnifiedCatalog = true
	opts.Parts.Budget = partsread.NewBudget(8 << 20)
	bound, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	want, err := r.opts.Legacy.Fetch(ctx, "metric", now-101, now-98)
	if err != nil {
		t.Fatal(err)
	}
	if err = bound.Fetch(ctx, "metric", now-101, now-98, func(got *partsread.Series) error {
		if len(got.Values) != len(want.Values) || len(got.Values) <= 1 || len(got.Occupied) != 0 {
			t.Fatal("did not keep legacy result", got)
		}
		for i, v := range want.Values {
			if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
				t.Fatal("changed legacy bits")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Fetch(ctx, "metric", now-101, now-98, func(got *partsread.Series) error {
		if len(got.Occupied) != 0 || len(got.Values) != len(want.Values) {
			t.Fatal("unbound disjoint query did not preserve native output", got)
		}
		for i, v := range want.Values {
			if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
				t.Fatal("unbound disjoint query changed native bits")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Disjointness cannot hide an incompatible archive policy.
	bad := m
	bad.AggregationMethod = "sum"
	p.Publish(partsread.View{Catalog: catalog(map[string]partsread.Metric{"metric": bad}), Parts: []partsread.Source{index}})
	if err := r.Fetch(ctx, "metric", now-101, now-98, func(*partsread.Series) error {
		t.Fatal("incompatible disjoint policy reached consumer")
		return nil
	}); !errors.Is(err, ErrPolicy) {
		t.Fatalf("disjoint policy mismatch=%v", err)
	}
	if bound.opts.Parts.Budget.Used() != 0 {
		t.Fatal("admission leaked")
	}
}

// Compare the frontier path to the original full-legacy overlay for physical
// wrap, occupied NaNs, late corrections, holes and zero/one-slot boundaries.
func TestBoundFrontierBitExact(t *testing.T) {
	const now int64 = 1794700800
	cases := [][]partsread.Record{
		nil,
		{{ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 99, Bits: 0x8000000000000000, Sequence: 1}}},
		{{ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 100, Bits: 0x7ff8000000000042, Sequence: 2}}, {ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 99 + 1000, Bits: math.Float64bits(9), Sequence: 3}}},
	}
	for ci, records := range cases {
		t.Run(fmt.Sprint(ci), func(t *testing.T) {
			r, p, m := fixture(t, records)
			frozen, e := r.opts.Legacy.ResolveFrozen(context.Background(), "metric")
			if e != nil {
				t.Fatal(e)
			}
			binding, e := BindLegacy(frozen, m)
			if e != nil {
				t.Fatal(e)
			}
			cat := entryCatalog{Catalog: r.opts.LegacyCatalog, entries: map[string]catalogEntry{"metric": {m, binding}}}
			p.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{partsread.Freeze(records)}})
			snap, e := p.Snapshot()
			if e != nil {
				t.Fatal(e)
			}
			defer snap.Close()
			opts := r.opts
			opts.Snapshot = snap
			opts.UnifiedCatalog = true
			bound, e := New(opts)
			if e != nil {
				t.Fatal(e)
			}
			for _, span := range [][2]int64{{now - 105, now - 98}, {now - 101, now - 99}, {now - 100, now - 99}, {now - 99, now - 99}, {now - 999, now}, {now - 2000, now - 1001}} {
				var want *partsread.Series
				if e := r.Fetch(context.Background(), "metric", span[0], span[1], func(s *partsread.Series) error {
					if s != nil {
						c := *s
						c.Values = slices.Clone(s.Values)
						want = &c
					}
					return nil
				}); e != nil {
					t.Fatal(e)
				}
				if e := bound.Fetch(context.Background(), "metric", span[0], span[1], func(got *partsread.Series) error {
					if (got == nil) != (want == nil) {
						t.Fatalf("nil span%v", span)
					}
					if got == nil {
						return nil
					}
					if got.From != want.From || got.Until != want.Until || got.Step != want.Step || len(got.Values) != len(want.Values) {
						t.Fatalf("shape span%v", span)
					}
					for i, v := range want.Values {
						if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
							t.Fatalf("bits span%v slot%d: %x != %x", span, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
						}
					}
					return nil
				}); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
