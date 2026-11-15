//go:build !race

package mixedread

import (
	"context"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"math"
	"testing"
)

// Race instrumentation perturbs sync.Pool and adds allocations; ownership and
// concurrency remain covered by the other bound tests under the race detector.
func TestBoundMixedAddsNoAllocationsToFrozenLegacy(t *testing.T) {
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
	p.Publish(partsread.View{Catalog: cat, Parts: []partsread.Source{partsread.Freeze([]partsread.Record{{ID: 42, Step: 1, Sample: partsread.Sample{Time: now - 100, Bits: math.Float64bits(9), Sequence: 1}}})}})
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	opts := r.opts
	opts.Snapshot = snap
	opts.UnifiedCatalog = true
	bound, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	native := testing.AllocsPerRun(1000, func() {
		if _, err := r.opts.Legacy.FetchFrozen(ctx, frozen, now-101, now-98); err != nil {
			t.Fatal(err)
		}
	})
	mixed := testing.AllocsPerRun(1000, func() {
		if err := bound.Fetch(ctx, "metric", now-101, now-98, func(s *partsread.Series) error {
			if s.Values[0] != 9 || s.Values[1] != 7 {
				t.Fatal("mixed values changed")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	valuesOnly := testing.AllocsPerRun(1000, func() {
		if err := r.opts.Legacy.FetchFrozenValues(ctx, frozen, now-101, now-98, func(int, int, int, []float64) error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
	rotating := testing.AllocsPerRun(1000, func() {
		next, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := next.Fetch(ctx, "metric", now-101, now-98, func(*partsread.Series) error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("frozen legacy=%g values-only=%g mixed=%g fresh-reader=%g allocations/request", native, valuesOnly, mixed, rotating)
	// New owns the Reader itself; generation turnover needs no additional
	// scratch or per-reader sync.Pool allocations.
	if mixed > valuesOnly || rotating > mixed+1 {
		t.Fatalf("adapter allocations: warm=%g fresh-reader=%g", mixed-valuesOnly, rotating-valuesOnly)
	}
	if mixed >= native {
		t.Fatalf("value-only mixed path failed to save an allocation (delta %g)", mixed-native)
	}
}
