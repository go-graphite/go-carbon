package v2read

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2core"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestResolvedPhysicalSourceMatchesFrozen(t *testing.T) {
	archives := []sealrollup.Archive{{Step: 1, Count: 10}, {Step: 10, Count: 10}}
	rec := func(a int, slot, ts int64, bits, order uint64) sealrollup.Record {
		return sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: slot, Order: order, Sample: sealrollup.Sample{Time: ts, Bits: bits, Sequence: order}}}
	}
	base := []sealrollup.Record{rec(1, 2, 120, 0x7ff8000000000042, 1), rec(0, 1, 101, math.Float64bits(1), 1), rec(0, 2, 102, math.Float64bits(2), 1)}
	source := newResolvedSource(7, archives, base)
	updates := []sealrollup.Record{rec(0, 1, 111, 1<<63, 2), rec(0, 3, 103, 0, 2), rec(1, 2, 220, math.Float64bits(3), 2)}
	next := source.replace(updates)
	if p, _ := source.view(0, 1); p.Time != 101 {
		t.Fatal("old publication mutated")
	}
	all := append(slices.Clone(base), updates...)
	var frozen []Record
	for _, r := range all {
		frozen = append(frozen, Record{ID: 7, Step: archives[r.Archive].Step, Sample: Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}})
	}
	for _, ar := range archives {
		a := Archive{ar.Step, ar.Count}
		for from := int64(80); from < 250; from += ar.Step {
			for width := int64(0); width <= ar.Count; width++ {
				until := from + width*ar.Step
				compare := func(src Source) map[int64]Sample {
					out := map[int64]Sample{}
					err := src.Scan(context.Background(), 7, a, from, until, func(p Sample) error {
						slot := (p.Time / ar.Step) % ar.Count
						if old, ok := out[slot]; !ok || old.Sequence < p.Sequence {
							out[slot] = p
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
					return out
				}
				want, got := compare(Freeze(frozen)), compare(next)
				if len(want) != len(got) {
					t.Fatalf("range %d %d: %v != %v", from, until, got, want)
				}
				for slot, p := range want {
					if got[slot] != p {
						t.Fatalf("range %d %d slot %d: %v != %v", from, until, slot, got[slot], p)
					}
				}
			}
		}
	}
}

func TestResolvedCacheIsBoundedAndGenerationScoped(t *testing.T) {
	// Empty snapshots are enough to exercise scalar-source ownership and limits;
	// no cached entry may retain a snapshot after the caller closes it.
	r, _ := New(1, 1<<20, 1<<16)
	engine, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishBatches: r.Publish})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	defer r.Close()
	await(t, func() bool { return r.owners[0].Load() != nil })
	g := r.owners[0].Load()
	if !g.retain() {
		t.Fatal("retain")
	}
	defer g.release()
	g.load()
	if g.err != nil {
		t.Fatal(g.err)
	}
	c := &sealedCache{limit: 1024}
	sealed := &generation{sealed: g.sealed, cache: c}
	policy := v2core.RollupPolicy{Config: sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 10}}}}
	for i := 0; i < 100; i++ {
		sealed.number = uint64(i)
		if _, err := sealed.resolvedContext(context.Background(), fmt.Sprint(i), policy, v2core.RollupMetadata{ID: 1}); err != nil {
			t.Fatal(err)
		}
		if c.fullBytes > c.limit || len(c.full) > 64 {
			t.Fatal("unbounded scalar cache")
		}
	}
	old, err := sealed.resolvedContext(context.Background(), "repeat", policy, v2core.RollupMetadata{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	again, err := sealed.resolvedContext(context.Background(), "repeat", policy, v2core.RollupMetadata{ID: 1})
	if err != nil || again != old {
		t.Fatal("same generation not reused")
	}
	sealed.number++
	newer, err := sealed.resolvedContext(context.Background(), "repeat", policy, v2core.RollupMetadata{ID: 1})
	if err != nil || newer == old {
		t.Fatal("cross-generation source reused")
	}
}

func TestPendingPrefixExactAndCanonical(t *testing.T) {
	policy := v2core.RollupPolicy{Name: "prefix", Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 100}, {Step: 10, Count: 100}}}}
	encoded, err := v2core.EncodeRollupPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	batches := []sealrollup.Batch{
		{Order: 1, Now: 1000, Points: []sealrollup.Sample{{Time: 990, Bits: math.Float64bits(2), Sequence: 1}, {Time: 991, Bits: math.Float64bits(3), Sequence: 2}}},
		{Order: 2, Now: 990, Points: []sealrollup.Sample{{Time: 990, Bits: 1 << 63, Sequence: 3}, {Time: 992, Bits: 0x7ff8000000000042, Sequence: 4}}},
	}
	r, _ := New(1, 1<<24, 1<<20)
	makeMetric := func(b []sealrollup.Batch, cache *sealedCache) *metricState {
		return &metricState{name: "prefix", policy: policy, meta: v2core.RollupMetadata{ID: 1, Policy: encoded}, pending: b, reader: r, resource: &generation{cache: cache}}
	}
	cache := &sealedCache{limit: 1 << 20}
	prefix := makeMetric(batches[:1], cache)
	prefix.resolveContext(context.Background())
	if prefix.err != nil {
		t.Fatal(prefix.err)
	}
	incremental := makeMetric(batches, cache)
	if incremental.resource.cachedReduction(incremental) == nil {
		t.Fatal("exact prefix missed")
	}
	incremental.resolveContext(context.Background())
	if incremental.err != nil {
		t.Fatal(incremental.err)
	}
	whole := makeMetric(batches, &sealedCache{limit: 1 << 20})
	whole.resolveContext(context.Background())
	if whole.err != nil {
		t.Fatal(whole.err)
	}
	if len(whole.frozen.records) != len(incremental.frozen.records) {
		t.Fatal("incremental shape")
	}
	for i, w := range whole.frozen.records {
		if w.Delta != incremental.frozen.records[i].Delta {
			t.Fatal("incremental canonical mismatch")
		}
	}
	for _, change := range []func(*sealrollup.Batch){
		func(b *sealrollup.Batch) { b.Now++ }, func(b *sealrollup.Batch) { b.Order++ },
		func(b *sealrollup.Batch) { b.Points = slices.Clone(b.Points); b.Points[0].Bits ^= 1 },
		func(b *sealrollup.Batch) { b.Points = slices.Clone(b.Points); b.Points[0].Sequence++ },
	} {
		altered := slices.Clone(batches)
		change(&altered[0])
		m := makeMetric(altered, cache)
		if m.resource.cachedReduction(m) != nil {
			t.Fatal("different pending boundary reused")
		}
	}
	if cache.reductionBytes > cache.limit {
		t.Fatal("pending cache over budget")
	}
}

func TestPendingOrderingStable(t *testing.T) {
	input := []sealrollup.Sample{{Time: 2, Bits: 1, Sequence: 2}, {Time: 1, Bits: 2, Sequence: 1}, {Time: 1, Bits: 3, Sequence: 1}}
	original := slices.Clone(input)
	got := orderSamples(input)
	if got[0].Bits != 2 || got[1].Bits != 3 || got[2].Bits != 1 {
		t.Fatal("stable tie ordering changed")
	}
	if !slices.Equal(input, original) {
		t.Fatal("ordering mutated the published pending arena")
	}
}

func TestResolvedCacheSharesOnlyUnchangedPartSets(t *testing.T) {
	r, _ := New(1, 1<<30, 1<<20)
	e, err := v2core.Open(t.TempDir(), testConfig(), v2core.Hooks{PublishBatchesWithInventory: r.PublishWithInventory})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	defer r.Close()
	var pins []*generation
	defer func() {
		for _, g := range pins {
			g.release()
		}
	}()
	seal := func(name string, id, order uint64) *generation {
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}}}})
		if err != nil {
			t.Fatal(err)
		}
		if err = e.AppendBatches(0, []v2core.Batch{{ID: id, Order: order, Clock: 1700000002, Policy: policy, Points: []v2core.Row{{ID: id, Time: 1700000000, Bits: order, Seq: order}}}}, order, true); err != nil {
			t.Fatal(err)
		}
		g := r.owners[0].Load()
		if !g.retain() {
			t.Fatal("pin")
		}
		pins = append(pins, g)
		g.load()
		if g.err != nil {
			t.Fatal(g.err)
		}
		return g
	}
	first := seal("stable.a", 1, 1)
	a := first.lookup("stable.a")
	a.resolveContext(context.Background())
	if a.err != nil {
		t.Fatal(a.err)
	}
	second := seal("stable.b", 2, 2)
	b := second.lookup("stable.a")
	b.resolveContext(context.Background())
	if b.err != nil {
		t.Fatal(b.err)
	}
	if a.frozen != b.frozen {
		t.Fatal("unchanged name rescanned after unrelated seal")
	}
	third := seal("stable.a", 1, 3)
	c := third.lookup("stable.a")
	c.resolveContext(context.Background())
	if c.err != nil {
		t.Fatal(c.err)
	}
	if c.frozen == b.frozen {
		t.Fatal("changed name reused stale archives")
	}
	if a.frozen.records[0].Order != 1 || c.frozen.records[0].Order != 3 {
		t.Fatal("pinned or current records changed")
	}
}
