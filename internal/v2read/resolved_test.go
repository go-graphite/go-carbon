package v2read

import (
	"context"
	"math"
	"slices"
	"testing"

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
