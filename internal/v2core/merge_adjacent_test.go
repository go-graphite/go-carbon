package v2core

import (
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Merges take a window of sequence-adjacent parts that meets the size ratio,
// and folding small parts to quiescence keeps every record.
func TestMergeSelectsSequenceAdjacentWindow(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= 9; n++ {
		mergeAppend(t, e, n)
	}
	s := o.db.NewSnapshot()
	inputs, err := o.selectMerge(s)
	s.Close()
	if err != nil || len(inputs) < 2 {
		t.Fatalf("inputs=%d %v", len(inputs), err)
	}
	var covered, widest uint64
	for _, w := range orderWidths(inputs) {
		covered += w
		widest = max(widest, w)
	}
	selected := map[v2part.Digest]bool{}
	for i, d := range inputs {
		selected[d.SHA256] = true
		if i > 0 && d.Source.MinOrder <= inputs[i-1].Source.MaxOrder {
			t.Fatalf("inputs out of sequence order at %d", i)
		}
	}
	if len(inputs) != smallMergeRatio || 2*covered < smallMergeRatio*widest {
		t.Fatalf("window of %d parts, covered %d, widest %d", len(inputs), covered, widest)
	}
	lo, hi := inputs[0].Source.MinOrder, inputs[len(inputs)-1].Source.MaxOrder
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			t.Fatal(err)
		}
		if !selected[d.SHA256] && d.Source.MaxOrder >= lo && d.Source.MinOrder <= hi {
			t.Fatalf("live part [%d,%d] interleaves the merge window [%d,%d]", d.Source.MinOrder, d.Source.MaxOrder, lo, hi)
		}
	}

	for n := uint64(10); n <= 16; n++ {
		mergeAppend(t, e, n)
	}
	s = o.db.NewSnapshot()
	want := mergeRead(t, s)
	s.Close()
	merges := 0
	for {
		changed, err := o.mergeSealedParts(nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if !changed {
			break
		}
		merges++
	}
	// Widths 1..16: [1-4] [5-8], then [1-4] [5-8] 9 10 covers 2x its widest
	// part, then [13-16]; 11 and 12 wait for two more seals.
	if live := e.PartGCStats()[0].LiveParts; live != 4 || merges != 4 {
		t.Fatalf("16 parts folded into %d live parts with %d merges, want 4 and 4", live, merges)
	}
	s = o.db.NewSnapshot()
	defer s.Close()
	if got := mergeRead(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("records changed by adjacent merges:\n got %v\nwant %v", got, want)
	}
}

// A merged part covers the orders of all its inputs, so it is wider than new
// seal parts even when it is no larger on disk. Three new seals therefore do
// not re-merge it; the fourth makes a window of new seals only.
func TestMergeLevelsByOrderWidth(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	for n := uint64(1); n <= smallMergeRatio; n++ {
		mergeAppend(t, e, n)
	}
	if changed, err := o.mergeSealedParts(nil, false); err != nil || !changed {
		t.Fatalf("first merge: changed=%v %v", changed, err)
	}
	for n := uint64(smallMergeRatio + 1); n < 2*smallMergeRatio; n++ {
		mergeAppend(t, e, n)
	}
	s := o.db.NewSnapshot()
	inputs, err := o.selectMerge(s)
	s.Close()
	if err != nil || len(inputs) != 0 {
		t.Fatalf("merged part re-merged with %d narrow parts: %v", len(inputs), err)
	}
	mergeAppend(t, e, 2*smallMergeRatio)
	s = o.db.NewSnapshot()
	inputs, err = o.selectMerge(s)
	s.Close()
	if err != nil || len(inputs) != smallMergeRatio || inputs[0].Source.MinOrder <= smallMergeRatio {
		t.Fatalf("want the %d new seal parts, got %d parts from order %v: %v", smallMergeRatio, len(inputs), inputs, err)
	}
}

func TestOrderWidthsClipReachBack(t *testing.T) {
	part := func(lo, hi uint64) v2part.Descriptor {
		return v2part.Descriptor{Source: v2part.Provenance{MinOrder: lo, MaxOrder: hi}}
	}
	// The third part re-emits a closed window with order 3; it admitted 21..30.
	got := orderWidths([]v2part.Descriptor{part(1, 10), part(11, 20), part(3, 30), part(31, 31)})
	if want := []uint64{10, 10, 10, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("widths %v, want %v", got, want)
	}
}
