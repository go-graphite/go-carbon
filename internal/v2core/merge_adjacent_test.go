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
	for n := uint64(1); n <= smallMergeRatio; n++ {
		mergeAppend(t, e, n)
	}
	s := o.db.NewSnapshot()
	inputs, err := o.selectMerge(s)
	s.Close()
	if err != nil || len(inputs) < 2 {
		t.Fatalf("inputs=%d %v", len(inputs), err)
	}
	var total, largest uint64
	selected := map[v2part.Digest]bool{}
	for i, d := range inputs {
		total += d.Bytes
		largest = max(largest, d.Bytes)
		selected[d.SHA256] = true
		if i > 0 && d.Source.MinOrder <= inputs[i-1].Source.MaxOrder {
			t.Fatalf("inputs out of sequence order at %d", i)
		}
	}
	if len(inputs) != smallMergeRatio || !mergeWindowOK(len(inputs), total, largest) {
		t.Fatalf("window of %d parts, total %d, largest %d", len(inputs), total, largest)
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

	for n := uint64(smallMergeRatio + 1); n <= 2*smallMergeRatio; n++ {
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
	if live := e.PartGCStats()[0].LiveParts; live != 2 || merges != 2 {
		t.Fatalf("30 parts folded into %d live parts with %d merges, want 2 and 2", live, merges)
	}
	s = o.db.NewSnapshot()
	defer s.Close()
	if got := mergeRead(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("records changed by adjacent merges:\n got %v\nwant %v", got, want)
	}
}

// Small outputs merge up to fifteen parts once the output is at least 1.7
// times the largest input (VictoriaMetrics minMergeMultiplier).
func TestMergeWindowSmallOutputRule(t *testing.T) {
	cases := []struct {
		n              int
		total, largest uint64
		want           bool
	}{
		{smallMergeRatio, smallMergeRatio * 10, 10, true},
		{smallMergeRatio - 1, (smallMergeRatio - 1) * 10, 10, false},
		{smallMergeRatio, 170, 100, true},
		{smallMergeRatio, 169, 100, false},
		{smallMergeRatio, 14 + 100, 100, false},
	}
	for _, c := range cases {
		if got := mergeWindowOK(c.n, c.total, c.largest); got != c.want {
			t.Errorf("mergeWindowOK(%d,%d,%d)=%v want %v", c.n, c.total, c.largest, got, c.want)
		}
	}
}
