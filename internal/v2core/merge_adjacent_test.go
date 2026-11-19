package v2core

import (
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Merges take a window of sequence-adjacent parts that meets the span ratio. A
// young merged part does not re-absorb the next few seals, although its bytes
// shrank to one seal's: it waits until those seals fold among themselves.
func TestMergeSelectsSequenceAdjacentWindow(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	drain := func(from, to uint64, merges, live int) {
		t.Helper()
		for n := from; n <= to; n++ {
			mergeAppend(t, e, n)
		}
		s := o.db.NewSnapshot()
		want := mergeRead(t, s)
		s.Close()
		if got := mergeDrain(t, o); got != merges || mergeCount(o) != live {
			t.Fatalf("after seals %d..%d: %d merges and %d live parts, want %d and %d", from, to, got, mergeCount(o), merges, live)
		}
		s = o.db.NewSnapshot()
		defer s.Close()
		if got := mergeRead(t, s); !reflect.DeepEqual(got, want) {
			t.Fatalf("records changed by adjacent merges:\n got %v\nwant %v", got, want)
		}
	}
	for n := uint64(1); n <= smallMergeRatio; n++ {
		mergeAppend(t, e, n)
	}
	s := o.db.NewSnapshot()
	inputs, err := o.selectMerge(s)
	s.Close()
	if err != nil || len(inputs) != smallMergeRatio {
		t.Fatalf("inputs=%d %v", len(inputs), err)
	}
	for i, d := range inputs {
		if i > 0 && d.Source.MinOrder <= inputs[i-1].Source.MaxOrder {
			t.Fatalf("inputs out of sequence order at %d", i)
		}
	}
	drain(1, 0, 1, 1)
	drain(smallMergeRatio+1, 2*smallMergeRatio-1, 0, smallMergeRatio)
	drain(2*smallMergeRatio, 2*smallMergeRatio, 1, 2)
	var ranges [][2]uint64
	o.gc.mu.Lock()
	inventory := o.gc.inventory
	o.gc.mu.Unlock()
	for _, value := range inventory.bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			t.Fatal(err)
		}
		ranges = append(ranges, [2]uint64{d.Source.MinOrder, d.Source.MaxOrder})
	}
	slices.SortFunc(ranges, func(a, b [2]uint64) int { return int(a[0]) - int(b[0]) })
	if want := [][2]uint64{{1, smallMergeRatio}, {smallMergeRatio + 1, 2 * smallMergeRatio}}; !reflect.DeepEqual(ranges, want) {
		t.Fatalf("live order ranges %v, want %v", ranges, want)
	}
}
