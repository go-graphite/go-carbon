package v2core

import (
	"cmp"
	"context"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// A merge drops a record only where age retirement would: below the owner's
// retire clock. This replays the classic pending/sealed input, whose oracle
// renders a day below the newest admission clock while Hooks.Now holds
// retirement there, and forces a full fan-in merge through the production
// path. Every record the unmerged parts return must survive the merge,
// including the slot whose later write was too old for retention at its clock.
func TestMergeKeepsRecordsTheReaderReturns(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixtureIdle(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	_, policy := rollupTestPolicy(t, "test.metric", "1s:1d", sealrollup.Average, 0)
	_, pad := rollupTestPolicy(t, "test.pad", "1s:1d", sealrollup.Average, 0)
	now := int64(1794700800)
	step, advance := int64(1), int64(86400)
	start := now - 120*step
	var dense []row
	for i := int64(0); i < 90; i++ {
		dense = append(dense, row{ID: 7, Time: start + i*step, Bits: math.Float64bits(float64(i%17) - 8), Seq: uint64(i + 1)})
	}
	special := []row{{ID: 7, Time: start + step, Bits: math.Float64bits(9), Seq: 101}}
	for i, b := range []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1} {
		special = append(special, row{ID: 7, Time: start + int64(i+2)*step, Bits: b, Seq: uint64(102 + i)})
	}
	groups := [][]row{dense, special, {{ID: 7, Time: start + advance, Bits: math.Float64bits(17), Seq: 201}}, {{ID: 7, Time: start + step, Bits: math.Float64bits(127), Seq: 202}}, {{ID: 7, Time: now - 2*step, Bits: math.Float64bits(11), Seq: 203}}}
	clocks := []int64{now, now, now + advance, now + advance, now - 10*step}
	order := uint64(0)
	submit := func(b coreBatch) {
		order++
		b.Order = order
		if err := e.submitBatches(0, []coreBatch{b}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	for i, rows := range groups {
		submit(coreBatch{ID: 7, Policy: policy, Clock: clocks[i], Points: rows})
	}
	// Fill the size tier to the merge fan-in with another metric's parts.
	for mergeCount(o) < sealedMergeFanIn {
		submit(coreBatch{ID: 8, Policy: pad, Clock: now, Points: []row{{ID: 8, Time: start + int64(order), Bits: math.Float64bits(float64(order)), Seq: order}}})
	}
	read := func() map[string][]sealrollup.Record {
		s := o.db.NewSnapshot()
		defer s.Close()
		index, err := NewRollupSnapshot(context.Background(), s, nil, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string][]sealrollup.Record{}
		for _, name := range []string{"test.metric", "test.pad"} {
			if out[name], err = index.Records(context.Background(), name); err != nil {
				t.Fatal(err)
			}
			slices.SortFunc(out[name], func(a, b sealrollup.Record) int {
				return cmp.Or(cmp.Compare(a.Archive, b.Archive), cmp.Compare(a.Slot, b.Slot))
			})
		}
		return out
	}
	want := read()
	parts := mergeCount(o)
	if mergeDrain(t, o) == 0 || mergeCount(o) >= parts {
		t.Fatalf("no merge: %d parts", mergeCount(o))
	}
	// This fixture fits one run per name and certifies summary output. Its
	// physical order becomes the run maximum; every sample bit and other
	// record field must still survive exactly.
	for _, records := range want {
		var maximum uint64
		for _, r := range records {
			maximum = max(maximum, r.Order)
		}
		for i := range records {
			records[i].Order, records[i].Sample.Sequence = maximum, maximum
		}
	}
	if got := read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("merge changed records\ngot=%+v\nwant=%+v", got, want)
	}
}
