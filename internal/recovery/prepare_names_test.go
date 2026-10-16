package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestFilteredNamesEmptyAndBatchBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 512, 513, 1025} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			b := NewBuilder(func(string) bool { return false })
			var source, encoded bytes.Buffer
			for i := range count {
				p := points.OnePoint(fmt.Sprintf("new.metric%d", i), 1, 1)
				raw := p.AppendBinary(nil)
				source.Write(raw)
				if err := b.Add(0, p, len(raw)); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Write(&encoded); err != nil {
				t.Fatal(err)
			}
			in, err := Open(encoded.Bytes(), source.Bytes(), nil)
			if err != nil {
				t.Fatal(err)
			}
			var want, got []string
			if err = in.NewNames(func(name string) error { want = append(want, name); return nil }); err != nil {
				t.Fatal(err)
			}
			err = in.FilterNewNames(func([]byte) bool { return true }, func(name string) error { got = append(got, name); return nil })
			if err != nil || !reflect.DeepEqual(want, got) || len(got) != count {
				t.Fatal("filtered enumeration differs", err, len(want), len(got))
			}
		})
	}
}

func filteredNamesFixture(t *testing.T) (*Index, []byte, []byte) {
	t.Helper()
	b := NewBuilder(func(name string) bool { return strings.HasPrefix(name, "known.") })
	var source []byte
	for i := range 20000 {
		kind := "reject"
		if i%5 == 0 {
			kind = "keep"
		}
		name := fmt.Sprintf("%s.metric%d.%s", kind, i, strings.Repeat("x", i%1024)) + "z"
		if i%17 == 0 {
			name = "known." + name
		}
		p := points.OnePoint(name, 1, 1)
		raw := p.AppendBinary(nil)
		source = append(source, raw...)
		if err := b.Add(0, p, len(raw)); err != nil {
			t.Fatal(err)
		}
	}
	var encoded bytes.Buffer
	if err := b.Write(&encoded); err != nil {
		t.Fatal(err)
	}
	in, err := Open(encoded.Bytes(), source, nil)
	if err != nil {
		t.Fatal(err)
	}
	return in, source, encoded.Bytes()
}

func TestFilteredNamesMatchSerialOrderAndOwnership(t *testing.T) {
	in, source, encoded := filteredNamesFixture(t)
	var all []string
	if err := in.NewNames(func(name string) error { all = append(all, name); return nil }); err != nil {
		t.Fatal(err)
	}
	if in.NewNameCount() != uint64(len(all)) {
		t.Fatal("candidate count differs")
	}
	var retained, wantRetained []string
	for _, mode := range []string{"mixed", "all", "none"} {
		t.Run(mode, func(t *testing.T) {
			keep := func(name string) bool { return mode == "all" || (mode == "mixed" && strings.HasPrefix(name, "keep.")) }
			var want, got []string
			for _, name := range all {
				if keep(name) {
					want = append(want, name)
				}
			}
			wantRetained = append(wantRetained, want...)
			var active, calls atomic.Int32
			err := in.FilterNewNames(func(name []byte) bool {
				active.Add(1)
				defer active.Add(-1)
				calls.Add(1)
				return keep(string(name))
			}, func(name string) error {
				if active.Load() != 0 {
					t.Error("visitor overlaps predicate workers")
				}
				got = append(got, name)
				return nil
			})
			if err != nil || !reflect.DeepEqual(want, got) || int(calls.Load()) != len(all) || active.Load() != 0 {
				t.Fatal("filtered enumeration differs", err, len(want), len(got), calls.Load())
			}
			retained = append(retained, got...)
		})
	}
	// Replacing mapped bytes must not change any admitted name kept by callers.
	clear(source)
	clear(encoded)
	if !reflect.DeepEqual(retained, wantRetained) {
		t.Fatal("retained names alias mapped bytes")
	}
}

func TestFilteredNamesErrorKeepsSerialPrefixAndJoinsWorkers(t *testing.T) {
	in, _, _ := filteredNamesFixture(t)
	sentinel := errors.New("visitor stopped")
	var active atomic.Int32
	var got, want []string
	err := in.FilterNewNames(func([]byte) bool {
		active.Add(1)
		defer active.Add(-1)
		return true
	}, func(name string) error {
		if active.Load() != 0 {
			t.Fatal("worker still running during visitor")
		}
		got = append(got, name)
		if len(got) == 7 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) || active.Load() != 0 {
		t.Fatal("visitor error or worker lifetime differs", err)
	}
	_ = in.NewNames(func(name string) error {
		want = append(want, name)
		if len(want) == 7 {
			return sentinel
		}
		return nil
	})
	if !reflect.DeepEqual(want, got) {
		t.Fatal("visitor saw a different prefix")
	}
}

func TestParallelSlotTotalsRejectCorruptionAndOverflow(t *testing.T) {
	const slots = 1 << 22
	in := &Index{data: make([]byte, headerSize+slots*slotSize), slots: slots, metrics: 4, points: 10, recordCounts: [2]uint64{1, 0}}
	positions := []uint64{0, slots / 3, slots * 2 / 3, slots - 1}
	for i, slot := range positions {
		put64(in.data[headerSize+slot*slotSize:], 0, 1, 0, uint64(i+1))
	}
	if err := in.validateSlots(); err != nil {
		t.Fatal(err)
	}
	for _, slot := range []uint64{slots / 4, slots / 2, slots * 3 / 4} {
		put64(in.data[headerSize+slot*slotSize:], 0, 0, 0, 1)
		if err := in.validateSlots(); err == nil {
			t.Fatal("accepted nonempty points in an empty slot")
		}
		put64(in.data[headerSize+slot*slotSize:], 0, 0, 0, 0)
	}
	in.points--
	if err := in.validateSlots(); err == nil {
		t.Fatal("accepted incorrect cross-worker total")
	}
	in.points = math.MaxUint64
	in.metrics = 2
	for _, slot := range positions {
		put64(in.data[headerSize+slot*slotSize:], 0, 0, 0, 0)
	}
	put64(in.data[headerSize:], 0, 1, 0, math.MaxUint64-1)
	put64(in.data[headerSize+(slots-1)*slotSize:], 0, 1, 0, 2)
	if err := in.validateSlots(); err == nil {
		t.Fatal("accepted overflowing cross-worker total")
	}
}
