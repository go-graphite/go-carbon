package v2core

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestMoveBoundaryPreservesMatureAndDonorOnlyWindows(t *testing.T) {
	for _, method := range []sealrollup.Method{sealrollup.Sum, sealrollup.Average, sealrollup.First, sealrollup.Last, sealrollup.Min, sealrollup.Max} {
		t.Run(string(rune('0'+method)), func(t *testing.T) {
			policy := RollupPolicy{Name: "move.metric", Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 100}, {Step: 10, Count: 100}, {Step: 100, Count: 100}}}}
			record := func(a int, ts int64, bits uint64) sealrollup.Record {
				return sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Slot: ts / policy.Config.Archives[a].Step % policy.Config.Archives[a].Count, Order: 7, Sample: sealrollup.Sample{Time: ts, Bits: bits}}}
			}
			mature := record(1, 500, 0x7ff8000000000042)
			donorOnly := record(1, 990, math.Float64bits(777))
			older := record(2, 100, 0x8000000000000000)
			winning := []sealrollup.Record{mature, donorOnly, older, record(1, 1000, math.Float64bits(-1))}
			// This donor-only raw prefix must not recompute its own authoritative
			// coarse slot. Only receiving raw in the next window needs repair.
			for ts := int64(990); ts < 1000; ts++ {
				winning = append(winning, record(0, ts, math.Float64bits(1)))
			}
			var local []sealrollup.Record
			for ts := int64(1000); ts < 1010; ts++ {
				r := record(0, ts, math.Float64bits(float64(ts-999)))
				winning = append(winning, r)
				if ts >= 1005 {
					local = append(local, r)
				}
			}
			got, err := PlanMoveRollupBoundary(policy, winning, local, 1010, 9)
			if err != nil {
				t.Fatal(err)
			}
			bySlot := map[[2]int64]sealrollup.Record{}
			for _, r := range got {
				bySlot[[2]int64{int64(r.Archive), r.Slot}] = r
			}
			for _, r := range []sealrollup.Record{mature, donorOnly, older} {
				if bySlot[[2]int64{int64(r.Archive), r.Slot}] != r {
					t.Fatalf("untouched history changed: %+v", r)
				}
			}
			expected := map[sealrollup.Method]float64{sealrollup.Sum: 55, sealrollup.Average: 5.5, sealrollup.First: 1, sealrollup.Last: 10, sealrollup.Min: 1, sealrollup.Max: 10}[method]
			if r := bySlot[[2]int64{1, 0}]; r.Sample.Bits != math.Float64bits(expected) || r.Order != 9 {
				t.Fatalf("boundary=%+v want %g", r, expected)
			}
			noLocal, err := PlanMoveRollupBoundary(policy, winning, nil, 1010, 9)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range noLocal {
				if r.Archive > 0 && r.Order != 7 {
					t.Fatalf("no receiving raw rebuilt donor window: %+v", r)
				}
			}
		})
	}
}

func TestMoveAdoptMatureHistoryKeepsPartsAndSparseRepairs(t *testing.T) {
	ctx := context.Background()
	policy := adoptPolicy(t, sealrollup.Sum)
	part := func(old bool) MovePart {
		t.Helper()
		reducer := newCoreRollups()
		ts, clock := int64(1990), int64(2000)
		windows := int64(1)
		if old {
			ts, clock, windows = 1800, 1820, 2
		}
		var rows []Row
		for window := int64(0); window < windows; window++ {
			for i := int64(0); i < 5; i++ {
				rows = append(rows, Row{ID: 99, Time: ts + window*10 + i, Bits: math.Float64bits(float64(i + 1)), Seq: uint64(window*5 + i + 1)})
			}
		}
		plan, err := reducer.plan(coreSealInput{Batches: []Batch{{ID: 99, Order: 100, Clock: clock, Policy: policy, Points: rows}}})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "part.sst")
		entries := plan.Files[0]
		if old {
			// Mature coarse parts need no retained raw, even physically.
			kept := entries[:0]
			for _, entry := range entries {
				k, err := v2part.Decode(entry.Key)
				if err != nil {
					t.Fatal(err)
				}
				if k.Archive != 0 {
					kept = append(kept, entry)
				}
			}
			entries = kept
		}
		d, err := WriteDenseRollupPart(path, entries, v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: "donor"})
		if err != nil {
			t.Fatal(err)
		}
		return MovePart{Path: path, Descriptor: d}
	}
	prepared, err := PrepareMove(ctx, t.TempDir(), []MovePart{part(true), part(false)})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	root := t.TempDir()
	e, err := Open(root, adoptConfig(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e != nil {
			_ = e.Close()
		}
	}()
	var oldBatches []Batch
	for i, stamp := range []int64{1560, 1810} {
		var oldRows []Row
		for j := int64(0); j < 5; j++ {
			oldRows = append(oldRows, Row{ID: 7, Time: stamp + j, Bits: math.Float64bits(99), Seq: uint64(j + 1)})
		}
		oldBatches = append(oldBatches, Batch{ID: 7, Order: uint64(i + 1), Clock: stamp + 10, Policy: policy, Points: oldRows})
	}
	if err = e.AppendBatches(0, oldBatches, 10, true); err != nil {
		t.Fatal(err)
	}
	var rows []Row
	for ts := int64(1995); ts < 2000; ts++ {
		rows = append(rows, Row{ID: 7, Time: ts, Bits: math.Float64bits(float64(ts - 1989)), Seq: uint64(ts - 1994)})
	}
	if err = e.AppendBatches(0, []Batch{{ID: 7, Order: 3, Clock: 2000, Policy: policy, Points: rows}}, 15, true); err != nil {
		t.Fatal(err)
	}
	if err = e.AdoptMove(ctx, prepared, map[string]uint64{"move.metric": 7}, 2000); err != nil {
		t.Fatal(err)
	}
	b, closer, err := e.DBs()[0].Get(moveAuthorityKey("move.metric"))
	if err != nil {
		t.Fatal(err)
	}
	var authority moveAuthority
	err = json.Unmarshal(b, &authority)
	_ = closer.Close()
	if err != nil {
		t.Fatal(err)
	}
	if authority.Version != 2 || len(authority.Coarse) != 1 || authority.Coarse[0].Sample.Time != 1990 {
		t.Fatalf("history duplicated into repair catalog: %+v", authority)
	}
	_, before := adoptedRecords(t, e)
	values := map[int64]uint64{}
	for _, r := range before {
		if r.Archive == 1 {
			values[r.Sample.Time] = r.Sample.Bits
		}
	}
	if !reflect.DeepEqual(values, map[int64]uint64{1800: math.Float64bits(15), 1810: math.Float64bits(495), 1990: math.Float64bits(55)}) {
		t.Fatalf("coarse=%v", values)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e = nil
	e, err = Open(root, adoptConfig(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	_, after := adoptedRecords(t, e)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("restart changed mature parts or boundary repairs")
	}
}

func TestMoveBoundaryRetentionFloorRequiresOnlyStraddlingNeighbors(t *testing.T) {
	policy := RollupPolicy{Name: "move.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 100}, {Step: 10, Count: 100}}}}
	record := func(archive int, ts int64, value float64) sealrollup.Record {
		return sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Slot: ts / policy.Config.Archives[archive].Step % policy.Config.Archives[archive].Count, Order: 1, Sample: moveSample(ts, value)}}
	}
	winning := []sealrollup.Record{record(1, 500, 42), record(1, 910, 15)}
	for ts := int64(915); ts < 920; ts++ {
		winning = append(winning, record(0, ts, float64(ts-909)))
	}
	local := []sealrollup.Record{record(0, 915, 6)}
	if _, err := PlanMoveRollupBoundary(policy, winning, local, 1015, 2); err == nil {
		t.Fatal("silently rebuilt incomplete window across raw retention floor")
	}
	for ts := int64(910); ts < 915; ts++ {
		winning = append(winning, record(0, ts, float64(ts-909)))
	}
	got, err := PlanMoveRollupBoundary(policy, winning, local, 1015, 2)
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, r := range got {
		if r.Archive == 0 && r.Sample.Time < 915 {
			t.Fatal("expired boundary neighbor retained as raw")
		}
		if r.Archive == 1 && r.Sample.Time == 910 {
			seen = true
			if r.Sample.Bits != math.Float64bits(55) {
				t.Fatalf("partial boundary aggregate: %+v", r)
			}
		}
	}
	if !seen {
		t.Fatal("missing repaired boundary")
	}
}
