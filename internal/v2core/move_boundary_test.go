package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

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
		d, err := WriteDenseRollupPart(path, entries, v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: "donor"})
		if err != nil {
			t.Fatal(err)
		}
		if old {
			// Model a donor TTL merge: materialize the complete windows before
			// deleting raw input. This fixture works when seals store only raw.
			var p RollupPolicy
			if err = json.Unmarshal(policy, &p); err != nil {
				t.Fatal(err)
			}
			var versions []sealrollup.Version
			for _, row := range rows {
				versions = append(versions, sealrollup.Version{Archive: 0, Time: row.Time, Bits: row.Bits, Order: sealrollup.Tick(100, 0)})
			}
			_, mats := sealrollup.WriteDown(p.Config, versions, func(int) bool { return true })
			var settled []KV
			var template v2part.Key
			for _, entry := range entries {
				key, e := v2part.Decode(entry.Key)
				if e != nil {
					t.Fatal(e)
				}
				template = key
				if key.Archive == v2part.MetadataArchive {
					settled = append(settled, entry)
				}
			}
			for _, v := range mats {
				r := v.Record(p.Config)
				key := template
				key.Archive, key.BlockStart = uint32(r.Archive), r.Slot
				encoded, e := v2part.Encode(key)
				if e != nil {
					t.Fatal(e)
				}
				settled = append(settled, KV{Key: encoded, Record: &r})
			}
			slices.SortFunc(settled, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
			path = filepath.Join(t.TempDir(), "settled.sst")
			writer, e := v2part.NewDenseWriter(path, v2part.Spec{Scope: d.Scope, Source: d.Source, Policies: d.Coverage.Policies, MinTime: d.Coverage.MinTime, MaxTime: d.Coverage.MaxTime, MinClock: d.Coverage.MinClock, MinExpiry: d.Coverage.MinExpiry, MaxExpiry: d.Coverage.MaxExpiry})
			if e != nil {
				t.Fatal(e)
			}
			defer writer.Abort()
			for _, entry := range settled {
				if entry.Record != nil {
					e = writer.SetRecordEncoded(entry.Key, *entry.Record)
				} else {
					e = writer.SetEncoded(entry.Key, entry.Value)
				}
				if e != nil {
					t.Fatal(e)
				}
			}
			d, err = writer.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		return MovePart{Path: path, Descriptor: d}
	}
	prepared, err := PrepareMove(ctx, t.TempDir(), []MovePart{part(true), part(false)})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	root := t.TempDir()
	// Background merges apply raw retention to the expired 1560 and 1810 rows
	// on their own 20 ms schedule. Hold them so both snapshots show only what
	// the restart itself keeps.
	cfg := adoptConfig()
	cfg.DeferMerges = true
	e, err := Open(root, cfg, Hooks{})
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
	if authority.Version != 3 {
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
	e, err = Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	_, after := adoptedRecords(t, e)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("restart changed mature parts or boundary repairs")
	}
}
