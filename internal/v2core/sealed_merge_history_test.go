package v2core

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Compare exact persisted winners with an unmerged store after original parts
// are retired, unlinked and reopened. Late writes force cold older-window loads.
func TestSealedMergeHistoryDifferential(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for method := sealrollup.Average; method <= sealrollup.First; method++ {
		for _, schema := range []string{"1s:1h,1m:1d,1h:1y", "10s:6h,5m:7d,1h:1y,1d:10y"} {
			t.Run(fmt.Sprintf("%d/%s", method, schema), func(t *testing.T) {
				// Subtests own their engines; the parent's environment stays set
				// until all of them finish.
				t.Parallel()
				_, policy := rollupTestPolicy(t, "history.metric", schema, method, .1)
				cfg := coreTestConfig()
				cfg.Shards = 1
				cfg.Producers = 1
				cfg.Rollups = true
				cfg.MaxRows = 1024
				roots := []string{t.TempDir(), t.TempDir()}
				engines := make([]*coreEngine, 2)
				reopen := func(i int) {
					if engines[i] != nil {
						if err := engines[i].close(); err != nil {
							t.Fatal(err)
						}
					}
					e, err := openCoreWithHooks(roots[i], cfg, coreHooks{})
					if err != nil {
						t.Fatal(err)
					}
					engines[i] = e
				}
				reopen(0)
				reopen(1)
				defer func() {
					for _, e := range engines {
						_ = e.close()
					}
				}()
				compare := func(epoch int64) {
					snapshots := []*RollupSnapshot{}
					for _, e := range engines {
						s := e.owners[0].db.NewSnapshot()
						defer s.Close()
						index, err := NewRollupSnapshot(context.Background(), s, nil, nil, nil, 0)
						if err != nil {
							t.Fatal(err)
						}
						snapshots = append(snapshots, index)
					}
					want, err := snapshots[0].Records(context.Background(), "history.metric")
					if err != nil {
						t.Fatal(err)
					}
					got, err := snapshots[1].Records(context.Background(), "history.metric")
					if err != nil {
						t.Fatal(err)
					}
					less := func(a, b sealrollup.Record) int {
						if a.Archive != b.Archive {
							return a.Archive - b.Archive
						}
						if a.Slot < b.Slot {
							return -1
						}
						if a.Slot > b.Slot {
							return 1
						}
						return 0
					}
					slices.SortFunc(got, less)
					slices.SortFunc(want, less)
					// Idle materialization may happen independently in the two
					// owners. Correction selects physical placement; compare all
					// logical fields (timestamp, IEEE bits and source order).
					for i := range got {
						got[i].Correction = false
					}
					for i := range want {
						want[i].Correction = false
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("epoch %d merged winners differ\ngot=%+v\nwant=%+v", epoch, got, want)
					}
				}
				now := int64(1794700800)
				values := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1, math.Float64bits(7)}
				for epoch := int64(0); epoch < 40; epoch++ {
					now += 600
					order := uint64(epoch + 1)
					var rows []row
					for j := int64(0); j < 60; j++ {
						rows = append(rows, row{ID: 1, Time: now - 600 + j*10, Bits: values[(j+epoch)%int64(len(values))], Seq: uint64(j + 1)})
					}
					for _, age := range []int64{120, 1800, 3500, 7200, 18000, 100000} {
						stamp := now - age
						rows = append(rows, row{ID: 1, Time: stamp, Bits: math.Float64bits(float64(epoch)), Seq: 100}, row{ID: 1, Time: stamp, Bits: values[epoch%int64(len(values))], Seq: 101})
					}
					for _, e := range engines {
						if err := e.submitBatches(0, []coreBatch{{ID: 1, Policy: policy, Order: order, Clock: now, Points: rows}}, order, true); err != nil {
							t.Fatal(err)
						}
					}
					compare(epoch)
					if epoch == 15 || epoch == 31 {
						o := engines[1].owners[0]
						priorGC := o.gc
						removedBefore := priorGC.removedParts.Load()
						if n := mergeDrain(t, o); n == 0 {
							t.Fatal("no history parts merged")
						}
						compare(epoch)
						// Closing releases publication pins. Restart then runs ordinary fenced GC
						// with the grace advanced before either store reloads a later old window.
						reopen(0)
						reopen(1)
						o = engines[1].owners[0]
						gcAdvance(o.gc)
						if _, err := o.gc.sweep(); err != nil {
							t.Fatal(err)
						}
						// Released inputs may be reclaimed before close or after reopen.
						if priorGC.removedParts.Load()-removedBefore+o.gc.removedParts.Load() == 0 {
							t.Fatal("source parts not unlinked")
						}
						compare(epoch)
					}
				}
			})
		}
	}
}

// Different inputs contain disjoint and overlapping names. Metadata lookups
// must include every contributing input and ignore cursors already past a name.
func TestSealedMergeSparseMetadata(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	for order := uint64(1); order <= 32; order++ {
		name := fmt.Sprintf("sparse.%02d", order%7)
		_, policy := rollupTestPolicy(t, name, "1s:120,10s:1h", sealrollup.Last, .5)
		if err := e.submitBatches(0, []coreBatch{{ID: order%7 + 1, Policy: policy, Order: order, Clock: 1030, Points: []row{{ID: order%7 + 1, Time: 1000 + int64(order), Bits: order << 32, Seq: order}}}}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	read := func() map[string][]sealrollup.Record {
		s := e.owners[0].db.NewSnapshot()
		defer s.Close()
		index, err := NewRollupSnapshot(context.Background(), s, nil, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string][]sealrollup.Record{}
		index.Metadata(func(name string, _ RollupPolicy, _ RollupMetadata) {
			rs, err := index.Records(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			slices.SortFunc(rs, func(a, b sealrollup.Record) int {
				if a.Archive != b.Archive {
					return a.Archive - b.Archive
				}
				return int(a.Slot - b.Slot)
			})
			out[name] = rs
		})
		return out
	}
	want := read()
	if n := mergeDrain(t, e.owners[0]); n == 0 {
		t.Fatal("no sparse merge")
	}
	if got := read(); !reflect.DeepEqual(got, want) {
		t.Fatalf("sparse merge changed names or exact records: got=%v want=%v", got, want)
	}
}
