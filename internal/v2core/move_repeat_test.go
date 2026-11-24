package v2core

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Incremental snapshots may add another name while this name has only donor
// history. Its copied Mat must not become a mixed-window absence marker.
func TestMoveRepeatedRetiredHistory(t *testing.T) {
	ctx := context.Background()
	cfg := adoptConfig()
	cfg.DeferMerges = true
	donor, err := Open(t.TempDir(), cfg, Hooks{Now: func() time.Time { return time.Unix(1200, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	policy := adoptPolicy(t, sealrollup.Sum)
	pad, err := EncodeRollupPolicy(RollupPolicy{Name: "move.pad", Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 120}, {Step: 10, Count: 24}}}})
	if err != nil {
		t.Fatal(err)
	}
	var rows []Row
	for i := int64(0); i < 5; i++ {
		rows = append(rows, Row{ID: 7, Time: 1000 + i, Bits: math.Float64bits(float64(i + 1)), Seq: uint64(i + 1)})
	}
	if err = donor.AppendBatches(0, []Batch{{ID: 7, Order: 1, Clock: 1010, Policy: policy, Points: rows}}, 1, true); err != nil {
		t.Fatal(err)
	}
	if err = donor.AppendBatches(0, []Batch{{ID: 8, Order: 2, Clock: 1200, Policy: pad, Points: []Row{{ID: 8, Time: 1200, Bits: math.Float64bits(1), Seq: 1}}}}, 2, true); err != nil {
		t.Fatal(err)
	}
	if _, err = donor.ForceMerge(); err != nil {
		t.Fatal(err)
	}
	receiver, err := Open(t.TempDir(), cfg, Hooks{Now: func() time.Time { return time.Unix(1200, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	for pass := 0; pass < 3; pass++ {
		var parts []MovePart
		for _, b := range donor.owners[0].gc.inventory.bindings {
			d, err := v2part.ParseDescriptor(b)
			if err != nil {
				t.Fatal(err)
			}
			parts = append(parts, MovePart{Path: filepath.Join(donor.owners[0].dir, "parts", fmt.Sprintf("%x.sst", d.SHA256)), Descriptor: d})
		}
		prepared, err := PrepareMove(ctx, t.TempDir(), parts)
		if err != nil {
			t.Fatal(err)
		}
		defer prepared.Close()

		if err = receiver.AdoptMove(ctx, prepared, map[string]uint64{"move.metric": 7, "move.pad": 8}, 1200); err != nil {
			t.Fatal(err)
		}
		snapshot := receiver.DBs()[0].NewSnapshot()
		index, err := NewRollupSnapshot(ctx, snapshot, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		records, err := index.Records(ctx, "move.metric")
		if err != nil {
			t.Fatal(err)
		}
		snapshot.Close()
		found := false
		for _, r := range records {
			if r.Archive == 0 && r.Sample.Time < 1080 {
				t.Fatal("fixture retained old raw")
			}
			if r.Archive == 1 && r.Sample.Time == 1000 {
				found = r.Sample.Bits == math.Float64bits(15)
			}
		}
		if !found {
			t.Fatalf("pass %d lost retired coarse history: %+v", pass, records)
		}
		order := uint64(pass + 3)
		if err = donor.AppendBatches(0, []Batch{{ID: 8, Order: order, Clock: 1200, Policy: pad, Points: []Row{{ID: 8, Time: 1200 + int64(pass), Bits: math.Float64bits(1), Seq: 1}}}}, order, true); err != nil {
			t.Fatal(err)
		}
	}
}
