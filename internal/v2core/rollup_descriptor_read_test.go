package v2core

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// A directory extended from a prior pin must read every selected original;
// descriptor reuse must neither omit a part nor follow a newer publication.
func TestRollupSnapshotDescriptorSelectionMatchesScan(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 1, 1, true, 1024
	engine, err := Open(t.TempDir(), cfg, Hooks{Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	ctx := context.Background()
	var policies [][]byte
	for i := range 2 {
		_, p := rollupTestPolicy(t, fmt.Sprintf("descriptors.%d", i), "1s:1h,1m:100d", sealrollup.Last, 0)
		policies = append(policies, p)
	}
	less := func(a, b sealrollup.Record) int {
		if a.Archive != b.Archive {
			return a.Archive - b.Archive
		}
		return int(a.Slot - b.Slot)
	}
	var views []*RollupSnapshot
	for order := uint64(1); order <= 3; order++ {
		now := int64(1794700800) + int64(order)*60
		var batches []Batch
		for i, p := range policies {
			batches = append(batches, Batch{ID: uint64(i + 1), Policy: p, Order: order, Clock: now, Points: []Row{{ID: uint64(i + 1), Time: now - 120, Bits: order + uint64(i), Seq: order}, {ID: uint64(i + 1), Time: now - 40*86400, Bits: order + uint64(i) + 10, Seq: order + 10}}})
		}
		if err := engine.AppendBatches(0, batches, order, true); err != nil {
			t.Fatal(err)
		}
		pin := engine.DBs()[0].NewSnapshot()
		defer pin.Close()
		var prior *RollupSnapshot
		if len(views) > 0 {
			prior = views[len(views)-1]
		}
		view, err := NewRollupSnapshot(ctx, pin, prior, nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(view.parts) < int(order) {
			t.Fatal("fixture did not retain multiple originals")
		}
		if order == 1 {
			identities := map[archivePartIdentity]bool{}
			for _, part := range view.parts {
				identities[archivePartIdentity{part.key.Group, part.key.Part}] = true
			}
			if len(view.parts) <= len(identities) {
				t.Fatal("fixture did not split one part across monthly files")
			}
		}
		views = append(views, view)
		for _, v := range views {
			seen := 0
			err := ScanRollups(v.snapshot, func(p RollupPolicy, _ RollupMetadata, want []sealrollup.Record) error {
				got, err := v.Records(ctx, string(p.Name))
				if err != nil {
					return err
				}
				slices.SortFunc(got, less)
				slices.SortFunc(want, less)
				if len(want) == 0 || !slices.Equal(got, want) {
					return fmt.Errorf("order %d %s: records differ from pinned scan", order, p.Name)
				}
				seen++
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if seen != len(policies) {
				t.Fatalf("scanned %d metrics, want %d", seen, len(policies))
			}
		}
	}
}
