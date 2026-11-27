package v2core

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestRollupInventoryAddsOnlyChangedNamesAndRewinds(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	ctx := context.Background()
	var pins []*pebble.Snapshot
	defer func() {
		for _, p := range pins {
			_ = p.Close()
		}
	}()
	seal := func(name string, id, order uint64, prior *RollupSnapshot) *RollupSnapshot {
		t.Helper()
		_, policy := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
		if err := e.submitBatches(0, []coreBatch{{ID: id, Order: order, Clock: 1700000002, Policy: policy, Points: []row{{ID: id, Time: 1700000000, Bits: order, Seq: order}}}}, order, true); err != nil {
			t.Fatal(err)
		}
		p := e.owners[0].db.NewSnapshot()
		pins = append(pins, p)
		s, err := NewRollupSnapshot(ctx, p, prior, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	first := seal("inventory.a", 1, 1, nil)
	second := seal("inventory.b", 2, 2, first)
	third := seal("inventory.a", 1, 3, second)
	if len(third.inventory) != 3 {
		t.Fatalf("descriptor inventory: %d", len(third.inventory))
	}
	if first.metrics["inventory.a"] != second.metrics["inventory.a"] {
		t.Fatal("unaffected name rebuilt")
	}
	if second.metrics["inventory.b"] != third.metrics["inventory.b"] {
		t.Fatal("unaffected name rebuilt on correction")
	}
	if first.metrics["inventory.a"].latest.meta.Frontier.Order != 1 || len(first.metricParts("inventory.a", first.metrics["inventory.a"])) != 1 {
		t.Fatal("old pinned directory mutated")
	}
	if len(third.metricParts("inventory.a", third.metrics["inventory.a"])) != 2 {
		t.Fatal("added part missing")
	}
	rewind, err := NewRollupSnapshot(ctx, pins[0], third, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rewind.metrics["inventory.b"] != nil || rewind.metrics["inventory.a"].latest.meta.Frontier.Order != 1 {
		t.Fatal("removed parts survived rewind")
	}
	for _, s := range []*RollupSnapshot{first, second, third, rewind} {
		err := ScanRollups(s.snapshot, func(policy RollupPolicy, _ RollupMetadata, want []sealrollup.Record) error {
			got, err := s.Records(ctx, string(policy.Name))
			if err != nil {
				return err
			}
			slices.SortFunc(got, func(a, b sealrollup.Record) int {
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
			})
			if !slices.Equal(got, want) {
				t.Fatalf("inventory differs from full scan for %s", policy.Name)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublishedInventoryMatchesPinnedBindings(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	type publication struct {
		pin       *pebble.Snapshot
		inventory *RollupInventory
	}
	var publications []publication
	var mu sync.Mutex
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PublishBatchesWithInventory: func(_ int, pin *pebble.Snapshot, _ []coreBatch, _ uint64, inventory *RollupInventory) error {
		mu.Lock()
		publications = append(publications, publication{pin, inventory})
		mu.Unlock()
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	defer func() {
		for _, p := range publications {
			if p.pin != nil {
				_ = p.pin.Close()
			}
		}
	}()
	for order := uint64(1); order <= 3; order++ {
		_, policy := rollupTestPolicy(t, "inventory.live", "1s:1d", sealrollup.Last, .5)
		if err = e.submitBatches(0, []coreBatch{{ID: 1, Order: order, Clock: 1700000002, Policy: policy, Points: []row{{ID: 1, Time: 1700000000, Bits: order, Seq: order}}}}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	// Close producer goroutines before inspecting the callback-owned slice.
	// Pins keep all old inventories valid independently of later publication.
	mu.Lock()
	defer mu.Unlock()
	var prior *RollupSnapshot
	for i, p := range publications {
		if p.pin == nil {
			// A head-only publication shares its predecessor's pin and inventory.
			if i == 0 || p.inventory != publications[i-1].inventory {
				t.Fatal("unpinned publication changed the binding set")
			}
			continue
		}
		got, err := NewRollupSnapshot(context.Background(), p.pin, prior, nil, p.inventory)
		if err != nil {
			t.Fatal(err)
		}
		want, err := NewRollupSnapshot(context.Background(), p.pin, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		a, err := got.Records(context.Background(), "inventory.live")
		if err != nil {
			t.Fatal(err)
		}
		b, err := want.Records(context.Background(), "inventory.live")
		if err != nil {
			t.Fatal(err)
		}
		slices.SortFunc(a, recordOrderForInventory)
		slices.SortFunc(b, recordOrderForInventory)
		if !slices.Equal(a, b) {
			t.Fatal("published inventory differs from snapshot scan")
		}
		prior = got
	}
	if len(publications[0].inventory.bindings) != 0 {
		t.Fatal("initial immutable binding map mutated")
	}
}
func recordOrderForInventory(a, b sealrollup.Record) int {
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
