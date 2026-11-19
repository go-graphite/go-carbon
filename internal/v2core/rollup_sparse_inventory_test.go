package v2core

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type inventoryWorkContext struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int64
}

func (c *inventoryWorkContext) Err() error {
	if c.checks.Add(1) > 64 {
		c.cancel()
	}
	return c.Context.Err()
}

// A deterministic work bound complements the HTTP latency regression: a
// two-shard part must not enumerate the thousands of absent shards between it.
func TestSparseRollupInventoryWorkBound(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	var batches []coreBatch
	for _, shard := range []uint16{0, v2part.VirtualShards - 1} {
		for candidate := 0; ; candidate++ {
			name := fmt.Sprintf("sparse.inventory.%d", candidate)
			if v2part.Route(v2part.ID(name)) != shard {
				continue
			}
			_, policy := rollupTestPolicy(t, name, "1s:1d,1m:30d", sealrollup.Last, .5)
			id := uint64(len(batches) + 1)
			batches = append(batches, coreBatch{ID: id, Policy: policy, Clock: 1700000002, Points: []row{{ID: id, Time: 1700000000, Bits: id}}})
			break
		}
	}
	var prior *RollupSnapshot
	for order := uint64(1); order <= 2; order++ {
		for i := range batches {
			batches[i].Order, batches[i].Points[0].Seq = order, order
		}
		if err := e.submitBatches(0, batches, order, true); err != nil {
			t.Fatal(err)
		}
		pin := e.owners[0].db.NewSnapshot()
		defer pin.Close()
		ctx, cancel := context.WithCancel(context.Background())
		work := &inventoryWorkContext{Context: ctx, cancel: cancel}
		inventory, scopes, err := rollupInventory(work, pin, prior, nil)
		cancel()
		if err != nil {
			t.Fatalf("generation %d inventory exceeded actual-shard work bound (%d context checks): %v", order, work.checks.Load(), err)
		}
		if len(inventory) != int(order) || len(scopes) != int(order)*2 {
			t.Fatalf("incomplete inventory: parts=%d scopes=%d", len(inventory), len(scopes))
		}
		prior, err = NewRollupSnapshot(context.Background(), pin, prior, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Existing stores omit VShards. They must still open and yield identical
	// records through the one-pass physical-scope fallback.
	db := e.owners[0].db
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	defer batch.Close()
	for it.First(); it.Valid(); it.Next() {
		d, err := v2part.ParseDescriptor(it.Value())
		if err != nil || len(d.VShards) != 2 {
			t.Fatalf("seal did not record actual shards: %v %v", d.VShards, err)
		}
		d.VShards = nil
		value, err := d.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(it.Key(), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	pin := db.NewSnapshot()
	defer pin.Close()
	legacy, err := NewRollupSnapshot(context.Background(), pin, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name := range prior.metrics {
		want, err := prior.Records(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		got, err := legacy.Records(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		slices.SortFunc(want, recordOrderForInventory)
		slices.SortFunc(got, recordOrderForInventory)
		if !slices.Equal(got, want) {
			t.Fatal("legacy inventory changed record bits")
		}
	}
}
