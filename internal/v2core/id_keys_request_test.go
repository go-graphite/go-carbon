package v2core

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// The request supplies the exact name. Routing and mixed-generation cursor
// selection must not reconstruct it from the dictionary again.
func TestIDKeysRequestName(t *testing.T) {
	v2part.IDKeys.Store(true)
	t.Cleanup(func() { v2part.IDKeys.Store(false) })
	t.Setenv("CORE_MERGE_WORKERS", "0")
	const name = "request.identity.metric"
	_, policy := rollupTestPolicy(t, name, "1s:1h,1m:1d", sealrollup.Last, .5)
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	cfg.SealInterval = time.Hour
	root := t.TempDir()
	// Keep a name-keyed part in the same store as an ID-keyed part.
	for _, byID := range []bool{false, true} {
		e, err := openCoreWithHooks(root, cfg, coreHooks{IDKeys: byID})
		if err != nil {
			t.Fatal(err)
		}
		order := uint64(1)
		if byID {
			order = 2
		}
		rows := []row{{ID: 1, Time: 1700000000 + int64(order), Bits: math.Float64bits(float64(order)), Seq: order}}
		err = e.submitBatches(0, []coreBatch{{ID: 1, Order: order, Clock: 1700000003, Policy: policy, Points: rows}}, order, true)
		if err != nil {
			_ = e.close()
			t.Fatal(err)
		}
		if !byID {
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		defer e.close()
		pin := e.owners[0].db.NewSnapshot()
		defer pin.Close()
		inventory, err := loadRollupInventory(e.owners[0].db)
		if err != nil {
			t.Fatal(err)
		}
		parts, err := OpenRollupParts(context.Background(), pin, inventory, e.identities, testBlockCache(t), 1<<20)
		if err != nil || parts == nil {
			t.Fatalf("open parts: %v", err)
		}
		defer parts.Close()
		index, err := parts.SnapshotForNames(context.Background(), pin, []string{name}, NewRollupReadCache(0))
		if err != nil {
			t.Fatal(err)
		}
		want, err := index.Records(context.Background(), name)
		if err != nil || len(want) == 0 {
			t.Fatalf("reference records: %v", err)
		}
		identity := index.metrics[name].latest.PolicyIdentity
		if identity.name.Load() != nil || identity.dictID.Load() == 0 {
			t.Fatal("fixture did not release its admission name")
		}
		// Only the handle's byte resolver is poisoned; the store dictionary
		// remains authoritative for the initial name-to-ID metadata lookup.
		catalog := identity.catalog
		identity.catalog = &PolicyCatalog{}
		defer func() { identity.catalog = catalog }()
		if index.MetricVersion(name) == nil {
			t.Fatal("missing part identity")
		}
		got, err := index.Records(context.Background(), name)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("request identity changed records: %v", err)
		}
	}
}
