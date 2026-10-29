package chunkstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestPurgeUpdatesCatalogAndSidecarInventory(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t, t.TempDir(), Options{CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, db)
	m := createTestMetric(t, db, "kept.metric")
	m.Name = strings.Repeat("x", 300)
	m.ID++
	value, err := db.encodeCatalog(m)
	if err != nil {
		t.Fatal(err)
	}
	batch := db.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(catalogKey(m.Name), value, nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
		t.Fatal(err)
	}
	summary := SidecarSummary{Present: true, Points: 2, Oldest: 100}
	if err := db.commitSidecars(batch, m.Name, &summary, db.commit); err != nil {
		t.Fatal(err)
	}
	_, cursor, _, err := db.CatalogChanges(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := db.PurgeInvalidNames(ctx); err != nil || n != 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
	if inventory := db.Sidecars(); inventory.Count != 0 || inventory.Points != 0 {
		t.Fatal(inventory)
	}
	if _, closer, err := db.db.Get(sidecarCatalogKey(m.Name)); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			closer.Close()
		}
		t.Fatalf("purged sidecar remains: %v", err)
	}
	if _, _, reset, err := db.CatalogChanges(ctx, cursor); err != nil || !reset {
		t.Fatalf("purge failed to invalidate catalog index: reset=%v %v", reset, err)
	}
	if _, err := db.Metadata(ctx, "kept.metric"); err != nil {
		t.Fatal(err)
	}
}
