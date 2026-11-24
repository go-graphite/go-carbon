package v2core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A publication that holds an opened part keeps the file through retirement
// even with no Pebble snapshot left at or before the retirement fence.
func TestHeldPartOutlivesRetirement(t *testing.T) {
	root := t.TempDir()
	db, g := gcFixture(t, root)
	defer db.Close()
	ctx := context.Background()
	name := "registry.held"
	_, raw := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
	plan, err := newCoreRollups().plan(coreSealInput{Batches: []Batch{{ID: 1, Policy: raw, Order: 1, Clock: 1700000002, Points: []Row{{ID: 1, Time: 1700000000, Bits: 0x7ff8000000000042, Seq: 1}}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	stage := filepath.Join(root, "stage.sst")
	d, err := WriteDenseRollupPart(stage, plan.Files[0], v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: "test"})
	if err != nil {
		t.Fatal(err)
	}
	kv, _, err := retainSealedPart(root, stage, d)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Set(kv.Key, kv.Value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err = db.Set(rollupDirectoryKey, []byte(root), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	inventory, err := loadRollupInventory(db)
	if err != nil {
		t.Fatal(err)
	}
	inventory.registry = g.inventory.registry
	snap := db.NewSnapshot()
	parts, err := OpenRollupParts(ctx, snap, inventory, nil, testBlockCache(t), 4<<20)
	if err != nil || parts == nil {
		t.Fatalf("open: %v %v", parts, err)
	}
	if err = snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err = g.retire([]v2part.Digest{d.SHA256}, gcAuthorize); err != nil {
		t.Fatal(err)
	}
	gcAdvance(g)
	// This snapshot follows the retirement, so only the registry fences the file.
	late := db.NewSnapshot()
	defer late.Close()
	if n, err := g.sweep(); err != nil || n != 0 {
		t.Fatalf("held part unlinked: %d %v", n, err)
	}
	view, err := parts.SnapshotForNames(ctx, late, []string{name}, NewRollupReadCache(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	records, err := view.Records(ctx, name)
	if err != nil || len(records) != 1 || records[0].Sample.Bits != 0x7ff8000000000042 {
		t.Fatalf("held part read: %v %v", records, err)
	}
	parts.Close()
	if g.inventory.registry.held(d.SHA256) {
		t.Fatal("released part still registered")
	}
	if n, err := g.sweep(); err != nil || n == 0 {
		t.Fatalf("released part retained: %d %v", n, err)
	}
	if _, err = os.Stat(gcPath(g, d)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released part file: %v", err)
	}
}
