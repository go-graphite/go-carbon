package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestPerCallMetadataWriteThrough(t *testing.T) {
	ctx := context.Background()
	db, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, MetadataCacheSize: 64 << 20})
	defer closeTestStore(t, db)
	for round := range 4 {
		for i := range 100 {
			name := fmt.Sprintf("sparse.shape.%d", i)
			if round == 0 || i < 7 {
				if round != 0 {
					name = fmt.Sprintf("sparse.shape.%d.%d", round, i)
				}
				createTestMetric(t, db, name)
			}
			if err := db.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: float64(round)}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	stats := db.Stats().StateCache
	if stats.Misses != 0 || stats.Evictions != 0 || stats.Invalidations != 0 {
		t.Fatalf("write-through %+v", stats)
	}
	t.Logf("400 updates,7%% fresh names per subsequent round: %+v", stats)
}

func TestPerCallMissReasons(t *testing.T) {
	const stripe = 3
	c := newStateCache(1<<30, nil)
	c.locked(stripe, "absent")
	c.fillLocked(stripe, "x", testState("x", 1))
	c.beginMutation(stripe, "x")
	c.get(stripe, "x", c.epoch(stripe))
	old := c.epoch(stripe)
	c.installMutation(stripe, "x", testState("x", 2))
	c.endMutation(stripe, "x")
	c.get(stripe, "x", old)
	c.beginMutation(stripe, "x")
	c.endMutation(stripe, "x")
	c.locked(stripe, "x")
	stats := c.stats()
	if stats.Misses != 4 || stats.LockedMisses != 2 || stats.ReaderMisses != 2 || stats.PendingMisses != 1 || stats.StaleMisses != 1 || stats.Invalidations != 1 || stats.Evictions != 0 {
		t.Fatalf("miss reasons %+v", stats)
	}
}

func TestPerCallMiscRejectedNameShape(t *testing.T) {
	ctx := context.Background()
	db, now := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, MetadataCacheSize: 64 << 20})
	defer closeTestStore(t, db)
	// An already complete membership scan, as on the steady migrated store.
	f := newCatalogMembership(128)
	f.ready.Store(true)
	close(f.done)
	db.catalogMembership = f
	for i := range 93 {
		createTestMetric(t, db, fmt.Sprintf("sparse.%d", i))
	}
	for range 3 {
		for i := range 100 {
			name := fmt.Sprintf("sparse.%d", i)
			var want error
			if i >= 93 {
				name = "general.flink." + strings.Repeat("x", 300) + fmt.Sprintf(".%d.count", i)
				want = ErrNameTooLong
			}
			if err := db.UpdateMany(ctx, name, []Point{{Timestamp: int64(now - 60), Value: 1}}); !errors.Is(err, want) {
				t.Fatalf("%s: %v want %v", name, err, want)
			}
		}
	}
	stats := db.Stats().StateCache
	if stats.Misses != 0 || stats.Evictions != 0 || stats.Hits != 279 {
		t.Fatalf("93%% real metrics /7%% rejected names: %+v", stats)
	}
}

func TestPerCallTailAdmissionPreservesMetadata(t *testing.T) {
	first := testState("a", 1)
	c := newStateCache((2*first.size+64)*metricStripes, nil)
	const stripe = 2
	c.fillLocked(stripe, "a", first)
	c.fillLocked(stripe, "b", testState("b", 2))
	withTail := first.clone(first.meta)
	withTail.set([]byte{0, 't'}, make([]byte, 400))
	c.beginMutation(stripe, "a")
	c.installMutation(stripe, "a", withTail.seal())
	c.endMutation(stripe, "a")
	a, b := c.locked(stripe, "a"), c.locked(stripe, "b")
	if a == nil || b == nil {
		t.Fatal("a tail displaced metadata")
	}
	if _, known := a.lookup([]byte{0, 't'}); known {
		t.Fatal("optional tail admitted over budget")
	}
	stats := c.stats()
	if stats.Evictions != 0 || stats.SlotRefusals != 1 || stats.Oversized != 0 || stats.Invalidations != 0 {
		t.Fatalf("admission %+v", stats)
	}
}
