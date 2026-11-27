package v2core

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A fresh logical timestamp wins its reused physical slot even when the old
// epoch is local and the fresh epoch is copied.
func ringEpochPart(t *testing.T, policy []byte, source byte, id, order uint64, clock, timestamp int64, value float64) moveFixture {
	t.Helper()
	r := newCoreRollups()
	plan, err := r.plan(coreSealInput{Batches: []Batch{{ID: id, Order: order, Clock: clock, Policy: policy, Points: []Row{{ID: id, Time: timestamp, Bits: math.Float64bits(value), Seq: 1}}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	path := filepath.Join(t.TempDir(), "epoch.sst")
	d, err := WriteDenseRollupPart(path, plan.Files[0], v2part.Provenance{Incarnation: v2part.PartID{source}, Stream: "producer-0"})
	if err != nil {
		t.Fatal(err)
	}
	return moveFixture{path, d}
}

func ringEpochPolicy(t *testing.T) []byte {
	t.Helper()
	policy, err := EncodeRollupPolicy(RollupPolicy{Name: "move.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Sum, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestMoveCopiedFreshRingEpoch(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	policy := ringEpochPolicy(t)
	ingestLiveMovePart(t, db, ringEpochPart(t, policy, 2, 7, 1, 1010, 1000, 2))
	before := moveIndex(t, db, nil)
	if moveRaw(t, before)[1000] != math.Float64bits(2) {
		t.Fatal("missing old ring epoch")
	}
	fresh := ringEpochPart(t, policy, 1, 99, 100, 1110, 1100, 3)
	if _, err := InstallCopiedPart(context.Background(), db, dir, fresh.path, fresh.descriptor); err != nil {
		t.Fatal(err)
	}
	got := moveRaw(t, moveIndex(t, db, before))
	if got[1100] != math.Float64bits(3) {
		t.Fatalf("fresh copied t=1100 lost to expired live t=1000 in physical slot 0: %v", got)
	}
	if _, ok := got[1000]; ok {
		t.Fatal("old logical epoch remains visible")
	}
	if moveRaw(t, before)[1000] != math.Float64bits(2) {
		t.Fatal("pinned old epoch changed")
	}
	// A newer local order still cannot roll the ring back to an older epoch.
	ingestLiveMovePart(t, db, ringEpochPart(t, policy, 2, 7, 2, 1010, 1000, 4))
	if got := moveRaw(t, moveIndex(t, db, nil)); got[1100] != math.Float64bits(3) {
		t.Fatalf("new local order hid newer copied epoch: %v", got)
	}
}

func TestMoveAdoptFreshRingEpoch(t *testing.T) {
	policy := ringEpochPolicy(t)
	e, err := Open(t.TempDir(), adoptConfig(), Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	adoptLive(t, e, policy, 1, 1, moveSample(1000, 2))
	fresh := ringEpochPart(t, policy, 1, 99, 100, 1110, 1100, 3)
	prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{{Path: fresh.path, Descriptor: fresh.descriptor}})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err := e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1110); err != nil {
		t.Fatal(err)
	}
	_, records := adoptedRecords(t, e)
	for _, r := range records {
		if r.Archive == 0 && r.Sample.Time == 1100 && r.Sample.Bits == math.Float64bits(3) {
			return
		}
	}
	t.Fatalf("fresh copied t=1100 lost to expired live t=1000 in physical slot 0: %+v", records)
}
