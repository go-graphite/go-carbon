package whisperio

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func absoluteAnchorFixture(t *testing.T, points []store.Point) (mapContinuationIO, store.Snapshot) {
	t.Helper()
	r := store.Retention{Step: 10, Count: 60}
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "anchor", Retentions: []store.Retention{r}, AggregationMethod: store.Sum}}, Archives: []store.Archive{{Retention: r, Points: points}}}
	kv := mapContinuationIO{}
	if err := (&ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, time.Now); err != nil {
		t.Fatal(err)
	}
	fs := newContinuationFiles(kv, false)
	file, err := fs.OpenFile(fs.main(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	f := file.(*continuationFile)
	if err := kv.DeletePrefix(continuationBlockPrefix(f.data.id, 'p', 0, 0)); err != nil {
		t.Fatal(err)
	}
	block := store.PointBlock{}
	for _, p := range points {
		slot := int(p.Timestamp / 10 % 60)
		block.Present[slot/64] |= uint64(1) << uint(slot%64)
		block.Points[slot] = p
		f.allocateClassicPages(f.data.layout.Archives[0].Offset+slot*12, f.data.layout.Archives[0].Offset+slot*12+12)
	}
	if err := f.setPointBlock(continuationBodyKey(f.data.id, 'p', 0, 0, 0), &block); err != nil {
		t.Fatal(err)
	}
	if err := fs.flush(); err != nil {
		t.Fatal(err)
	}
	return kv, snap
}
func applyAnchorPlan(t *testing.T, kv mapContinuationIO, plan store.AnchorRepairPlan) {
	t.Helper()
	for _, k := range plan.Deletes {
		kv.Delete(k)
	}
	for _, p := range plan.Writes {
		kv.Set(p.Key, p.Value)
	}
}

func TestAnchorRepairPreservesEveryPhysicalBit(t *testing.T) {
	ctx := context.Background()
	codec := &ContinuationCodec{}
	points := []store.Point{{Timestamp: 999780, Value: math.Copysign(0, -1)}, {Timestamp: 999810, Value: math.Float64frombits(0x7ff8000000000042)}, {Timestamp: 999990, Value: math.Inf(-1)}}
	kv, want := absoluteAnchorFixture(t, points)
	before := len(kv)
	plan, err := codec.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Archives) != 1 || !plan.Archives[0].Changed || plan.Archives[0].Points != 3 {
		t.Fatalf("wrong diagnosis: %+v", plan)
	}
	if len(kv) != before {
		t.Fatal("dry run mutated input")
	}
	applyAnchorPlan(t, kv, plan)
	got, err := codec.Snapshot(ctx, kv, want.Metadata, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(want, got); err != nil {
		t.Fatal(err)
	}
	again, err := codec.PlanArchiveAnchors(ctx, kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Writes)+len(again.Deletes) != 0 || again.Archives[0].Changed {
		t.Fatalf("not idempotent: %+v", again)
	}
}

func TestAnchorRepairBlocksMixedAliasedAndOffGridLayouts(t *testing.T) {
	for _, bad := range []store.Point{{Timestamp: 1000380, Value: 9}, {Timestamp: 999781, Value: 8}} {
		kv, _ := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999780, Value: 1}, {Timestamp: 999990, Value: 2}})
		// Simulate a foreground write after an empty-anchor import. A conflicting
		// point in a second physical slot must never be silently canonicalized.
		for k, v := range kv {
			key := []byte(k)
			if len(key) == 20 && key[9] == 'p' {
				var b store.PointBlock
				if err := store.DecodePointBlock(v, &b); err != nil {
					t.Fatal(err)
				}
				b.Present[0] |= 1 << 5
				b.Points[5] = bad
				kv[k] = store.EncodePointBlock(&b)
			}
		}
		plan, err := (&ContinuationCodec{}).PlanArchiveAnchors(context.Background(), kv)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Archives) == 0 || plan.Archives[0].Blocked == "" || len(plan.Writes)+len(plan.Deletes) != 0 {
			t.Fatalf("unsafe plan: %+v", plan)
		}
	}
}

func TestAnchorRepairKeepsHealthyNonMinimumAnchor(t *testing.T) {
	kv, _ := absoluteAnchorFixture(t, []store.Point{{Timestamp: 999600, Value: 1}, {Timestamp: 999780, Value: 2}, {Timestamp: 999990, Value: 3}})
	plan, err := (&ContinuationCodec{}).PlanArchiveAnchors(context.Background(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Writes)+len(plan.Deletes) != 0 {
		t.Fatal("rewrote consistent ring")
	}
	// Existing slot zero can have wrapped to a timestamp newer than the minimum;
	// relative modulo still validates every old physical position.
	for k, v := range kv {
		key := []byte(k)
		if len(key) == 20 && key[9] == 'p' && binary.BigEndian.Uint32(key[16:]) == 0 {
			var b store.PointBlock
			if err := store.DecodePointBlock(v, &b); err != nil {
				t.Fatal(err)
			}
			b.Points[0].Timestamp += 600
			kv[k] = store.EncodePointBlock(&b)
		}
	}
	plan, err = (&ContinuationCodec{}).PlanArchiveAnchors(context.Background(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Writes)+len(plan.Deletes) != 0 {
		t.Fatal("rewrote wrapped consistent ring")
	}
}

type anchorHeaderOnlyIO struct{ store.ContinuationIO }

func (k anchorHeaderOnlyIO) Get(key []byte) ([]byte, error) {
	if (len(key) > 0 && key[0] == 'T') || (len(key) >= 10 && key[0] == 'f' && (key[9] == 'c' || key[9] == 'b')) {
		return nil, fmt.Errorf("anchor census read compressed body")
	}
	return k.ContinuationIO.Get(key)
}
func (k anchorHeaderOnlyIO) Visit(key []byte, fn func([]byte, []byte) error) error {
	if (len(key) > 0 && key[0] == 'T') || (len(key) >= 10 && key[0] == 'f' && (key[9] == 'c' || key[9] == 'b')) {
		return fmt.Errorf("anchor census scanned compressed body")
	}
	return k.ContinuationIO.Visit(key, fn)
}

type anchorHeaderOnlyCodec struct{ *ContinuationCodec }

func (c anchorHeaderOnlyCodec) PlanArchiveAnchors(ctx context.Context, kv store.ContinuationIO) (store.AnchorRepairPlan, error) {
	return c.ContinuationCodec.PlanArchiveAnchors(ctx, anchorHeaderOnlyIO{kv})
}

func TestAnchorCensusSkipsCompressedHistory(t *testing.T) {
	ctx := context.Background()
	codec := anchorHeaderOnlyCodec{&ContinuationCodec{}}
	db, err := store.Open(t.TempDir(), coldOptions(codec))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	snapshot := coldFixture(t, db)
	coldSweep(t, db, "exact")
	before := db.Stats().TransferExport
	result, err := db.RepairArchiveAnchors(ctx, snapshot.Metadata.Name, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Archives) != 0 || result.Applied {
		t.Fatalf("compressed file changed %+v", result)
	}
	after := db.Stats().TransferExport
	if after.Requests != before.Requests || after.Metrics != before.Metrics {
		t.Fatal("census exported compressed history")
	}
}
