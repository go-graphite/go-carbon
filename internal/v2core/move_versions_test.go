package v2core

import (
	"context"
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// A pre-attach prefix merge has written only the receiver's partial window.
// Attaching donor versions must still compute the union and survive later merges.
func TestMoveAdoptVersionsAcrossMerge(t *testing.T) {
	for _, mergeBefore := range []bool{false, true} {
		name := "after-attach"
		if mergeBefore {
			name = "before-attach"
		}
		t.Run(name, func(t *testing.T) { testMoveAdoptVersionsAcrossMerge(t, mergeBefore) })
	}
}

func testMoveAdoptVersionsAcrossMerge(t *testing.T, mergeBefore bool) {
	cfg := adoptConfig()
	cfg.DeferMerges = true
	e, err := Open(t.TempDir(), cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	policy := adoptPolicy(t, sealrollup.Sum)
	for order := uint64(1); order <= 2; order++ {
		adoptLive(t, e, policy, order, order, moveSample(1005, 6), moveSample(1006, 7), moveSample(1007, 8), moveSample(1008, 9), moveSample(1009, 10))
	}
	if mergeBefore {
		if _, err = e.ForceMerge(); err != nil {
			t.Fatal(err)
		}
	}
	part := adoptDonor(t, policy, moveSample(1000, 1), moveSample(1001, 2), moveSample(1002, 3), moveSample(1003, 4), moveSample(1004, 5))
	prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{part})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err = e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
		t.Fatal(err)
	}
	check := func(stage string) {
		_, records := adoptedRecords(t, e)
		for _, r := range records {
			if r.Archive == 1 && r.Sample.Time == 1000 && r.Sample.Bits == math.Float64bits(55) {
				return
			}
		}
		t.Fatalf("%s: incomplete attached window: %+v", stage, records)
	}
	check("pre-attach merge")
	adoptLive(t, e, policy, 3, 3, moveSample(1009, 10))
	if _, err = e.ForceMerge(); err != nil {
		t.Fatal(err)
	}
	check("post-attach merge")
}
