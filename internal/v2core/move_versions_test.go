package v2core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A pre-attach prefix merge has written only the receiver's partial window.
// Attaching donor versions must still compute the union and survive later merges.
func TestMoveAdoptVersionsAcrossMerge(t *testing.T) {
	for _, mergeBefore := range []bool{false, true} {
		for _, donorMat := range []bool{false, true} {
			for _, restart := range []bool{false, true} {
				t.Run(fmt.Sprintf("merge-before=%v/donor-mat=%v/restart=%v", mergeBefore, donorMat, restart), func(t *testing.T) { testMoveAdoptVersionsAcrossMerge(t, mergeBefore, donorMat, restart) })
			}
		}
	}
}

func testMoveAdoptVersionsAcrossMerge(t *testing.T, mergeBefore, donorMat, restart bool) {
	cfg := adoptConfig()
	cfg.DeferMerges = true
	root := t.TempDir()
	e, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close() }()
	reopen := func() {
		if !restart {
			return
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		e, err = Open(root, cfg, Hooks{})
		if err != nil {
			t.Fatal(err)
		}
	}
	policy := adoptPolicy(t, sealrollup.Sum)
	for order := uint64(1); order <= 2; order++ {
		adoptLive(t, e, policy, order, order, moveSample(1005, 6), moveSample(1006, 7), moveSample(1007, 8), moveSample(1008, 9), moveSample(1009, 10), moveSample(980, 4), moveSample(981, 4), moveSample(982, 4), moveSample(983, 4), moveSample(984, 4))
	}
	if mergeBefore {
		if _, err = e.ForceMerge(); err != nil {
			t.Fatal(err)
		}
	}
	reopen()
	part := adoptDonor(t, policy, moveSample(1000, 1), moveSample(1001, 2), moveSample(1002, 3), moveSample(1003, 4), moveSample(1004, 5), moveSample(990, 3), moveSample(991, 3), moveSample(992, 3), moveSample(993, 3), moveSample(994, 3))
	if donorMat {
		donor, err := Open(t.TempDir(), cfg, Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		defer donor.Close()
		for order := uint64(1); order <= 2; order++ {
			adoptLive(t, donor, policy, order, order, moveSample(1000, 1), moveSample(1001, 2), moveSample(1002, 3), moveSample(1003, 4), moveSample(1004, 5), moveSample(990, 3), moveSample(991, 3), moveSample(992, 3), moveSample(993, 3), moveSample(994, 3))
		}
		if _, err = donor.ForceMerge(); err != nil {
			t.Fatal(err)
		}
		for _, value := range donor.owners[0].gc.inventory.bindings {
			d, err := v2part.ParseDescriptor(value)
			if err != nil {
				t.Fatal(err)
			}
			part = MovePart{Path: filepath.Join(t.TempDir(), "donor-mat.sst"), Descriptor: d}
			if err = os.Link(filepath.Join(donor.owners[0].dir, "parts", fmt.Sprintf("%x.sst", d.SHA256)), part.Path); err != nil {
				t.Fatal(err)
			}
		}
	}
	prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{part})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if err = e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
		t.Fatal(err)
	}
	value, closer, err := e.DBs()[0].Get(moveAuthorityKey("move.metric"))
	if err != nil {
		t.Fatal(err)
	}
	var authority moveAuthority
	err = json.Unmarshal(value, &authority)
	closer.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(authority.Coarse) != 1 || authority.Coarse[0].Kind != sealrollup.KindMat || authority.Coarse[0].Sample.Bits != math.Float64bits(55) || authority.Coarse[0].Upto != sealrollup.Tick(2, 0) {
		t.Fatalf("adopt must persist exact union Mat at receiver order: %+v", authority)
	}
	check := func(stage string) {
		_, records := adoptedRecords(t, e)
		got := map[int64]uint64{}
		for _, r := range records {
			if r.Archive == 1 {
				got[r.Sample.Time] = r.Sample.Bits
			}
		}
		// The union changes only the mixed window; single-stream history
		// retains its own Mat/value through both merge orders and restarts.
		for stamp, want := range map[int64]float64{980: 20, 990: 15, 1000: 55} {
			if got[stamp] != math.Float64bits(want) {
				t.Fatalf("%s: window %d got=%v want=%v; records=%+v", stage, stamp, math.Float64frombits(got[stamp]), want, records)
			}
		}
	}
	check("pre-attach merge")
	reopen()
	check("adopt restart")
	adoptLive(t, e, policy, 3, 3, moveSample(1009, 10))
	beforeMerge := partVersions(t, e.owners[0])
	if _, err = e.ForceMerge(); err != nil {
		t.Fatal(err)
	}
	// This merge excludes the copied parts, so it must neither write a
	// receiver-only Mat nor retire a dependency hidden by the union Mat.
	assertMoveDependencies(t, beforeMerge, partVersions(t, e.owners[0]))
	check("post-attach merge")
	reopen()
	check("merge restart")
}

func assertMoveDependencies(t *testing.T, before, after []sealrollup.Version) {
	t.Helper()
	dependencies := map[sealrollup.Version]int{}
	for _, v := range before {
		dependencies[v]++
	}
	for _, v := range after {
		dependencies[v]--
	}
	for v, count := range dependencies {
		if count != 0 {
			t.Fatalf("merge excluding copied inputs changed dependency %v by %d", v, count)
		}
	}
}
