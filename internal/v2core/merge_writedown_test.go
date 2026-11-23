package v2core

import (
	"encoding/hex"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// partVersions returns every stored version of the owner's live parts.
func partVersions(t *testing.T, o *coreOwner) []sealrollup.Version {
	t.Helper()
	o.gc.mu.Lock()
	bindings := o.gc.inventory.bindings
	o.gc.mu.Unlock()
	var out []sealrollup.Version
	for _, value := range bindings {
		d, err := v2part.ParseDescriptor(value)
		if err != nil {
			t.Fatal(err)
		}
		forEachPartKV(t, filepath.Join(o.dir, "parts", hex.EncodeToString(d.SHA256[:])+".sst"), func(key, value []byte) {
			k, err := v2part.DecodeKey(key)
			if err != nil {
				t.Fatal(err)
			}
			if k.Archive == v2part.MetadataArchive {
				return
			}
			if out, _, err = v2part.DecodeVersionsInto(out, nil, k.Archive, k.BlockStart, value); err != nil {
				t.Fatal(err)
			}
		})
	}
	return out
}

// versionImage is every physical slot's value under the version reader,
// without values expired at retire (readers filter them as merges drop them).
func versionImage(c sealrollup.Config, vs []sealrollup.Version, retire int64) map[[2]int64][2]uint64 {
	f := sealrollup.NewVersions(c, vs)
	img := map[[2]int64][2]uint64{}
	for a, ar := range c.Archives {
		for s := range ar.Count {
			if v, ok := f.SlotValue(a, s); ok && rollupRecordExpiry(c, sealrollup.Record{Delta: sealrollup.Delta{Archive: a, Sample: v}}) >= retire {
				img[[2]int64{int64(a), s}] = [2]uint64{uint64(v.Time), v.Bits}
			}
		}
	}
	return img
}

// TestMergeWriteDownVersionReads: tier merges and a forced merge rewrite
// parts holding versions (late points, same-slot corrections, ring wraps,
// direct coarse writes). Every coarse window the version reader computes from
// the merged parts equals what it computed from the sealed parts, so merges
// keep rollups a pure function of stored records; the per-slot reads do not
// change; and the merges did drop versions and write windows down.
func TestMergeWriteDownVersionReads(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	p, policy := rollupTestPolicy(t, "merge.metric", "1s:60,10s:600,60s:3600", sealrollup.Average, .3)
	for order := uint64(1); order <= 48; order++ {
		clock := int64(1000 + 5*order)
		near := clock - int64(order*13%50)
		rows := []row{
			{ID: 7, Time: near, Bits: order, Seq: order * 4},
			{ID: 7, Time: clock - 70 - int64(order%20), Bits: 1000 + order, Seq: order*4 + 1}, // beyond the 1s ring
			{ID: 7, Time: near, Bits: 2000 + order, Seq: order*4 + 2},                         // same-slot correction
			{ID: 7, Time: near - 60, Bits: 3000 + order, Seq: order*4 + 3},                    // takes the slot from another epoch
		}
		if err := e.submitBatches(0, []coreBatch{{ID: 7, Policy: policy, Order: order, Clock: clock, Points: rows}}, order, true); err != nil {
			t.Fatal(err)
		}
	}
	o := e.owners[0]
	c := p.Config
	sealed := partVersions(t, o)
	want := versionImage(c, sealed, o.retireClock())
	pin := o.db.NewSnapshot()
	slots := mergeRead(t, pin)
	_ = pin.Close()
	check := func(stage string) []sealrollup.Version {
		got := partVersions(t, o)
		if img := versionImage(c, got, o.retireClock()); !reflect.DeepEqual(img, want) {
			for k, v := range want {
				if img[k] != v {
					t.Errorf("%s: slot %v version read %v, sealed parts %v", stage, k, img[k], v)
				}
			}
			for k, v := range img {
				if _, ok := want[k]; !ok {
					t.Errorf("%s: slot %v version read %v, sealed parts none", stage, k, v)
				}
			}
			t.FailNow()
		}
		s := o.db.NewSnapshot()
		defer s.Close()
		live := func(rs []sealrollup.Record) map[sealrollup.Record]bool {
			out := map[sealrollup.Record]bool{}
			for _, r := range rs {
				if rollupRecordExpiry(c, r) >= o.retireClock() {
					out[r] = true
				}
			}
			return out
		}
		if got, want := live(mergeRead(t, s)), live(slots); !reflect.DeepEqual(got, want) {
			for r := range got {
				if !want[r] {
					t.Errorf("%s: per-slot record %+v is new", stage, r)
				}
			}
			for r := range want {
				if !got[r] {
					t.Errorf("%s: per-slot record %+v is gone", stage, r)
				}
			}
			t.FailNow()
		}
		return got
	}
	tiers := mergeDrain(t, o)
	check("tier merges")
	if _, err := e.ForceMerge(); err != nil {
		t.Fatal(err)
	}
	merged := check("forced merge")
	if mergeCount(o) != 1 {
		t.Fatalf("forced merge left %d parts", mergeCount(o))
	}
	writtenDown := 0
	for _, v := range merged {
		if (v.Mat || v.None) && v.Upto != 0 {
			writtenDown++
		}
	}
	t.Logf("tier merges %d; versions sealed %d, merged %d (%d written-down windows)", tiers, len(sealed), len(merged), writtenDown)
	if writtenDown == 0 || len(merged) >= len(sealed) {
		t.Fatalf("merges wrote %d windows down and kept %d of %d versions", writtenDown, len(merged), len(sealed))
	}
}
