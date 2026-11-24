package v2part

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func versionRecord(slot, ts int64, order, bits uint64, kind sealrollup.Kind) sealrollup.Record {
	return sealrollup.Record{Delta: sealrollup.Delta{Archive: 1, Slot: slot, Order: order, Sample: sealrollup.Sample{Time: 1700000000 + ts, Bits: bits, Sequence: order}}, Kind: kind}
}

func withUpto(r sealrollup.Record, upto uint64) sealrollup.Record {
	r.Upto = upto
	return r
}

// TestDenseRecordVersions checks the kinds column: a run of versions keeps
// every version, kind, slot repeat and upto; per-slot readers see the last Raw
// or seal-written Mat record of each slot, without kinds (windows a merge
// wrote down stay invisible to them); the version reader maps orders to ticks; Raw-only
// runs keep the older form; and a reader without kinds rejects versions.
func TestDenseRecordVersions(t *testing.T) {
	K := sealrollup.KindRaw
	rs := []sealrollup.Record{
		// slot 3: batch 5's point, a Ghost of batch 7 (another epoch took
		// the slot), then the winner, a closed window the seal wrote.
		versionRecord(3, 0, 5, 1, K), versionRecord(3, -3600, 7, 2, sealrollup.KindGhost), versionRecord(3, 0, 6, 3, sealrollup.KindMat),
		versionRecord(9, 600, 8, 4, K),
		// slot 12: batch 4's point superseded by batch 8's correction.
		versionRecord(12, 900, 4, 5, K), versionRecord(12, 900, 8, 6, K),
		versionRecord(13, 960, 8, 7, sealrollup.KindMat),
		// slot 14: a window a merge wrote down, then a written-down absence.
		withUpto(versionRecord(14, 1020, 9, 8, sealrollup.KindMat), sealrollup.Tick(9, 0)),
		withUpto(versionRecord(15, 1080, 9, 0, sealrollup.KindNone), sealrollup.Tick(10, 0)),
	}
	b, err := appendSlotsOrder(nil, rs, nil, ExactOrders)
	if err != nil || b[4]&denseExtended == 0 || b[4]&denseAligned != 0 {
		t.Fatalf("encode: mode %08b, %v", b[4], err)
	}
	all, err := DecodeRecordsInto(nil, 1, 3, b)
	if err != nil || !reflect.DeepEqual(all, rs) {
		t.Fatalf("versions round trip: %v\n got %+v\nwant %+v", err, all, rs)
	}
	live, err := DecodeSlotsInto(nil, 1, 3, b)
	want := []sealrollup.Record{rs[2], rs[3], rs[5], rs[6]}
	for i := range want {
		want[i].Kind = sealrollup.KindRaw // the per-slot view has no kinds
	}
	if err != nil || !reflect.DeepEqual(live, want) {
		t.Fatalf("per-slot view: %v\n got %+v\nwant %+v", err, live, want)
	}
	vs, _, err := DecodeVersionsInto(nil, nil, 1, 3, b)
	if err != nil || len(vs) != len(rs) {
		t.Fatal(err)
	}
	if v := vs[0]; v.Order != sealrollup.Tick(5, 1) || v.Upto != v.Order || v.Mat || v.Ghost {
		t.Errorf("raw version %+v", v)
	}
	if v := vs[1]; v.Order != sealrollup.Tick(7, 1) || !v.Ghost {
		t.Errorf("ghost version %+v", v)
	}
	if v := vs[2]; v.Order != 1 || v.Upto != 0 || !v.Mat {
		t.Errorf("fallback window version %+v", v)
	}
	if v := vs[7]; v.Order != sealrollup.Tick(9, 0) || v.Upto != sealrollup.Tick(9, 0) || !v.Mat {
		t.Errorf("written-down window version %+v", v)
	}
	if v := vs[8]; v.Upto != sealrollup.Tick(10, 0) || !v.None {
		t.Errorf("written-down absence version %+v", v)
	}
	// Upto belongs to Mat and None records only.
	if _, err := appendSlotsOrder(nil, []sealrollup.Record{withUpto(versionRecord(3, 0, 5, 1, K), 7)}, nil, ExactOrders); err == nil {
		t.Error("raw record with upto encoded")
	}
	// Raw-only runs, including the common aligned run, keep the older bytes.
	plain := []sealrollup.Record{versionRecord(3, 0, 5, 1, K), versionRecord(4, 60, 5, 2, K)}
	pb, err := appendSlotsOrder(nil, plain, nil, ExactOrders)
	if err != nil || pb[4]&denseExtended != 0 || pb[4]&denseAligned == 0 {
		t.Fatalf("raw run mode %08b, %v", pb[4], err)
	}
	// A reader without the kinds column must not misread versions.
	old := append([]byte(nil), b...)
	old[4] &^= denseExtended
	if _, err := DecodeSlotsInto(nil, 1, 3, old); err == nil {
		t.Fatal("versions decoded without the kinds column")
	}
	// The extension byte admits known columns only.
	for _, ext := range []byte{0, 2, 0x80} {
		bad := append([]byte(nil), b...)
		bad[5] = ext
		if _, err := DecodeSlots(1, 3, bad); err == nil {
			t.Fatalf("extension byte %#x accepted", ext)
		}
	}
	if _, err := appendSlotsOrder(nil, []sealrollup.Record{rs[1], rs[0]}, nil, ExactOrders); err != nil {
		t.Fatal("equal slots rejected", err)
	}
	if _, err := appendSlotsOrder(nil, []sealrollup.Record{rs[3], rs[0]}, nil, ExactOrders); err == nil {
		t.Fatal("descending slots accepted")
	}
}

// TestDenseVersionsKeepRun checks that a full run never splits one slot's
// versions: the planned writer starts the next run with them, and per-slot
// readers of the part see each slot's winner once.
func TestDenseVersionsKeepRun(t *testing.T) {
	spec := fixtureSpec()
	spec.Scope.VShard = GroupVShard
	spec.Encoding = DenseEncoding
	name := ID("svc.versions.m1")
	path := filepath.Join(t.TempDir(), "versions.sst")
	w, err := NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	var want []sealrollup.Record
	for s := int64(0); s < MaxDenseSlots+2; s++ {
		n := 1
		if s == MaxDenseSlots-1 {
			n = 3 // versions straddle the run boundary
		}
		for v := 0; v < n; v++ {
			r := versionRecord(s, 60*s, uint64(10+v), uint64(s*8+int64(v)), sealrollup.KindRaw)
			r.Archive = 0
			if err := w.SetPlannedRecord(Route(name), 0, name, &r); err != nil {
				t.Fatal(err)
			}
			if v == n-1 {
				want = append(want, r)
			}
		}
	}
	if err := w.SetPlannedMetadata(Route(name), 0, name, []byte("envelope")); err != nil {
		t.Fatal(err)
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	var got []sealrollup.Record
	runs := 0
	err = Read(context.Background(), path, &d, func(k Key, v []byte) error {
		if k.Archive == MetadataArchive {
			return nil
		}
		_, run, err := SplitHeader(v)
		if err != nil {
			return err
		}
		rs, err := DecodeSlots(k.Archive, k.BlockStart, run)
		runs++
		got = append(got, rs...)
		return err
	})
	if err != nil || runs != 2 || !reflect.DeepEqual(got, want) {
		t.Fatalf("runs %d, %v; got %d records, want %d", runs, err, len(got), len(want))
	}
	_ = os.Remove(path)
}
