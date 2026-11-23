package v2core

import (
	"context"
	"encoding/hex"
	"math"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestPartSequenceComparison(t *testing.T) {
	record := func(stamp int64, order, seq uint64) sealrollup.Record {
		return sealrollup.Record{Delta: sealrollup.Delta{Order: order, Sample: sealrollup.Sample{Time: stamp, Sequence: seq}}}
	}
	for _, tc := range []struct {
		name   string
		a, b   partOrigin
		ar, br sealrollup.Record
		want   bool
	}{
		{"time", partOrigin{Class: CopiedPart}, partOrigin{Class: LivePart}, record(11, 1, 1), record(10, 100, 9), true},
		{"class", partOrigin{Class: LivePart}, partOrigin{Class: CopiedPart}, record(10, 1, 1), record(10, 100, 9), true},
		{"sequence", partOrigin{Class: LivePart}, partOrigin{Class: LivePart}, record(10, 1, 2), record(10, 100, 1), true},
		{"order", partOrigin{Class: LivePart}, partOrigin{Class: LivePart}, record(10, 3, 2), record(10, 2, 2), true},
		{"older sequence", partOrigin{Class: LivePart}, partOrigin{Class: LivePart}, record(10, 100, 1), record(10, 1, 2), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newerOrigin(tc.a, tc.ar, tc.b, tc.br)
			if err != nil || got != tc.want {
				t.Fatalf("winner=%v want=%v: %v", got, tc.want, err)
			}
		})
	}
}

// The newer part deliberately has a lower stored order, and a later part has
// an older timestamp. Reads, cached reads, history, recovery and merges must
// all choose the same record. A reset then fences parts, regardless of order.
func TestPartSequenceReadMergeHistoryRecovery(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for _, reset := range []uint64{0, 2} {
		t.Run(string(rune('0'+reset)), func(t *testing.T) {
			e := mergeFixture(t, t.TempDir(), nil)
			defer e.close()
			o := e.owners[0]
			policy, raw := rollupTestPolicy(t, "sequence.metric", "1s:1h,10s:2h", sealrollup.Last, 0)
			digest, _ := sealrollup.PolicyDigest(policy.Config)
			for i, order := range []uint64{90, 10, 100} {
				seq := uint64(i + 1)
				stamp := int64(1000)
				if i == 2 {
					stamp--
				}
				r := sealrollup.Record{Delta: sealrollup.Delta{Slot: stamp % 3600, Order: order, Sample: sealrollup.Sample{Time: stamp, Bits: math.Float64bits(float64(seq)), Sequence: order}}}
				md := RollupMetadata{Version: 4, ID: 7, Policy: raw, Revision: seq, ResetOrder: min(reset, seq), Frontier: sealrollup.Frontier{Order: 100, MaxTime: 1000, Policy: digest}}
				o.partSource.Seq = seq
				installMergeRecords(t, o, byte(seq), []RollupMetadata{md}, [][]sealrollup.Record{{r}})
			}
			o.partSource.Seq = 0
			snap := o.db.NewSnapshot()
			defer snap.Close()
			check := func(records []sealrollup.Record) {
				t.Helper()
				var seen bool
				for _, r := range records {
					if r.Sample.Time == 1000 {
						seen = true
						if reset != 0 || r.Sample.Bits != math.Float64bits(2) {
							t.Fatalf("wrong equal-time/reset winner: %+v", r)
						}
					}
				}
				if seen != (reset == 0) {
					t.Fatalf("reset visibility: seen=%v reset=%d", seen, reset)
				}
			}
			index, err := NewRollupSnapshot(context.Background(), snap, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				rs, err := index.Records(context.Background(), string(policy.Name))
				if err != nil {
					t.Fatal(err)
				}
				check(rs)
			}
			if err := ScanRollups(snap, func(_ RollupPolicy, _ RollupMetadata, rs []sealrollup.Record) error { check(rs); return nil }); err != nil {
				t.Fatal(err)
			}
			// Owner recovery uses only envelopes; full record validation shares
			// the snapshot reader rather than retaining another sample fold.
			if err := scanRollups(snap, nil, func(_ RollupPolicy, _ RollupMetadata, rs []sealrollup.Record) error { check(rs); return nil }); err != nil {
				t.Fatal(err)
			}
			var rs []sealrollup.Record
			var inputs []v2part.Descriptor
			for _, b := range o.gc.inventory.bindings {
				d, err := v2part.ParseDescriptor(b)
				if err != nil {
					t.Fatal(err)
				}
				inputs = append(inputs, d)
			}
			out := filepath.Join(o.dir, "sequence-merged.sst")
			merged, err := mergePartFiles(context.Background(), o.dir, out, inputs, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			if merged.Source.Seq != 3 {
				t.Fatalf("merged seq=%d", merged.Source.Seq)
			}
			rs = nil
			if err := v2part.ReadExpanded(context.Background(), out, &merged, func(k v2part.Key, b []byte) error {
				if k.Archive == v2part.MetadataArchive {
					return nil
				}
				r, err := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, b)
				rs = append(rs, r)
				return err
			}); err != nil {
				t.Fatalf("%s: %v", hex.EncodeToString(merged.SHA256[:]), err)
			}
			check(rs)
		})
	}
}
