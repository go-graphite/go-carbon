package v2core

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Each input has a disjoint dense run large enough for the whole-run path. A
// durable reset masks whole earlier runs; surviving runs must retain every bit
// and coalesce into one run that carries the envelope.
func TestSealedMergeDisjointEncodedRuns(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for _, reset := range []uint64{0, 8} {
		t.Run(fmt.Sprint(reset), func(t *testing.T) {
			e := mergeFixture(t, t.TempDir(), nil)
			defer e.close()
			policy, raw := rollupTestPolicy(t, "merge.blocks", "1s:1h,1m:1d", sealrollup.Last, .5)
			digest, err := sealrollup.PolicyDigest(policy.Config)
			if err != nil {
				t.Fatal(err)
			}
			for n := 1; n <= sealedMergeFanIn; n++ {
				var rs []sealrollup.Record
				for i := 0; i < 32; i++ {
					slot := int64(n*32 + i)
					order := uint64(n)
					rs = append(rs, sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: slot, Order: order, Sample: sealrollup.Sample{Time: 1000 + slot, Bits: []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000}[i%6], Sequence: order}}, Correction: i%3 == 0})
				}
				m := RollupMetadata{Version: 2, ID: 7, Policy: raw, Frontier: sealrollup.Frontier{Order: uint64(n), MaxTime: 2000, Policy: digest}, Revision: uint64(n)}
				if n == sealedMergeFanIn {
					m.ResetOrder = reset
				}
				installMergeRecords(t, e.owners[0], byte(n), []RollupMetadata{m}, [][]sealrollup.Record{rs})
			}
			s := e.owners[0].db.NewSnapshot()
			want := mergeRead(t, s)
			_ = s.Close()
			wantMerges := 1
			if sealedMergeFanIn < sealedMergeFanIn {
				wantMerges = sealedMergeFanIn/sealedMergeFanIn + 1
			}
			if n := mergeDrain(t, e.owners[0]); n != wantMerges || mergeCount(e.owners[0]) != 1 {
				t.Fatalf("merges=%d", n)
			}
			s = e.owners[0].db.NewSnapshot()
			defer s.Close()
			if got := mergeRead(t, s); !reflect.DeepEqual(got, want) {
				t.Fatal("encoded copy changed exact winners")
			}
			e.owners[0].gc.mu.Lock()
			inventory := e.owners[0].gc.inventory
			e.owners[0].gc.mu.Unlock()
			for _, value := range inventory.bindings {
				d, err := v2part.ParseDescriptor(value)
				if err != nil {
					t.Fatal(err)
				}
				// Surviving runs coalesce into one run that carries the envelope.
				if d.Coverage.Records != 1 {
					t.Fatalf("runs were not repacked or masked incorrectly: %d", d.Coverage.Records)
				}
				if err = v2part.ReadExpanded(context.Background(), gcPath(e.owners[0].gc, d), &d, nil); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// The name heap must match the canonical encoded order, including GCP3's
// length-prefixed name ordering. Most live names differ in length.
func TestSealedMergeMetricOrder(t *testing.T) {
	var cursors mergeHeap
	for i := 0; i < 1024; i++ {
		name := v2part.ID(fmt.Sprintf("namespace.%s.%d", strings.Repeat("a", i%47), i))
		// The merge decodes the key once and keeps the name in its bytes.
		key := v2part.Key{Group: "namespace", VShard: v2part.Route(name), Metric: name, Part: v2part.PartID{77}}
		enc, err := v2part.Encode(key)
		if err != nil {
			t.Fatal(err)
		}
		c := &mergeCursor{key: key, ident: enc[:len(enc)-16]}
		c.name = len(c.ident) - len(name) - 4
		cursors = append(cursors, c)
	}
	slices.SortFunc(cursors, func(a, b *mergeCursor) int {
		x, err := v2part.Encode(a.key)
		if err != nil {
			t.Fatal(err)
		}
		y, err := v2part.Encode(b.key)
		if err != nil {
			t.Fatal(err)
		}
		return bytes.Compare(x, y)
	})
	for i := 1; i < len(cursors); i++ {
		if !cursors.Less(i-1, i) || cursors.Less(i, i-1) {
			t.Fatalf("metric heap differs from encoded order at %d", i)
		}
	}
}
