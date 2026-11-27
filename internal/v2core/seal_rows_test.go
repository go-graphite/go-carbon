package v2core

import (
	"cmp"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// TestSealKeysMatchBatches reads every application of frozen pending views
// as the seal planner does (sealKeys order, app, samples) and requires
// exactly the decoded applications, stably sorted by ID. The views mix
// escapes (wide IDs, with and without, and wide times), multi-point packets,
// explicit Seq, clock changes, packet splits and cold segments between rows.
func TestSealKeysMatchBatches(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		s := newPendingRowsScript(seed)
		for _, narrow := range []bool{false, true} {
			var head pendingHead
			for pi, packet := range s.packets {
				if narrow {
					packet = slices.DeleteFunc(slices.Clone(packet), func(b coreBatch) bool { return b.ID > sealKeyPosMask })
				}
				if pi%5 == 3 {
					packet = slices.Clone(packet)
					for i := range packet {
						packet[i].Registered, packet[i].Policy = nil, []byte("cold")
					}
				}
				head.add(packet, s.find)
			}
			view := head.view(s.find)
			want := slices.Clone(view.Batches())
			slices.SortStableFunc(want, func(a, b coreBatch) int { return cmp.Compare(a.ID, b.ID) })
			keys, _, maxID, wide, points := view.sealKeys(nil, nil)
			if wide == narrow || len(keys) != len(want) {
				t.Fatalf("seed %d narrow %v: wide %v, %d keys for %d applications", seed, narrow, wide, len(keys), len(want))
			}
			total, top := 0, uint64(0)
			for i, k := range keys {
				a, w := view.app(int(k&sealKeyPosMask)), &want[i]
				if a.ID != w.ID || a.Order != w.Order || a.Clock != w.Clock || a.points() != len(w.Points) || (a.cold != nil) != (w.Policy != nil) || (!wide && k>>32 != w.ID) {
					t.Fatalf("seed %d narrow %v: application %d is id %d order %d clock %d points %d, want %d %d %d %d", seed, narrow, i, a.ID, a.Order, a.Clock, a.points(), w.ID, w.Order, w.Clock, len(w.Points))
				}
				got := make([]sealrollup.Sample, a.points())
				a.samples(got)
				for j, p := range w.Points {
					if want := (sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}); got[j] != want {
						t.Fatalf("seed %d narrow %v: application %d sample %d is %+v, want %+v", seed, narrow, i, j, got[j], want)
					}
				}
				total, top = total+len(w.Points), max(top, w.ID)
			}
			if points != total || maxID != top {
				t.Fatalf("seed %d narrow %v: %d points max ID %d, want %d and %d", seed, narrow, points, maxID, total, top)
			}
		}
	}
}
