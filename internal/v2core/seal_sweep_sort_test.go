package v2core

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"
)

// TestSealKeysStable checks the seal sweep's application sort against a
// stable comparison sort on all three paths (insertion-size, radix, and the
// cold path for IDs of 32 bits or more), and the idle pass membership probe
// on both its bitset and binary-search forms.
func TestSealKeysStable(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, tc := range []struct {
		n     int
		maxID uint64
	}{{100, 50}, {5000, 900}, {5000, 1 << 20}, {5000, 1 << 27}, {3000, 1 << 40}} {
		batches := make([]coreBatch, tc.n)
		for i := range batches {
			id := rng.Uint64N(tc.maxID) + 1
			if tc.maxID > sealKeyPosMask {
				id = (rng.Uint64N(500) + 1) << 33 // wide IDs that still repeat
			}
			batches[i] = coreBatch{ID: id, Order: uint64(i)}
		}
		view := PendingBatches(batches)
		keys, _, maxID, wide, _ := view.sealKeys(nil, nil)
		keyID := func(k uint64) uint64 {
			if wide {
				return view.idAt(int(k & sealKeyPosMask))
			}
			return k >> 32
		}
		want := make([]uint64, tc.n)
		for i := range want {
			want[i] = uint64(i)
		}
		slices.SortStableFunc(want, func(a, b uint64) int { return cmp.Compare(batches[a].ID, batches[b].ID) })
		for i := range keys {
			if got := keys[i] & sealKeyPosMask; got != want[i] {
				t.Fatalf("n=%d maxID=%d: position %d holds batch %d, want %d (unstable or misordered)", tc.n, tc.maxID, i, got, want[i])
			}
		}
		staged := map[uint64]bool{}
		var seen []uint64
		if words := sealSeenWords(maxID, len(keys)); words != 0 {
			seen = make([]uint64, words)
		}
		for _, b := range batches {
			staged[b.ID] = true
			if seen != nil {
				seen[b.ID>>6] |= 1 << (b.ID & 63)
			}
		}
		for probe := 0; probe < 2000; probe++ {
			id := rng.Uint64N(tc.maxID + 2)
			if probe < len(batches) {
				id = batches[probe].ID
			}
			if got := sealKeySeen(keys, seen, maxID, id, keyID); got != staged[id] {
				t.Fatalf("n=%d maxID=%d bitset=%v: seen(%d) = %v, want %v", tc.n, tc.maxID, seen != nil, id, got, staged[id])
			}
		}
	}
}
