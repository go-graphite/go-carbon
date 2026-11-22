package v2core

import "testing"

// TestSealScratchFollowsCurrentSeal pins that retained seal scratch is sized
// by the current seal: a first-wave peak is dropped once seals shrink, and
// steady-state scratch keeps its backing array.
func TestSealScratchFollowsCurrentSeal(t *testing.T) {
	peak := make([]uint64, 1<<20)
	if s := sealScratch(peak, 100_000); cap(s) > 2*100_000 || len(s) != 0 {
		t.Fatalf("after a 10x smaller seal: len %d cap %d, want the peak dropped", len(s), cap(s))
	}
	steady := make([]uint64, 150_000)
	if s := sealScratch(steady, 100_000); &s[:1][0] != &steady[0] {
		t.Fatal("scratch within twice the seal was reallocated")
	}
	if s := sealScratch(steady, 200_000); cap(s) < 200_000 || len(s) != 0 {
		t.Fatalf("growth: len %d cap %d", len(s), cap(s))
	}
	small := make([]uint64, 8000)
	if s := sealScratch(small, 0); &s[:1][0] != &small[0] {
		t.Fatal("small scratch dropped on an empty seal")
	}
}
