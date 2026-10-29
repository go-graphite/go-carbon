package chunkstore

import (
	"math"
	"testing"
)

func TestLevelTargets(t *testing.T) {
	const gb = int64(1) << 30
	// A populated L1 pins the base level there; targets grow from lbase to the
	// bottom by one ratio.
	sizes := [7]int64{4 * gb, 14 * gb, 25 * gb, 45 * gb, 71 * gb, 164 * gb, 1340 * gb}
	got := levelTargets(sizes, 4*gb, 10)
	if got[0] != 0 || got[1] != 4*gb {
		t.Fatalf("L0/L1 targets %v", got[:2])
	}
	var db int64
	for _, s := range sizes {
		db += s
	}
	if bottom := db - db/10; math.Abs(float64(got[6]-bottom)) > 1 {
		t.Fatalf("bottom target %d, want %d", got[6], bottom)
	}
	ratio := float64(got[2]) / float64(got[1])
	for level := 3; level < 7; level++ {
		if r := float64(got[level]) / float64(got[level-1]); math.Abs(r-ratio) > 1e-6 {
			t.Fatalf("level %d ratio %.4f, want %.4f", level, r, ratio)
		}
	}
	// With empty upper levels the base level moves up from the first
	// non-empty level only while its target exceeds lbase; levels above it
	// have no target.
	sparse := [7]int64{gb, 0, 0, 0, 22 * gb, 155 * gb, 1340 * gb}
	got = levelTargets(sparse, 4*gb, 10)
	if got[1] != 0 || got[2] != 0 || got[3] != 4*gb {
		t.Fatalf("sparse targets %v", got)
	}
	if levelTargets([7]int64{}, 64<<20, 10) != ([7]int64{}) {
		t.Fatal("empty database has targets")
	}
}
