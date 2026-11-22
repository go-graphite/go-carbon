package sealrollup

import (
	"math"
	"testing"
)

// TestWriteDownClosureRecursion: dropping a superseded raw version at the
// finest archive puts its level-1 window (540) in the closure. Level-1 slot 1
// also held the epoch at 780, whose level-2 parent (600) differs from the
// dropped version's own (0). The merge must write window 600 down too: its
// event walk reads level-1 slot 1, which the drop changes. Only the recursion
// over the slots of the parents reaches it.
func TestWriteDownClosureRecursion(t *testing.T) {
	c := Config{Method: Last, Archives: []Archive{{Step: 10, Count: 100}, {Step: 60, Count: 4}, {Step: 600, Count: 10}}}
	if slot(540, c.Archives[1]) != slot(780, c.Archives[1]) {
		t.Fatal("fixture: 540 and 780 must share a level-1 slot")
	}
	recs := []Version{
		{Archive: 0, Time: 540, Bits: math.Float64bits(1), Order: Tick(1, 0)},
		{Archive: 0, Time: 540, Bits: math.Float64bits(2), Order: Tick(2, 0)}, // supersedes the first
		{Archive: 0, Time: 780, Bits: math.Float64bits(3), Order: Tick(3, 0)},
	}
	keep, mats := WriteDown(c, recs, nil)
	if keep[0] || !keep[1] || !keep[2] {
		t.Fatalf("keep = %v, want only the superseded version dropped", keep)
	}
	has := func(a int, w int64) bool {
		for _, m := range mats {
			if m.Archive == a && m.Time == w {
				return true
			}
		}
		return false
	}
	for _, w := range [][2]int64{{1, 540}, {2, 0}, {2, 600}} {
		if !has(int(w[0]), w[1]) {
			t.Errorf("window (%d,%d) not written down; mats = %+v", w[0], w[1], mats)
		}
	}
}

// A seal fallback marks a closed window but cannot bound an exact read. A
// prefix merge upgrades it even when no raw version is superseded, then
// leaves that write-down alone until a newer event arrives.
func TestWriteDownUpgradesClosedWindow(t *testing.T) {
	c := Config{Method: Last, Archives: []Archive{{Step: 10, Count: 100}, {Step: 60, Count: 100}, {Step: 600, Count: 100}}}
	recs := []Version{
		{Archive: 0, Time: 10, Bits: math.Float64bits(1), Order: Tick(1, 0)},
		{Archive: 0, Time: 20, Bits: math.Float64bits(2), Order: Tick(2, 0)},
		{Archive: 1, Time: 0, Bits: math.Float64bits(99), Order: 1, Mat: true},
	}
	merge := func() []Version {
		t.Helper()
		keep, mats := WriteDown(c, recs, nil)
		var next []Version
		for i, r := range recs {
			if keep[i] {
				next = append(next, r)
			}
		}
		recs = append(next, mats...)
		return mats
	}
	check := func(mats []Version, want float64) {
		t.Helper()
		for _, m := range mats {
			if m.Archive == 1 && m.Time == 0 && m.Mat && m.Upto > 0 {
				if m.Bits != math.Float64bits(want) {
					t.Fatalf("written down bits = %x, want %x", m.Bits, math.Float64bits(want))
				}
				return
			}
		}
		t.Fatalf("closed window was not written down: %+v", mats)
	}
	check(merge(), 2)
	if mats := merge(); len(mats) != 0 {
		t.Fatalf("unchanged closed window rewritten: %+v", mats)
	}
	recs = append(recs, Version{Archive: 0, Time: 30, Bits: math.Float64bits(3), Order: Tick(3, 0)})
	check(merge(), 3)
	for _, m := range merge() {
		if m.Archive == 1 && m.Time == 0 {
			t.Fatalf("updated closed window rewritten: %+v", m)
		}
	}
}
