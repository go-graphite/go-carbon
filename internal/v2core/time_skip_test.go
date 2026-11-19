package v2core

import (
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// TestReadPartMayHoldSlots checks the ring mapping of a part's event-time
// coverage: a part is skipped only when no slot of its span is requested.
func TestReadPartMayHoldSlots(t *testing.T) {
	a := sealrollup.Archive{Step: 10, Count: 100} // ring of 1000 s
	part := func(lo, hi int64) *readPart { return &readPart{minTime: lo, maxTime: hi, timed: true} }
	for _, c := range []struct {
		name        string
		p           *readPart
		first, last int64
		want        bool
	}{
		{"unknown coverage", &readPart{}, 0, 1, true},
		{"inside", part(10_050, 10_059), 5, 6, true},
		{"before", part(10_050, 10_059), 6, 10, false},
		{"after", part(10_050, 10_059), 0, 5, false},
		{"span edge", part(10_050, 10_119), 11, 12, true},
		{"past span edge", part(10_050, 10_119), 12, 20, false},
		{"wrapped span tail", part(10_950, 11_019), 99, 100, true},
		{"wrapped span head", part(10_950, 11_019), 0, 2, true},
		{"wrapped span gap", part(10_950, 11_019), 2, 95, false},
		{"newer epoch shares the slot", part(21_050, 21_050), 5, 6, true},
		{"whole ring", part(10_000, 10_999), 42, 43, true},
	} {
		if got := c.p.mayHoldSlots(a, c.first, c.last); got != c.want {
			t.Errorf("%s: [%d,%d] slots [%d,%d) = %t, want %t", c.name, c.p.minTime, c.p.maxTime, c.first, c.last, got, c.want)
		}
	}
}
