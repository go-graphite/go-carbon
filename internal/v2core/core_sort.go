package v2core

import (
	"math/bits"
	"slices"
)

// Bucket the most significant varying ID byte before comparison sorting. The
// original head remains untouched until the result is known to be unambiguous:
// comparison-equal rows carrying different value bits use the original sort,
// preserving its tie behavior too. Mixed time partitions retain the original
// partition comparator. Scratch is private to one owner and reused at seals.
func sortCoreRowsBucketed(head []row, seconds int64, scratch []row) []row {
	if len(head) < 4096 {
		sortCoreRows(head, seconds)
		return scratch
	}
	low, high, base := head[0].Time, head[0].Time, head[0].ID
	var varied uint64
	for _, r := range head {
		low, high = min(low, r.Time), max(high, r.Time)
		varied |= r.ID ^ base
	}
	if varied == 0 || corePartition(low, seconds) != corePartition(high, seconds) {
		sortCoreRows(head, seconds)
		return scratch
	}
	shift := uint((bits.Len64(varied) - 1) &^ 7)
	var counts [256]int
	for _, r := range head {
		counts[byte(r.ID>>shift)]++
	}
	var offsets [256]int
	for i := 1; i < len(offsets); i++ {
		offsets[i] = offsets[i-1] + counts[i-1]
	}
	if cap(scratch) < len(head) {
		scratch = make([]row, len(head))
	} else {
		scratch = scratch[:len(head)]
	}
	for _, r := range head {
		b := byte(r.ID >> shift)
		scratch[offsets[b]] = r
		offsets[b]++
	}
	first := 0
	for _, n := range counts {
		slices.SortFunc(scratch[first:first+n], less)
		first += n
	}
	for i := 1; i < len(scratch); i++ {
		if less(scratch[i-1], scratch[i]) == 0 && scratch[i-1].Bits != scratch[i].Bits {
			sortCoreRows(head, seconds)
			return scratch
		}
	}
	copy(head, scratch)
	return scratch
}
