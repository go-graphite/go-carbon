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

// Stable LSD passes order portable numeric IDs, then comparison-sort only each
// ID's original time/sequence run. Original input remains untouched until all
// comparison-equal rows are known to carry identical bits; otherwise use the
// original unstable comparator to preserve its exact tie behavior.
func sortCoreRowsRadix(head []row, seconds int64, scratch, work []row) ([]row, []row) {
	if len(head) < 4096 {
		sortCoreRows(head, seconds)
		return scratch, work
	}
	low, high, base := head[0].Time, head[0].Time, head[0].ID
	var varied uint64
	for _, r := range head {
		low, high = min(low, r.Time), max(high, r.Time)
		varied |= r.ID ^ base
	}
	if varied == 0 || corePartition(low, seconds) != corePartition(high, seconds) {
		sortCoreRows(head, seconds)
		return scratch, work
	}
	if cap(scratch) < len(head) {
		scratch = make([]row, len(head))
	} else {
		scratch = scratch[:len(head)]
	}
	if cap(work) < len(head) {
		work = make([]row, len(head))
	} else {
		work = work[:len(head)]
	}
	src, dst := head, scratch
	for shift := uint(0); shift < 64; shift += 8 {
		if byte(varied>>shift) == 0 {
			continue
		}
		var counts [256]int
		for _, r := range src {
			counts[byte(r.ID>>shift)]++
		}
		var offsets [256]int
		for i := 1; i < 256; i++ {
			offsets[i] = offsets[i-1] + counts[i-1]
		}
		for _, r := range src {
			b := byte(r.ID >> shift)
			dst[offsets[b]] = r
			offsets[b]++
		}
		src = dst
		if &dst[0] == &scratch[0] {
			dst = work
		} else {
			dst = scratch
		}
	}
	for first := 0; first < len(src); {
		end := first + 1
		ordered := true
		for end < len(src) && src[end].ID == src[first].ID {
			if less(src[end-1], src[end]) > 0 {
				ordered = false
			}
			end++
		}
		if !ordered {
			slices.SortFunc(src[first:end], less)
		}
		for i := first + 1; i < end; i++ {
			if less(src[i-1], src[i]) == 0 && src[i-1].Bits != src[i].Bits {
				sortCoreRows(head, seconds)
				return scratch, work
			}
		}
		first = end
	}
	copy(head, src)
	return scratch, work
}
