package chunkstore

import "math"

// levelTargets mirrors Pebble's dynamic level sizing
// (compactionPickerByScore.initLevelMaxBytes, ignoring compactions in
// progress). The bottom level is sized from the whole database, the base level
// is the first non-empty level (raised while its target would exceed lbase),
// and the levels between grow by one smoothed multiplier. L0 and levels above
// the base have no size target and report zero.
func levelTargets(sizes [7]int64, lbase int64, multiplier int) [7]int64 {
	var targets [7]int64
	first, dbSize := -1, int64(0)
	for level := 1; level < len(sizes); level++ {
		if sizes[level] > 0 {
			if first < 0 {
				first = level
			}
			dbSize += sizes[level]
		}
	}
	if dbSize == 0 || lbase <= 0 || multiplier <= 0 {
		return targets
	}
	dbSize += sizes[0]
	bottom := dbSize - dbSize/int64(multiplier)
	size := float64(bottom)
	for level := len(sizes) - 2; level >= first; level-- {
		size /= float64(multiplier)
	}
	base := first
	for base > 1 && size > float64(lbase) {
		base--
		size /= float64(multiplier)
	}
	smoothed := 1.0
	if base < len(sizes)-1 {
		smoothed = math.Pow(float64(bottom)/float64(lbase), 1/float64(len(sizes)-1-base))
	}
	target := float64(lbase)
	for level := base; level < len(sizes); level++ {
		if level > base {
			target *= smoothed
		}
		targets[level] = int64(math.Round(target))
	}
	return targets
}
