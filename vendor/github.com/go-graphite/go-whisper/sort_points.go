package whisper

import "sort"

// dataPointsByInterval sorts with typed swaps; sort.Slice's reflection
// swapper dominated sorting large classic archive reads. sort.Sort and
// sort.Slice run the same generated algorithm, so equal intervals end in the
// same order as before.
type dataPointsByInterval []dataPoint

func (p dataPointsByInterval) Len() int           { return len(p) }
func (p dataPointsByInterval) Less(i, j int) bool { return p[i].interval < p[j].interval }
func (p dataPointsByInterval) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// sortDataPointsByInterval orders points by interval with the same result as
// sort.Slice(points, interval less). A classic ring read is one strictly
// increasing run, or two whose second ends before the first starts; every
// interval is then distinct, the sorted order is unique, and a rotation
// produces it without sorting.
func sortDataPointsByInterval(points []dataPoint) {
	n := len(points)
	cut := 0
	for i := 1; i < n; i++ {
		if points[i].interval <= points[i-1].interval {
			if cut != 0 {
				cut = -1
				break
			}
			cut = i
		}
	}
	switch {
	case cut == 0:
		return
	case cut > 0 && points[n-1].interval < points[0].interval:
		head := append([]dataPoint(nil), points[:cut]...)
		copy(points, points[cut:])
		copy(points[n-cut:], head)
		return
	}
	sort.Sort(dataPointsByInterval(points))
}
