package chunkstore

import (
	"errors"
	"fmt"
	"math"
)

// EqualSnapshots compares physical archive contents without normalizing float bits.
// Storage IDs, generations and revisions are internal and do not affect replies.
func EqualSnapshots(want, got Snapshot) error {
	wm, gm := want.Metadata, got.Metadata
	if wm.Name != gm.Name || wm.AggregationMethod != gm.AggregationMethod || wm.XFilesFactor != gm.XFilesFactor || len(want.Archives) != len(got.Archives) {
		return errors.New("metadata mismatch")
	}
	for i, a := range want.Archives {
		b := got.Archives[i]
		if a.Retention != b.Retention || len(a.Points) != len(b.Points) {
			return fmt.Errorf("archive %d shape mismatch: source=%d destination=%d", i, len(a.Points), len(b.Points))
		}
		for j, p := range a.Points {
			q := b.Points[j]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				return fmt.Errorf("archive %d point %d bit mismatch", i, j)
			}
		}
	}
	return nil
}
