package downsize

import (
	"context"
	"errors"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type combineArchiveShape struct {
	Name                                         string
	Archive, Step, DonorPoints, SurvivorPoints   int
	DonorMin, DonorMax, SurvivorMin, SurvivorMax int64
	OverlapBuckets                               int64 // inclusive archive intervals in the range intersection
	SurvivorAfterDonor                           bool
}

type combineShape struct {
	PresentNames, SurvivorAfterDonorNames                           int
	PairedArchives, SurvivorAfterDonorArchives, OverlappingArchives int
	Archives                                                        []combineArchiveShape
}

// The measurement is outside all copy timers. These are logical time ranges,
// not circular-slot or physical-block intersections; empty archives are reported
// but excluded from the paired-archive denominator.
func measureCombineShape(ctx context.Context, donor, survivor *store.Store, names []string) (*combineShape, error) {
	result := &combineShape{}
	for _, name := range names {
		b, err := survivor.Snapshot(ctx, name)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		a, err := donor.Snapshot(ctx, name)
		if err != nil {
			return nil, err
		}
		result.PresentNames++
		paired, after := false, true
		for i, left := range a.Archives {
			right := b.Archives[i]
			row := combineArchiveShape{Name: name, Archive: i, Step: left.Retention.Step, DonorPoints: len(left.Points), SurvivorPoints: len(right.Points)}
			if len(left.Points) > 0 {
				row.DonorMin, row.DonorMax = left.Points[0].Timestamp, left.Points[len(left.Points)-1].Timestamp
			}
			if len(right.Points) > 0 {
				row.SurvivorMin, row.SurvivorMax = right.Points[0].Timestamp, right.Points[len(right.Points)-1].Timestamp
			}
			if len(left.Points) > 0 && len(right.Points) > 0 {
				paired = true
				result.PairedArchives++
				row.SurvivorAfterDonor = row.SurvivorMin > row.DonorMax
				if row.SurvivorAfterDonor {
					result.SurvivorAfterDonorArchives++
				} else {
					after = false
				}
				lo, hi := max(row.DonorMin, row.SurvivorMin), min(row.DonorMax, row.SurvivorMax)
				if lo <= hi {
					row.OverlapBuckets = (hi-lo)/int64(row.Step) + 1
					result.OverlappingArchives++
				}
			}
			result.Archives = append(result.Archives, row)
		}
		if paired && after {
			result.SurvivorAfterDonorNames++
		}
	}
	return result, nil
}
