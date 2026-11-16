package v2life

import (
	"context"
	"errors"
	"math"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Partition is one physical deletion unit. LongestSeconds is the maximum of
// the ACTUAL stored policies of every archive in the partition, not today's
// config lookup. End is the exclusive upper bound of all block intervals, not
// merely the latest input timestamp (rollup blocks can extend past it).
// A mixed-policy partition is kept until all its policies have expired. For
// per-schema physical reclamation writers must make policy-homogeneous units.
type Partition struct {
	Scope          Scope
	ID             string
	Key            int64 // exact partsformat time-partition identity
	Start, End     int64
	LongestSeconds int64
	Sealed         bool
}

// Reclaim records retired partitions and their range deletes in the same
// catalog commit. Existing readers keep their database snapshot pin; physical
// removal is left to the part engine. Copies must honor retired partitions.
func (o *Owner) Reclaim(ctx context.Context, partitions []Partition, now time.Time) ([]Partition, error) {
	drops, err := ExpiredPartitions(partitions, now)
	if err != nil || len(drops) == 0 {
		return drops, err
	}
	retired := make(map[int64]bool)
	for _, p := range o.view.Load().DroppedPartitions {
		retired[p.Key] = true
	}
	unique := drops[:0]
	for _, p := range drops {
		if p.Scope != o.view.Load().Scope {
			return nil, errors.New("partition outside owner scope")
		}
		if !retired[p.Key] {
			unique = append(unique, p)
			retired[p.Key] = true
		}
	}
	drops = unique
	if len(drops) == 0 {
		return drops, nil
	}
	return drops, o.apply(ctx, nil, nil, drops)
}

func LongestRetention(retentions []store.Retention) (int64, error) {
	var longest int64
	for _, r := range retentions {
		if r.Step <= 0 || r.Count <= 0 || int64(r.Step) > math.MaxInt64/int64(r.Count) {
			return 0, errors.New("invalid retention")
		}
		longest = max(longest, int64(r.Step)*int64(r.Count))
	}
	if longest == 0 {
		return 0, errors.New("missing retention")
	}
	return longest, nil
}

// ExpiredPartitions returns whole sealed units only; there is no per-point
// deletion API. Publication and file unlink/pin release belong to the engine.
func ExpiredPartitions(partitions []Partition, now time.Time) ([]Partition, error) {
	var out []Partition
	seen := make(map[struct {
		Scope Scope
		Key   int64
	}]bool)
	for _, p := range partitions {
		unit := struct {
			Scope Scope
			Key   int64
		}{p.Scope, p.Key}
		if seen[unit] {
			return nil, errors.New("duplicate physical deletion unit; aggregate all stored policies first")
		}
		seen[unit] = true
		if p.ID == "" || p.End <= p.Start || p.LongestSeconds <= 0 {
			return nil, errors.New("invalid partition manifest")
		}
		if !p.Sealed {
			continue
		}
		// Avoid overflow at the ends of the signed timestamp domain.
		if p.End <= math.MaxInt64-p.LongestSeconds && p.End+p.LongestSeconds <= now.Unix() {
			out = append(out, p)
		}
	}
	return out, nil
}
