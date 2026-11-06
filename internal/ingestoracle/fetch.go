// Package ingestoracle contains private evaluation gates, never apply logic.
package ingestoracle

import (
	"context"
	"fmt"
	"math"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type Fetcher interface {
	Fetch(context.Context, string, int, int) (*store.Series, error)
}

// VerifyFetch checks the serving reader as well as the physical Snapshot. Only
// timestamps in each archive's current readable window are compared; expired
// physical history may legitimately remain stored. A finite render must match
// Float64bits, including zero signs and infinities. NaN payload preservation is
// checked by the raw/point oracle: Fetch uses NaN for missing data too.
func VerifyFetch(ctx context.Context, db Fetcher, snapshot store.Snapshot, now int64) (archives, points int64, err error) {
	for _, archive := range snapshot.Archives {
		r := archive.Retention
		from := max(int64(1), now-int64(r.Step)*int64(r.Count)+1)
		until := now - 1
		if from > until {
			continue
		}
		series, err := db.Fetch(ctx, snapshot.Metadata.Name, int(from), int(until))
		if err != nil {
			return archives, points, err
		}
		archives++
		if series == nil {
			for _, p := range archive.Points {
				if p.Timestamp > from && p.Timestamp <= until && !math.IsNaN(p.Value) {
					return archives, points, fmt.Errorf("Fetch %s step%d lost stored point at%d", snapshot.Metadata.Name, r.Step, p.Timestamp)
				}
			}
			continue
		}
		if series.Step != r.Step {
			return archives, points, fmt.Errorf("Fetch %s selected step%d, expected%d", snapshot.Metadata.Name, series.Step, r.Step)
		}
		want := make(map[int64]uint64, len(archive.Points))
		for _, p := range archive.Points {
			offset := p.Timestamp - int64(series.FromTime)
			if offset < 0 || offset%int64(series.Step) != 0 || offset/int64(series.Step) >= int64(len(series.Values)) || math.IsNaN(p.Value) {
				continue
			}
			i := int(offset / int64(series.Step))
			if math.Float64bits(series.Values[i]) != math.Float64bits(p.Value) {
				return archives, points, fmt.Errorf("Fetch %s step%d timestamp%d differs from stored bits: %016x/%016x", snapshot.Metadata.Name, r.Step, p.Timestamp, math.Float64bits(series.Values[i]), math.Float64bits(p.Value))
			}
			want[p.Timestamp] = math.Float64bits(p.Value)
			points++
		}
		for i, value := range series.Values {
			if math.IsNaN(value) {
				continue
			}
			timestamp := int64(series.FromTime) + int64(i)*int64(series.Step)
			bits, ok := want[timestamp]
			if !ok || bits != math.Float64bits(value) {
				return archives, points, fmt.Errorf("Fetch %s step%d exposes unexpected point at%d", snapshot.Metadata.Name, r.Step, timestamp)
			}
		}
	}
	return archives, points, nil
}
