package v2read

import (
	"context"
	"slices"
	"time"
)

// FetchSeries implements the narrow timeseries shape consumed by carbonserver.
// Its values are detached from read pins and belong to the caller. Metric carries
// the selected retention ladder, aggregation method and XFF for response metadata.
type FetchSeries struct {
	Metric            Metric
	from, until, step int
	values            []float64
}

func (s *FetchSeries) FromTime() int     { return s.from }
func (s *FetchSeries) UntilTime() int    { return s.until }
func (s *FetchSeries) Step() int         { return s.step }
func (s *FetchSeries) Values() []float64 { return s.values }

// Fetch is the detached single-name boundary used by fetch/render consumers.
// FetchMany avoids output copies when consumers serialize inside the callback.
func (r *Reader) Fetch(ctx context.Context, name string, from, until int) (*FetchSeries, error) {
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	var result *FetchSeries
	err := r.FetchMany(ctx, []string{name}, int64(from), int64(until), now().Unix(), func(_ string, s *Series) error {
		if s != nil {
			m := s.Metric
			m.Archives = slices.Clone(m.Archives)
			result = &FetchSeries{Metric: m, from: int(s.From), until: int(s.Until), step: int(s.Step), values: slices.Clone(s.Values)}
		}
		return nil
	})
	return result, err
}
