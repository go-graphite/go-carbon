package chunkstore

import (
	"context"
	"math"
	"slices"
)

// SameReadRevision includes the data revision and archive interpretation. A
// generation/revision alone cannot validate values after a configuration change.
func (m Metadata) SameReadRevision(other Metadata) bool {
	return m.ID == other.ID && m.Generation == other.Generation && m.Revision == other.Revision &&
		m.Continuation == other.Continuation && m.AggregationMethod == other.AggregationMethod &&
		math.Float32bits(m.XFilesFactor) == math.Float32bits(other.XFilesFactor) && slices.Equal(m.Retentions, other.Retentions)
}

// FetchWithPrefix validates an immutable finest-archive prefix in the same read
// view that fetches the remainder. A revision miss follows the ordinary Fetch
// path once, without a separate Metadata read or a metadata-only cache fill.
// The caller owns prefix; returned values are always detached from it. epoch is
// the ReadCacheEpoch at prefix admission. reused counts prefix values returned.
func (s *Store) FetchWithPrefix(ctx context.Context, name string, from, until int, prefix *Series, epoch uint64) (series *Series, reused int, err error) {
	p := fetchPrefix{series: prefix, epoch: epoch}
	series, err = s.fetch(ctx, name, from, until, &p)
	// An import can change namespace independently of the revision; a clock or
	// archive boundary can also change the tail's grid. Fail closed in those rare
	// races, just as a normal Fetch takes a new view on retry.
	if err == nil && p.count > 0 && (series == nil || s.ReadCacheEpoch() != epoch) {
		series, err = s.Fetch(ctx, name, from, until)
		return series, 0, err
	}
	if err != nil {
		return series, 0, err
	}
	return series, p.count, nil
}

type fetchPrefix struct {
	series                          *Series
	epoch                           uint64
	start, end, until, first, count int
}

func (p *fetchPrefix) plan(m Metadata, from, until int, now int64, epoch uint64) bool {
	old := p.series
	if old == nil || epoch != p.epoch || !m.SameReadRevision(old.Metadata) ||
		len(m.Retentions) == 0 || old.Step <= 0 || old.Step != m.Retentions[0].Step ||
		from < 0 || until <= from || int64(until) > now || now-int64(from) > int64(m.Retentions[0].MaxRetention()) {
		return false
	}
	step := old.Step
	p.start = from - from%step + step
	p.end = until - until%step + step
	p.until = min(old.UntilTime, p.end)
	if p.start < old.FromTime || p.start >= p.until {
		return false
	}
	p.first = (p.start - old.FromTime) / step
	p.count = (p.until - p.start) / step
	if p.first < 0 || p.count <= 0 || p.first+p.count > len(old.Values) {
		p.count = 0
		return false
	}
	return true
}

func (p *fetchPrefix) join(m Metadata, tail *Series) *Series {
	if tail != nil && (tail.Step != p.series.Step || tail.FromTime != p.until || tail.UntilTime != p.end || len(tail.Values) != (p.end-p.until)/p.series.Step) {
		return nil
	}
	if tail == nil && p.until != p.end {
		return nil
	}
	result := &Series{Metadata: m, FromTime: p.start, UntilTime: p.end, Step: p.series.Step, Values: make([]float64, (p.end-p.start)/p.series.Step)}
	copy(result.Values, p.series.Values[p.first:p.first+p.count])
	if tail != nil {
		copy(result.Values[p.count:], tail.Values)
	}
	return result
}
