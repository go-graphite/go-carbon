package v2read

import (
	"context"
	"slices"
)

// Metadata pins exactly the same publication used by render. It does not need
// a renderable time range and works for stored policies with no current points.
func (r *Reader) Metadata(ctx context.Context, name string) (Metric, error) {
	var result Metric
	found := false
	for i := range r.owners {
		if err := ctx.Err(); err != nil {
			return Metric{}, err
		}
		for {
			g := r.owners[i].Load()
			if g == nil {
				break
			}
			if !g.retain() {
				continue
			}
			g.once.Do(g.load)
			if g.err != nil {
				g.release()
				return Metric{}, g.err
			}
			m := g.metrics[name]
			if m == nil {
				m = g.sealed.metrics[name]
			}
			if m != nil {
				if found {
					g.release()
					return Metric{}, ErrNotFound
				}
				found = true
				result = Metric{ID: m.meta.ID, XFilesFactor: m.policy.Config.XFilesFactor, AggregationMethod: aggregationName(m.policy.Config.Method)}
				for _, a := range m.policy.Config.Archives {
					result.Archives = append(result.Archives, Archive{Step: a.Step, Points: a.Count})
				}
			}
			g.release()
			break
		}
	}
	if !found {
		return Metric{}, ErrNotFound
	}
	result.Archives = slices.Clone(result.Archives)
	return result, nil
}

// Names projects the pinned reader publications, including admitted pending
// applications; producer-local mutable catalogs are never inspected.
func (r *Reader) Names(ctx context.Context) ([]string, error) {
	var names []string
	for i := range r.owners {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			g := r.owners[i].Load()
			if g == nil {
				break
			}
			if !g.retain() {
				continue
			}
			g.once.Do(g.load)
			if g.err != nil {
				g.release()
				return nil, g.err
			}
			for n := range g.sealed.metrics {
				names = append(names, n)
			}
			for n := range g.metrics {
				names = append(names, n)
			}
			g.release()
			break
		}
	}
	slices.Sort(names)
	return slices.Compact(names), nil
}
