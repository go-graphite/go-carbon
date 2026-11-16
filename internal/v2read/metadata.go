package v2read

import (
	"context"
	"slices"
)

// Metadata pins exactly the same publication used by render. It does not need
// a renderable time range and works for stored policies with no current points.
func (r *Reader) Metadata(ctx context.Context, name string) (Metric, error) {
	metric, _, err := r.MetadataWithMask(ctx, name)
	return metric, err
}

// MetadataWithMask reads discovery policy and reset authority from the same pins.
func (r *Reader) MetadataWithMask(ctx context.Context, name string) (Metric, bool, error) {
	return r.metadataWithMask(ctx, name, false)
}

// BaselineMetadata also returns expired policy so readmission keeps the installed
// baseline authority while the reset barrier prevents historical resurrection.
func (r *Reader) BaselineMetadata(ctx context.Context, name string) (Metric, error) {
	m, _, err := r.metadataWithMask(ctx, name, true)
	if err == nil && m.BaselineOrder == 0 {
		return Metric{}, ErrNotFound
	}
	return m, err
}

func (r *Reader) metadataWithMask(ctx context.Context, name string, includeExpired bool) (Metric, bool, error) {
	masked := false
	var result Metric
	found := false
	for i := range r.owners {
		if err := ctx.Err(); err != nil {
			return Metric{}, masked, err
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
				return Metric{}, masked, g.err
			}
			m := g.metrics[name]
			if m == nil {
				m = g.sealed.metrics[name]
			}
			if m != nil {
				masked = masked || m.meta.BaselineOrder > 0 || m.meta.Expired || m.meta.ResetOrder > 0
			}
			if m != nil && (includeExpired || !m.meta.Expired) {
				if found {
					g.release()
					return Metric{}, masked, ErrNotFound
				}
				found = true
				result = Metric{BaselineOrder: m.meta.BaselineOrder, ID: m.meta.ID, XFilesFactor: m.policy.Config.XFilesFactor, AggregationMethod: aggregationName(m.policy.Config.Method)}
				for _, a := range m.policy.Config.Archives {
					result.Archives = append(result.Archives, Archive{Step: a.Step, Points: a.Count})
				}
			}
			g.release()
			break
		}
	}
	if !found {
		return Metric{}, masked, ErrNotFound
	}
	result.Archives = slices.Clone(result.Archives)
	return result, masked, nil
}

// Names projects the pinned reader publications, including admitted pending
// applications; producer-local mutable catalogs are never inspected.
func (r *Reader) Names(ctx context.Context) ([]string, error) {
	names, _, err := r.NamesWithMasks(ctx)
	return names, err
}

// NamesWithMasks projects visible names and classic reset barriers together.
// Each owner is pinned once; masks never sample a later publication than names.
func (r *Reader) NamesWithMasks(ctx context.Context) ([]string, map[string]bool, error) {
	masks := make(map[string]bool)
	var names []string
	for i := range r.owners {
		for {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
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
				return nil, nil, g.err
			}
			for n, m := range g.sealed.metrics {
				if p := g.metrics[n]; p != nil {
					m = p
				}
				if m.meta.BaselineOrder > 0 || m.meta.Expired || m.meta.ResetOrder > 0 {
					masks[n] = true
				}
				if !m.meta.Expired {
					names = append(names, n)
				}
			}
			for n, m := range g.metrics {
				if m.meta.BaselineOrder > 0 || m.meta.Expired || m.meta.ResetOrder > 0 {
					masks[n] = true
				}
				if !m.meta.Expired {
					names = append(names, n)
				}
			}
			g.release()
			break
		}
	}
	slices.Sort(names)
	return slices.Compact(names), masks, nil
}

// Masked reports durable whole-name reset authority from the same pinned
// publication as the data. A reset keeps masking classic history after resume.
func (r *Reader) Masked(ctx context.Context, name string) (bool, error) {
	for i := range r.owners {
		for {
			if err := ctx.Err(); err != nil {
				return false, err
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
				err := g.err
				g.release()
				return false, err
			}
			m := g.metrics[name]
			if m == nil {
				m = g.sealed.metrics[name]
			}
			masked := m != nil && (m.meta.BaselineOrder > 0 || m.meta.Expired || m.meta.ResetOrder > 0)
			g.release()
			if masked {
				return true, nil
			}
			break
		}
	}
	return false, nil
}
