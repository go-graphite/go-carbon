package v2read

import (
	"context"
	"slices"

	"github.com/go-graphite/go-carbon/internal/v2core"
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
	pins, err := r.loadOwners(ctx)
	if err != nil {
		return Metric{}, masked, err
	}
	defer releaseOwners(pins)
	views, errs := r.viewsForNames(ctx, pins, []string{name})
	for i, g := range views {
		if pins[i] == nil {
			continue
		}
		if errs[i] != nil {
			return Metric{}, masked, errs[i]
		}
		if g == nil {
			continue
		}
		m := g.metrics[name]
		if m == nil {
			m = g.sealed.lookup(name)
		}
		if m != nil {
			masked = masked || m.meta.BaselineOrder > 0 || m.meta.Expired || m.meta.ResetOrder > 0
		}
		if m != nil && (includeExpired || !m.meta.Expired) {
			if found {
				return Metric{}, masked, ErrNotFound
			}
			found = true
			result = Metric{BaselineOrder: m.meta.BaselineOrder, ID: m.meta.ID, XFilesFactor: m.policy.Config.XFilesFactor, AggregationMethod: aggregationName(m.policy.Config.Method)}
			for _, a := range m.policy.Config.Archives {
				result.Archives = append(result.Archives, Archive{Step: a.Step, Points: a.Count})
			}
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
	pins, err := r.loadOwners(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer releaseOwners(pins)
	masks := make(map[string]bool)
	var names []string
	for _, g := range pins {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if g == nil {
			continue
		}
		g, err = g.discover(ctx)
		if err != nil {
			return nil, nil, err
		}
		err = g.sealed.index.MetadataContext(ctx, func(n string, _ v2core.RollupPolicy, meta v2core.RollupMetadata) {
			if pending := g.metrics[n]; pending != nil {
				meta = pending.meta
			}
			if meta.BaselineOrder > 0 || meta.Expired || meta.ResetOrder > 0 {
				masks[n] = true
			}
			if !meta.Expired {
				names = append(names, n)
			}
		})
		if err != nil {
			return nil, nil, err
		}
		for n, m := range g.metrics {
			if m.meta.BaselineOrder > 0 || m.meta.Expired || m.meta.ResetOrder > 0 {
				masks[n] = true
			}
			if !m.meta.Expired {
				names = append(names, n)
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names), masks, nil
}

// loadOwners pins publications without starting any directory work.
func (r *Reader) loadOwners(ctx context.Context) ([]*generation, error) {
	// Requests own these pins until their synchronous work or callback ends.
	pins := make([]*generation, len(r.owners))
	ok := false
	defer func() {
		if !ok {
			releaseOwners(pins)
		}
	}()
	for i := range r.owners {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			g := r.owners[i].Load()
			if g == nil {
				break
			}
			if g.retain() {
				pins[i] = g
				break
			}
		}
	}
	ok = true
	return pins, nil
}

func releaseOwners(pins []*generation) {
	for _, g := range pins {
		if g != nil {
			g.release()
		}
	}
}

// Masked reports durable whole-name reset authority from the same pinned
// publication as the data. A reset keeps masking classic history after resume.
func (r *Reader) Masked(ctx context.Context, name string) (bool, error) {
	_, masked, err := r.metadataWithMask(ctx, name, true)
	if err == ErrNotFound {
		err = nil
	}
	return masked, err
}
