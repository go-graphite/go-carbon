package partsread

import (
	"context"
	"errors"
)

var ErrMetadataGeneration = errors.New("metadata belongs to another snapshot generation")

// ResolvedMetric borrows immutable catalog metadata until its snapshot closes.
// Its unexported generation prevents reuse with another publication version.
type ResolvedMetric struct {
	generation *version
	metric     Metric
	legacy     any
}

// LegacyCatalog explicitly supplies legacy presence in the same immutable entry.
// The opaque value is nil for v2-only names; consumers validate its binding
// type. Ordinary union catalogs cannot establish legacy presence.
type LegacyCatalog interface {
	Catalog
	LookupLegacy(string) (Metric, any, bool)
}

func (m ResolvedMetric) Metric() Metric      { return m.metric }
func (m ResolvedMetric) LegacyPresent() bool { return m.legacy != nil }
func (m ResolvedMetric) Legacy() any         { return m.legacy }
func (h *Handle) Resolve(name string) (ResolvedMetric, error) {
	if h.v == nil || h.closed.Load() {
		return ResolvedMetric{}, ErrClosed
	}
	var m Metric
	var ok bool
	var legacy any
	if cat, yes := h.v.view.Catalog.(LegacyCatalog); yes {
		m, legacy, ok = cat.LookupLegacy(name)
	} else {
		m, ok = h.v.view.Catalog.Lookup(name)
	}
	if !ok {
		return ResolvedMetric{}, ErrNotFound
	}
	return ResolvedMetric{generation: h.v, metric: m, legacy: legacy}, nil
}
func (h *Handle) RenderResolved(ctx context.Context, m ResolvedMetric, from, until, now int64) (*Series, error) {
	return h.renderResolved(ctx, m, from, until, now, nil)
}
func (h *Handle) RenderIntoResolved(ctx context.Context, m ResolvedMetric, from, until, now int64, base *Series) (*Series, error) {
	if base == nil {
		return nil, errors.New("nil render base")
	}
	return h.renderResolved(ctx, m, from, until, now, base)
}
