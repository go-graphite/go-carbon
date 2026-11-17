package v2read

import "slices"

// Entries own detached values and an immutable scalar source, never a metric
// state, generation, Pebble pin, Handle or pooled render workspace.
type renderedEntry struct {
	source           *resolvedSource
	from, until, now int64
	series           *Series
	bytes            int64
}

func (c *sealedCache) renderedSeries(source *resolvedSource, from, until, now int64) *Series {
	c.renderMu.Lock()
	defer c.renderMu.Unlock()
	for _, e := range c.rendered {
		if e.source == source && e.from == from && e.until == until && e.now == now {
			return e.series
		}
	}
	return nil
}
func (c *sealedCache) rememberRendered(source *resolvedSource, from, until, now int64, series *Series) {
	size := source.RetainedBytes() + int64(len(series.Values))*9 + int64(len(series.Metric.Archives))*16 + 192
	if size > c.limit {
		return
	}
	c.renderMu.Lock()
	defer c.renderMu.Unlock()
	for _, e := range c.rendered {
		if e.source == source && e.from == from && e.until == until && e.now == now {
			return
		}
	}
	result := *series
	result.Values = slices.Clone(series.Values)
	result.Occupied = slices.Clone(series.Occupied)
	result.Metric.Archives = slices.Clone(series.Metric.Archives)
	for len(c.rendered) > 0 && (len(c.rendered) >= 64 || c.renderedBytes+size > c.limit) {
		c.renderedBytes -= c.rendered[0].bytes
		copy(c.rendered, c.rendered[1:])
		c.rendered[len(c.rendered)-1] = renderedEntry{}
		c.rendered = c.rendered[:len(c.rendered)-1]
	}
	c.rendered = append(c.rendered, renderedEntry{source, from, until, now, &result, size})
	c.renderedBytes += size
}
