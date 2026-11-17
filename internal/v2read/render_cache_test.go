package v2read

import (
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestRenderedCacheDetachedExactAndBounded(t *testing.T) {
	c := &sealedCache{limit: 4096}
	source := newResolvedSource(1, []sealrollup.Archive{{Step: 1, Count: 4}}, nil)
	input := &Series{Values: []float64{math.Float64frombits(1 << 63), math.Float64frombits(0x7ff8000000000042)}, Occupied: []bool{true, true}, Metric: Metric{ID: 1, Archives: []Archive{{1, 4}}}}
	c.rememberRendered(source, 1, 3, 4, input)
	hit := c.renderedSeries(source, 1, 3, 4)
	if hit == nil {
		t.Fatal("exact request missed")
	}
	input.Values[0] = 9
	input.Occupied[0] = false
	input.Metric.Archives[0].Step = 2
	if math.Float64bits(hit.Values[0]) != 1<<63 || math.Float64bits(hit.Values[1]) != 0x7ff8000000000042 || !hit.Occupied[0] || hit.Metric.Archives[0].Step != 1 {
		t.Fatal("cache aliases render workspace")
	}
	if c.renderedSeries(source, 1, 3, 5) != nil || c.renderedSeries(source, 0, 3, 4) != nil || c.renderedSeries(newResolvedSource(1, source.archives, nil), 1, 3, 4) != nil {
		t.Fatal("different clock/range/source reused")
	}
	for i := int64(0); i < 100; i++ {
		c.rememberRendered(source, i, i+1, i+2, hit)
	}
	if c.renderedBytes > c.limit || len(c.rendered) > 64 {
		t.Fatal("unbounded render cache")
	}
	if math.Float64bits(hit.Values[0]) != 1<<63 {
		t.Fatal("eviction changed borrowed immutable result")
	}
}
