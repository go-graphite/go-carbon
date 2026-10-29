package carbonserver

import (
	"fmt"
	"math"
	"math/rand"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
)

// renderSampleShape describes one cached render response: a carbonapi request
// for series metrics over duration seconds at a fixed retention step.
type renderSampleShape struct {
	step     int
	duration int
	series   int
}

func (s renderSampleShape) String() string {
	return fmt.Sprintf("step=%ds/range=%s/series=%d", s.step, renderSampleDuration(s.duration), s.series)
}

func renderSampleDuration(seconds int) string {
	if seconds%86400 == 0 {
		return fmt.Sprintf("%dd", seconds/86400)
	}
	return fmt.Sprintf("%dh", seconds/3600)
}

// Value kinds seen in render responses. renderSampleMixed rotates through all
// of them, one kind per series.
const (
	renderSampleRepeated = iota // gauge that rarely changes
	renderSampleInteger         // per-interval counts
	renderSampleSparse          // mostly absent
	renderSampleNoisy           // full-precision floats such as averages
	renderSampleLate            // absent until the metric appeared
	renderSampleKinds

	renderSampleMixed = -1
)

var renderSampleKindNames = map[int]string{
	renderSampleRepeated: "repeated",
	renderSampleInteger:  "integer",
	renderSampleSparse:   "sparse",
	renderSampleNoisy:    "noisy",
	renderSampleLate:     "late",
	renderSampleMixed:    "mixed",
}

var renderSampleShapes = []renderSampleShape{
	{step: 10, duration: 3600, series: 1},
	{step: 10, duration: 3600, series: 10},
	{step: 10, duration: 3600, series: 100},
	{step: 10, duration: 3600, series: 500},
	{step: 10, duration: 6 * 3600, series: 100},
	{step: 10, duration: 86400, series: 10},
	{step: 10, duration: 86400, series: 100},
	{step: 60, duration: 86400, series: 100},
	{step: 60, duration: 86400, series: 500},
	{step: 60, duration: 7 * 86400, series: 1},
	{step: 60, duration: 7 * 86400, series: 100},
	{step: 60, duration: 7 * 86400, series: 500},
}

// renderSample builds the carbonapi_v2_pb response a store renders for shape,
// converting NaN to absent points exactly as the render path does.
func renderSample(shape renderSampleShape, kind int, seed int64) *protov2.MultiFetchResponse {
	rng := rand.New(rand.NewSource(seed))
	const until = 1761465600
	from := until - shape.duration
	points := shape.duration / shape.step
	var multi protov2.MultiFetchResponse
	for s := range shape.series {
		k := kind
		if k == renderSampleMixed {
			k = s % renderSampleKinds
		}
		r := response{
			Name:      fmt.Sprintf("aggregations.secondly.service-%03d.dc%d.host-%04d.http.requests.%s.count", s%37, s%3, rng.Intn(10000), renderSampleKindNames[k]),
			StartTime: int64(from),
			StopTime:  int64(until),
			StepTime:  int64(shape.step),
			Values:    renderSampleValues(rng, k, points),
		}
		multi.Metrics = append(multi.Metrics, r.proto2())
	}
	return &multi
}

func renderSampleValues(rng *rand.Rand, kind, points int) []float64 {
	values := make([]float64, points)
	switch kind {
	case renderSampleRepeated:
		level := float64(rng.Intn(5))
		for i := range values {
			if rng.Intn(100) == 0 {
				level = float64(rng.Intn(5))
			}
			values[i] = level
		}
	case renderSampleInteger:
		mean := 1 + rng.Intn(1000)
		for i := range values {
			values[i] = float64(mean + rng.Intn(mean/5+1))
		}
	case renderSampleSparse:
		for i := range values {
			if rng.Intn(100) < 85 {
				values[i] = math.NaN()
			} else {
				values[i] = float64(1 + rng.Intn(3))
			}
		}
	case renderSampleNoisy:
		base := rng.Float64() * 1000
		for i := range values {
			values[i] = base + rng.NormFloat64()*base*0.05
		}
	case renderSampleLate:
		start := rng.Intn(points)
		for i := range values {
			if i < start {
				values[i] = math.NaN()
			} else {
				values[i] = float64(rng.Intn(50))
			}
		}
	}
	return values
}
