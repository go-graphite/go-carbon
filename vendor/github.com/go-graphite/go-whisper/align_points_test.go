package whisper

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
)

// alignPointsMap is the original map-based alignPoints, kept as the oracle
// for the ordered fast path.
func alignPointsMap(archive *archiveInfo, points []*TimeSeriesPoint) []dataPoint {
	alignedPoints := make([]dataPoint, 0, len(points))
	positions := make(map[int]int)
	for _, point := range points {
		dPoint := dataPoint{point.Time - mod(point.Time, archive.secondsPerPoint), point.Value}
		if p, ok := positions[dPoint.interval]; ok {
			alignedPoints[p] = dPoint
		} else {
			alignedPoints = append(alignedPoints, dPoint)
			positions[dPoint.interval] = len(alignedPoints) - 1
		}
	}
	return alignedPoints
}

func assertSameAligned(t *testing.T, name string, archive *archiveInfo, points []*TimeSeriesPoint) {
	t.Helper()
	want := alignPointsMap(archive, points)
	got := alignPoints(archive, points)
	if len(got) != len(want) {
		t.Fatalf("%s: len %d, want %d\npoints=%v\ngot=%v\nwant=%v", name, len(got), len(want), dumpPoints(points), got, want)
	}
	for i := range want {
		if got[i].interval != want[i].interval || math.Float64bits(got[i].value) != math.Float64bits(want[i].value) {
			t.Fatalf("%s: [%d] = %+v, want %+v\npoints=%v\ngot=%v\nwant=%v", name, i, got[i], want[i], dumpPoints(points), got, want)
		}
	}
}

func dumpPoints(points []*TimeSeriesPoint) []TimeSeriesPoint {
	out := make([]TimeSeriesPoint, len(points))
	for i, p := range points {
		out[i] = *p
	}
	return out
}

func TestAlignPointsMatchesMapAlgorithm(t *testing.T) {
	rng := rand.New(rand.NewSource(20261024))
	steps := []int{1, 7, 10, 60, 3600}
	type shape struct {
		name  string
		build func(ts []int) []int
	}
	shapes := []shape{
		{"sorted", func(ts []int) []int { sort.Ints(ts); return ts }},
		{"reversed", func(ts []int) []int { sort.Sort(sort.Reverse(sort.IntSlice(ts))); return ts }},
		{"shuffled", func(ts []int) []int { rng.Shuffle(len(ts), func(i, j int) { ts[i], ts[j] = ts[j], ts[i] }); return ts }},
		{"sorted-one-swap", func(ts []int) []int {
			sort.Ints(ts)
			if len(ts) > 1 {
				i := rng.Intn(len(ts) - 1)
				ts[i], ts[i+1] = ts[i+1], ts[i]
			}
			return ts
		}},
		{"sorted-late-revisit", func(ts []int) []int {
			sort.Ints(ts)
			if len(ts) > 1 {
				ts = append(ts, ts[rng.Intn(len(ts))])
			}
			return ts
		}},
	}

	cases := 0
	for _, step := range steps {
		archive := &archiveInfo{Retention: Retention{secondsPerPoint: step, numberOfPoints: 1024}}
		// Empty and single point.
		assertSameAligned(t, "empty", archive, nil)
		assertSameAligned(t, "single", archive, []*TimeSeriesPoint{{Time: 1700000003, Value: 1}})
		for _, sh := range shapes {
			for iter := 0; iter < 400; iter++ {
				n := 1 + rng.Intn(48)
				base := 1700000000 + rng.Intn(100000)
				if rng.Intn(10) == 0 {
					base = -base // exercise mod's floor on negative times
				}
				spread := n * step
				if rng.Intn(2) == 0 {
					spread = 1 + n*step/4 // force many same-interval points
				}
				ts := make([]int, n)
				for i := range ts {
					ts[i] = base + rng.Intn(spread+1)
				}
				// Exact timestamp duplicates.
				if n > 2 && rng.Intn(2) == 0 {
					for d := rng.Intn(n / 2); d >= 0; d-- {
						ts[rng.Intn(n)] = ts[rng.Intn(n)]
					}
				}
				ts = sh.build(ts)
				points := make([]*TimeSeriesPoint, len(ts))
				for i, tm := range ts {
					v := float64(rng.Intn(1000))
					if rng.Intn(50) == 0 {
						v = math.NaN()
					}
					points[i] = &TimeSeriesPoint{Time: tm, Value: v}
				}
				assertSameAligned(t, fmt.Sprintf("step=%d/%s/%d", step, sh.name, iter), archive, points)
				cases++
			}
		}
	}
	if cases == 0 {
		t.Fatal("no cases ran")
	}
}

var alignSink []dataPoint

func benchAlignInput(n, step int) []*TimeSeriesPoint {
	points := make([]*TimeSeriesPoint, n)
	for i := range points {
		points[i] = &TimeSeriesPoint{Time: 1700000000 + i*step, Value: float64(i)}
	}
	return points
}

func BenchmarkAlignPoints(b *testing.B) {
	archive := &archiveInfo{Retention: Retention{secondsPerPoint: 60, numberOfPoints: 1440}}
	for _, n := range []int{8, 16, 32} {
		points := benchAlignInput(n, 60)
		b.Run(fmt.Sprintf("map/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				alignSink = alignPointsMap(archive, points)
			}
		})
		b.Run(fmt.Sprintf("fast/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				alignSink = alignPoints(archive, points)
			}
		})
	}
}
