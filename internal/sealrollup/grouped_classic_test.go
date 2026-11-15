package sealrollup

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

// A classic cache write and a same-age v2 head receive exactly the same grouped
// input, clock and admission order. Check physical archive timestamp/value bytes
// after each seal, before and after the fine ring wraps. No batch coalescing.
func TestGroupedHeadClassicWrapDifferential(t *testing.T) {
	for _, density := range []int{118, 513} {
		for _, wrap := range []bool{false, true} {
			for method := Average; method <= First; method++ {
				for _, xff := range []float32{0, .5, 1} {
					t.Run(fmt.Sprintf("density%d/wrap%t/method%d/xff%g", density, wrap, method, xff), func(t *testing.T) {
						c := config("1s:600,1m:2h,1h:2d", method, xff)
						clock := int64(1794700800)
						path := filepath.Join(t.TempDir(), "classic.wsp")
						w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs("1s:600,1m:2h,1h:2d"), whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
						if err != nil {
							t.Fatal(err)
						}
						defer w.Close()
						state := map[key]Sample{}
						candidates := map[key]bool{}
						prior := Frontier{}
						for seal := uint64(1); seal <= 3; seal++ {
							if seal > 1 {
								if wrap {
									clock += 600
								} else {
									clock += 20
								}
							}
							batch := Batch{Order: seal, Now: clock}
							start := clock - int64(density)
							for j := range density {
								bits := math.Float64bits(float64(j%17) - 8)
								if j == 3 {
									bits = 1 << 63
								}
								if j == 5 {
									bits = 0x7ff80000000000ab
								}
								batch.Points = append(batch.Points, Sample{Time: start + int64(j), Bits: bits, Sequence: uint64(j + 1)})
							}
							p, err := planBoth(t, c, prior, state, []Batch{batch})
							if err != nil {
								t.Fatal(err)
							}
							for _, r := range p.Records {
								decoded, err := DecodeRecord(r.Archive, r.Slot, EncodeRecord(r))
								if err != nil {
									t.Fatal(err)
								}
								install(state, []Delta{decoded.Delta})
							}
							prior = p.Next
							points := make([]*whisper.TimeSeriesPoint, len(batch.Points))
							for i, p := range batch.Points {
								points[i] = &whisper.TimeSeriesPoint{Time: int(p.Time), Value: math.Float64frombits(p.Bits)}
								for ai, a := range c.Archives {
									candidates[key{ai, slot(align(p.Time, a.Step), a)}] = true
								}
							}
							if err = w.UpdateMany(points); err != nil {
								t.Fatal(err)
							}
							checkClassic(t, path, c, state, candidates, int(seal))
						}
					})
				}
			}
		}
	}
}
