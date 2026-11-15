//go:build partsread_integration

package sealrollup

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsread"
	whisper "github.com/go-graphite/go-whisper"
)

// Run in a combined stack containing the immutable reader. Every persisted seal
// record (including corrections and ring overwrites) enters a separate frozen
// source. The reader must resolve all versions before filtering timestamps.
func TestSealedRecordsRenderClassic(t *testing.T) {
	queries := 0
	for _, def := range schemas {
		for method := Average; method <= First; method++ {
			for _, xff := range []float32{0, .5, 1} {
				c := config(def, method, xff)
				clock := int64(1794700800)
				step := c.Archives[0].Step
				path := filepath.Join(t.TempDir(), "classic.wsp")
				w, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(def), whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
				if err != nil {
					t.Fatal(err)
				}
				state := map[key]Sample{}
				frontier := Frontier{}
				var sources []partsread.Source
				original := clock
				for order := uint64(1); order <= 4; order++ {
					if order >= 3 {
						clock = original + step*c.Archives[0].Count
					}
					points := []Sample{sample(align(clock-20*step, step), 2, 1), sample(align(clock-19*step, step), 3, 2), sample(align(clock-19*step, step), 7, 3), {Time: align(clock-18*step, step), Bits: 0x7ff80000000000ab, Sequence: 4}, sample(align(clock-17*step, step), math.Copysign(0, -1), 5)}
					// Direct old writes populate independently authoritative retained archives.
					for ai := 1; ai < len(c.Archives); ai++ {
						a := c.Archives[ai]
						prev := c.Archives[ai-1]
						ts := align(clock-prev.Step*prev.Count-3*a.Step, a.Step)
						points = append(points, sample(ts, float64(order*11), uint64(len(points)+1)))
					}
					if order == 4 {
						points = append(points, sample(align(original-20*step, step), 127, 999))
					}
					plan, e := Plan(c, frontier, view(state), []Batch{{Order: order, Now: clock, Points: points}})
					if e != nil {
						t.Fatal(e)
					}
					records := make([]partsread.Record, 0, len(plan.Records))
					for _, r := range plan.Records {
						decoded, e := DecodeRecord(r.Archive, r.Slot, EncodeRecord(r))
						if e != nil {
							t.Fatal(e)
						}
						install(state, []Delta{decoded.Delta})
						records = append(records, partsread.Record{ID: 7, Step: c.Archives[r.Archive].Step, Sample: partsread.Sample{Time: decoded.Sample.Time, Bits: decoded.Sample.Bits, Sequence: decoded.Order}})
					}
					frontier = plan.Next
					sources = append(sources, partsread.Freeze(records))
					ptrs := make([]*whisper.TimeSeriesPoint, len(points))
					for i, p := range points {
						ptrs[i] = &whisper.TimeSeriesPoint{Time: int(p.Time), Value: math.Float64frombits(p.Bits)}
					}
					if e = w.UpdateMany(ptrs); e != nil {
						t.Fatal(e)
					}
				}
				var archives []partsread.Archive
				for _, a := range c.Archives {
					archives = append(archives, partsread.Archive{Step: a.Step, Points: a.Count})
				}
				var pub partsread.Publication
				cat := partsread.CatalogFuncs{LookupFunc: func(name string) (partsread.Metric, bool) {
					return partsread.Metric{ID: 7, Archives: archives}, name == "metric"
				}}
				pub.Publish(partsread.View{Catalog: cat, Blocks: sources[:1], Parts: sources[1:3], Head: sources[3:]})
				for ai, a := range c.Archives {
					from := clock - 30*a.Step
					if ai > 0 {
						p := c.Archives[ai-1]
						from = clock - p.Step*p.Count - 5*a.Step
					}
					until := from + 4*a.Step
					h, e := pub.Pin(partsread.NewBudget(4<<20), 2<<20)
					if e != nil {
						t.Fatal(e)
					}
					got, e := h.Render(context.Background(), "metric", from, until, clock)
					if e != nil {
						t.Fatal(e)
					}
					want, e := w.Fetch(int(from), int(until))
					if e != nil {
						t.Fatal(e)
					}
					where := fmt.Sprintf("%s method%d xff%g archive%d", def, method, xff, ai)
					if (got == nil) != (want == nil) {
						t.Fatal(where, "nil mismatch")
					}
					if got != nil {
						values := want.Values()
						if got.From != int64(want.FromTime()) || got.Until != int64(want.UntilTime()) || got.Step != int64(want.Step()) || len(got.Values) != len(values) {
							t.Fatal(where, "grid mismatch")
						}
						for i, v := range values {
							if math.Float64bits(v) != math.Float64bits(got.Values[i]) {
								t.Fatalf("%s slot%d got%x want%x", where, i, math.Float64bits(got.Values[i]), math.Float64bits(v))
							}
						}
					}
					h.Close()
					queries++
				}
				pub.Close()
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	t.Logf("sealed record render classic queries=%d", queries)
}
