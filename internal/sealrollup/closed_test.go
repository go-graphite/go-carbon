package sealrollup

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

func projectClosed(c Config, state map[key]Sample, maxTime, through int64) map[key]Sample {
	out := make(map[key]Sample, len(state))
	for k, p := range state {
		out[k] = p
	}
	for a := 1; a < len(c.Archives); a++ {
		coarse, fine := c.Archives[a], c.Archives[a-1]
		start := align(maxTime, coarse.Step)
		if start+coarse.Step <= through {
			continue
		}
		var samples []Sample
		for ts := start; ts < start+coarse.Step; ts += fine.Step {
			if p, ok := out[key{a - 1, slot(ts, fine)}]; ok && p.Time == ts {
				samples = append(samples, p)
			}
		}
		if p, ok := AggregateWindow(c, a, start, samples); ok {
			out[key{a, slot(start, coarse)}] = p
		}
	}
	return out
}

// Canonical Plan remains the old eager oracle. Compare physical-slot time and
// value bits after every original application, including all open levels.
func TestClosedWindowsClassicDifferential(t *testing.T) {
	for method := Average; method <= First; method++ {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("method%d/xff%g", method, xff), func(t *testing.T) {
				c := config("1s:120,10s:1h,1m:1d,1h:2d", method, xff)
				p, _ := Prepare(c)
				eager, deferred := map[key]Sample{}, map[key]Sample{}
				var old, frontier Frontier
				var through int64
				bits := []uint64{1 << 63, 0, 0x7ff8000000000042, math.Float64bits(7), 0x7ff0000000000000, 0xfff0000000000000}
				apply := func(order uint64, now int64, points []Sample) {
					t.Helper()
					b := []Batch{{Order: order, Now: now, Points: points}}
					want, err := p.Plan(old, view(eager), nil, b)
					if err != nil {
						t.Fatal(err)
					}
					got, nextThrough, err := p.PlanClosedInto(nil, frontier, through, 0, view(deferred), nil, b, nil)
					if err != nil {
						t.Fatal(err)
					}
					for _, r := range want.Records {
						install(eager, []Delta{r.Delta})
					}
					for _, r := range got.Records {
						install(deferred, []Delta{r.Delta})
					}
					old, frontier, through = want.Next, got.Next, nextThrough
					actual := projectClosed(c, deferred, frontier.MaxTime, through)
					logical := func(s map[key]Sample) map[key][2]uint64 {
						out := map[key][2]uint64{}
						for k, p := range s {
							out[k] = [2]uint64{uint64(p.Time), p.Bits}
						}
						return out
					}
					if !reflect.DeepEqual(logical(actual), logical(eager)) {
						for k, w := range eager {
							if a, ok := actual[k]; !ok || a.Time != w.Time || a.Bits != w.Bits {
								t.Errorf("order%d slot%v got%+v want%+v", order, k, a, w)
							}
						}
						for k, a := range actual {
							if _, ok := eager[k]; !ok {
								t.Errorf("order%d extra%v %+v", order, k, a)
							}
						}
						t.FailNow()
					}
				}
				for i := int64(0); i < 180; i++ {
					apply(uint64(i+1), 3600+i, []Sample{{Time: 3600 + i, Bits: bits[i%6], Sequence: uint64(i + 1)}})
				}
				// Both a still-resident closed window and an older retention target.
				apply(181, 3780, []Sample{{Time: 3770, Bits: math.Float64bits(17), Sequence: 1}})
				apply(182, 3780, []Sample{{Time: 3610, Bits: 1 << 63, Sequence: 1}})
				idle, closed, err := p.PlanClosedInto(nil, frontier, through, 7200, view(deferred), nil, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if closed != 7200 {
					t.Fatal("idle watermark", closed)
				}
				for _, r := range idle.Records {
					install(deferred, []Delta{r.Delta})
				}
			})
		}
	}
}

func TestClosedWindowEmitsOnceAndLateRecomputes(t *testing.T) {
	c := config("1s:120,1m:1d", Average, 0)
	p, _ := Prepare(c)
	state := map[key]Sample{}
	var frontier Frontier
	var through int64
	coarse := 0
	for i := int64(0); i <= 60; i++ {
		plan, next, err := p.PlanClosedInto(nil, frontier, through, 0, view(state), nil, []Batch{{Order: uint64(i + 1), Now: 3600 + i, Points: []Sample{sample(3600+i, float64(i), 1)}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range plan.Records {
			if r.Archive == 1 {
				coarse++
			}
			install(state, []Delta{r.Delta})
		}
		frontier, through = plan.Next, next
	}
	if coarse != 1 {
		t.Fatalf("coarse writes=%d want1", coarse)
	}
	plan, _, err := p.PlanClosedInto(nil, frontier, through, 0, view(state), nil, []Batch{{Order: 62, Now: 3661, Points: []Sample{sample(3601, 101, 1)}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Records) != 2 || plan.Records[1].Archive != 1 {
		t.Fatal("late closed correction", plan.Records)
	}
}
