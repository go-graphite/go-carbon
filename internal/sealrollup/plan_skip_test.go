package sealrollup

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// The full reducer remains the oracle; this wrapper fixes the unskipped
// closure and retention behavior independently of the fast-path predicate.
func (p *Prepared) planClosedFull(records []Record, prior Frontier, through, closeThrough int64, view View, ranges RangeView, batches []Batch, w *Workspace) (SealPlan, int64, error) {
	if prior.Policy != ([32]byte{}) && prior.Policy != p.policy || prior.Order != 0 && prior.Policy == ([32]byte{}) {
		return SealPlan{}, through, fmt.Errorf("stored rollup policy changed")
	}
	next := prior
	next.Policy = p.policy
	for _, b := range batches {
		if b.Order <= next.Order {
			return SealPlan{}, through, fmt.Errorf("rollup batch is not after frontier")
		}
		next.Order = b.Order
		for _, sample := range b.Points {
			next.MaxTime = max(next.MaxTime, sample.Time)
		}
	}
	closing := windowClosure{through: max(through, prior.MaxTime)}
	if len(batches) == 0 && prior.Order != 0 && closeThrough > closing.through {
		closing.advance = closeThrough
		batches = []Batch{{Order: prior.Order, Now: closeThrough}}
	}
	winners, versions, err := reduceMode(p.config, view, ranges, batches, w, &closing, true)
	if err != nil {
		return SealPlan{}, through, err
	}
	plan := SealPlan{Next: next, Records: records[:0], Versions: len(versions)}
	for _, d := range append(winners, versions...) {
		d.Correction = d.Sample.Time <= prior.MaxTime
		plan.Records = append(plan.Records, d)
	}
	return plan, closing.through, nil
}

func TestClosedPlanSkipDifferential(t *testing.T) {
	for method := Average; method <= First; method++ {
		for _, xff := range []float32{0, .5, 1} {
			t.Run(fmt.Sprintf("method%d/xff%g", method, xff), func(t *testing.T) {
				p, err := Prepare(config("1s:120,10s:1h,1m:1d,1h:2d", method, xff))
				if err != nil {
					t.Fatal(err)
				}
				left, right := map[key]Sample{}, map[key]Sample{}
				var prior Frontier
				var through int64
				var fast, full Workspace
				rng := rand.New(rand.NewSource(91))
				clock := int64(360001)
				for round := 1; round <= 300; round++ {
					clock += int64(rng.Intn(4))
					batches := []Batch{{Order: uint64(round * 2), Now: clock + 1, Points: []Sample{sample(clock, float64(round), 1)}}}
					closeThrough := int64(0)
					switch round % 12 {
					case 0: // idle close, then resume at an earlier sample clock
						batches, closeThrough = nil, clock+3
					case 1: // fine-retention overflow despite a sample in the open window
						batches[0].Now += 10000
					case 2: // correction in a closed window
						batches[0].Points[0].Time -= 20
					case 3: // physical wrap and multiple original batches
						batches = append(batches, Batch{Order: uint64(round*2 + 1), Now: clock + 1001, Points: []Sample{sample(clock+1000, 0, 1)}})
						clock += 1000
					case 4: // stable ties and NaN payloads
						batches[0].Points = []Sample{{Time: clock, Bits: 1 << 63, Sequence: 2}, {Time: clock, Bits: 0x7ff8000000000042, Sequence: 3}}
					case 5: // empty ordered batch
						batches[0].Points = nil
					}
					got, gt, ge := p.PlanClosedInto(nil, prior, through, closeThrough, view(left), nil, batches, &fast)
					want, wt, we := p.planClosedFull(nil, prior, through, closeThrough, view(right), nil, batches, &full)
					if (ge == nil) != (we == nil) || !reflect.DeepEqual(got, want) || gt != wt {
						t.Fatalf("round %d: got %+v through=%d err=%v; want %+v through=%d err=%v", round, got, gt, ge, want, wt, we)
					}
					if ge != nil {
						continue
					}
					for _, r := range got.Records {
						install(left, []Delta{r.Delta})
					}
					for _, r := range want.Records {
						install(right, []Delta{r.Delta})
					}
					prior, through = got.Next, gt
				}
			})
		}
	}
}

func TestKeepRecentSkipBoundary(t *testing.T) {
	p, err := Prepare(config("1s:120,10s:1h,1m:1d", Average, 0))
	if err != nil {
		t.Fatal(err)
	}
	s := p.NewSlots()
	// Populate through Install before the first trim, as restored slots can be.
	for _, a := range []int{0, 1, 2} {
		s.Install(Delta{Archive: a, Slot: 0, Sample: sample(0, 1, 1)})
	}
	floors := make([]int64, len(p.config.Archives)-1)
	for i := range floors {
		floors[i] = -10000
	}
	for _, now := range []int64{1, 2, 9, 10, 11, 59, 60, 61, 58, 6001} {
		s.KeepRecent(now)
		for a := range floors {
			want := align(now, p.config.Archives[a+1].Step) - 2*p.config.Archives[a].Step
			if want > floors[a] {
				floors[a] = want
			}
			// The full implementation starts floors at zero, retaining completeness
			// for the initial epoch even when its calculated floor is negative.
			if s.data[a].floor != max(int64(0), floors[a]) {
				t.Fatalf("now=%d archive=%d floor=%d want=%d", now, a, s.data[a].floor, max(int64(0), floors[a]))
			}
		}
		if len(s.data[len(s.data)-1].data) != 0 {
			t.Fatal("terminal archive retained")
		}
	}
}

// Even an open-window sample can route directly to an older archive when its
// admission clock is much later than its sample clock.
func TestClosedPlanSkipRetention(t *testing.T) {
	p, err := Prepare(config("1s:120,10s:1h,1m:1d", Average, 0))
	if err != nil {
		t.Fatal(err)
	}
	prior := Frontier{Order: 1, MaxTime: 360001, Policy: p.Digest()}
	batches := []Batch{{Order: 2, Now: 370002, Points: []Sample{sample(360002, 7, 1)}}}
	got, through, err := p.PlanClosedInto(nil, prior, 360001, 0, nil, nil, batches, nil)
	if err != nil {
		t.Fatal(err)
	}
	if through != 360060 || len(got.Records) != 1 || got.Records[0].Archive != 2 || got.Records[0].Sample.Time != 360000 {
		t.Fatalf("retention target lost: %+v through=%d", got, through)
	}
}
