package sealrollup

import (
	"fmt"
	"math/rand"
	"testing"
)

// A newer seal must never hold an older version of a slot than an earlier
// seal: across parts, part sequence then decides like the per-record order.
// A retention-routed write into the open coarse window used to lose that: a
// later seal closed the window with order max(neighbour orders), below the
// direct write's batch order (order-proof case A).
func TestCloseOnDirectWriteOrdersFollowSeals(t *testing.T) {
	type stamp struct {
		key
		time int64
	}
	for method := Average; method <= First; method++ {
		for _, xff := range []float32{0, .5} {
			t.Run(fmt.Sprintf("method%d/xff%g", method, xff), func(t *testing.T) {
				p, err := Prepare(config("1s:120,10s:1h,1m:1d,1h:2d", method, xff))
				if err != nil {
					t.Fatal(err)
				}
				state, newest := map[key]Sample{}, map[stamp]uint64{}
				var prior Frontier
				var through int64
				var w Workspace
				rng := rand.New(rand.NewSource(17))
				clock := int64(360001)
				for round := 1; round <= 600; round++ {
					clock += int64(rng.Intn(4))
					b := Batch{Order: uint64(round * 2), Now: clock + 1, Points: []Sample{sample(clock, float64(round), 1)}}
					closeThrough := int64(0)
					batches := []Batch{b}
					switch rng.Intn(6) {
					case 0: // idle close
						batches, closeThrough = nil, clock+3
					case 1: // retention-routed write into an open coarse window
						batches[0].Now += 200 + int64(rng.Intn(4))*4000
					case 2: // correction in a closed window
						batches[0].Points[0].Time -= 20
					}
					plan, next, err := p.PlanClosedInto(nil, prior, through, closeThrough, view(state), nil, batches, &w)
					if err != nil {
						t.Fatal(err)
					}
					for _, r := range plan.Records[:len(plan.Records)-plan.Versions] {
						s := stamp{key{r.Archive, r.Slot}, r.Sample.Time}
						if old, ok := newest[s]; ok && r.Order < old {
							t.Fatalf("round %d: seal writes archive %d t=%d order %d below an earlier seal's %d", round, r.Archive, r.Sample.Time, r.Order, old)
						}
						newest[s] = r.Order
						install(state, []Delta{r.Delta})
					}
					prior, through = plan.Next, next
				}
			})
		}
	}
}

// The direct write closes its open window in its own seal, replaces the
// closure there, and the next seal does not close the window again.
func TestCloseOnDirectWriteClosesOpenWindow(t *testing.T) {
	p, err := Prepare(config("1s:120,10s:1h,1m:1d", Average, 0))
	if err != nil {
		t.Fatal(err)
	}
	state := map[key]Sample{}
	var points []Sample
	for ts := int64(360001); ts <= 360030; ts++ {
		points = append(points, sample(ts, 1, uint64(ts)))
	}
	seal := func(prior Frontier, through int64, b Batch) (SealPlan, int64) {
		plan, next, err := p.PlanClosedInto(nil, prior, through, 0, view(state), nil, []Batch{b}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range plan.Records[:len(plan.Records)-plan.Versions] {
			install(state, []Delta{r.Delta})
		}
		return plan, next
	}
	first, through := seal(Frontier{}, 0, Batch{Order: 2, Now: 360031, Points: points})
	// 360035 is past the 120 s fine retention at Now 360200: it lands in the
	// open 10 s window [360030, 360040) directly.
	direct, through := seal(first.Next, through, Batch{Order: 4, Now: 360200, Points: []Sample{sample(360035, 9, 1)}})
	if through != 360040 {
		t.Fatalf("direct write left its window open: through=%d", through)
	}
	slot := slot(360030, p.config.Archives[1])
	var winner bool
	for _, r := range direct.Records[:len(direct.Records)-direct.Versions] {
		winner = winner || r.Archive == 1 && r.Slot == slot && r.Order == 4 && r.Kind == KindRaw
	}
	if !winner {
		t.Fatalf("direct write is not its seal's winner: %+v", direct.Records)
	}
	later, _ := seal(direct.Next, through, Batch{Order: 6, Now: 360201, Points: []Sample{sample(360200, 1, 1)}})
	for _, r := range later.Records {
		if r.Archive == 1 && r.Slot == slot {
			t.Fatalf("later seal closed the window again: %+v", r)
		}
	}
}
