package sealrollup

import (
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

func TestSparseSlotsWrap(t *testing.T) {
	c := Config{Archives: []Archive{{1, 120}, {60, 121}}, Method: Average}
	index := NewSlots(c.Archives)
	state := map[key]Sample{}
	rng := rand.New(rand.NewPCG(31, 71))
	for n := 0; n < 3000; n++ {
		a := n % 2
		ar := c.Archives[a]
		now := int64(1800000000) + int64(n)*ar.Step
		tm := align(now-int64(rng.IntN(200))*ar.Step, ar.Step)
		p := Sample{Time: tm, Bits: rng.Uint64(), Sequence: uint64(n + 1)}
		k := key{a, slot(tm, ar)}
		index.Install(Delta{Archive: a, Slot: k.slot, Sample: p})
		state[k] = p
		got, ok := index.View(a, k.slot)
		if !ok || got != p {
			t.Fatal("lookup replacement")
		}
		start := align(now-int64(rng.IntN(200))*ar.Step, ar.Step)
		end := start + int64(rng.IntN(120)+1)*ar.Step
		var want []Sample
		for k, p := range state {
			if k.archive == a && p.Time >= start && p.Time < end {
				want = append(want, p)
			}
		}
		slices.SortFunc(want, func(a, b Sample) int {
			if a.Time < b.Time {
				return -1
			}
			if a.Time > b.Time {
				return 1
			}
			return 0
		})
		actual := index.Range(a, start, end, nil)
		if !reflect.DeepEqual(actual, want) {
			t.Fatalf("range wrap n%d got%v want%v", n, actual, want)
		}
	}
}
func TestPreparedSparseDifferential(t *testing.T) {
	for method := Average; method <= First; method++ {
		for _, xff := range []float32{0, .5, 1} {
			c := Config{Archives: []Archive{{1, 120}, {60, 180}, {3600, 240}}, Method: method, XFilesFactor: xff}
			p, err := Prepare(c)
			if err != nil {
				t.Fatal(err)
			}
			index := NewSlots(c.Archives)
			state := map[key]Sample{}
			prior := Frontier{}
			var workspace Workspace
			rng := rand.New(rand.NewPCG(11, 23))
			for n := 1; n <= 400; n++ {
				now := int64(1800000000 + n*3)
				var ps []Sample
				for j := 0; j < 5; j++ {
					ps = append(ps, Sample{Time: now - int64(rng.IntN(9000)), Bits: rng.Uint64(), Sequence: uint64(j)})
				}
				batch := []Batch{{Order: uint64(n), Now: now, Points: ps}}
				want, err := Plan(c, prior, view(state), batch)
				if err != nil {
					t.Fatal(err)
				}
				got, err := p.PlanWithWorkspace(prior, index.View, index.Range, batch, &workspace)
				if err != nil || !reflect.DeepEqual(want, got) {
					t.Fatalf("method%d xff%g batch%d sparse mismatch: %v", method, xff, n, err)
				}
				for _, r := range got.Records {
					index.Install(r.Delta)
					state[key{r.Archive, r.Slot}] = r.Sample
				}
				prior = got.Next
			}
		}
	}
}

func TestWorkspacePositionReuse(t *testing.T) {
	var w Workspace
	for _, count := range []int64{4096, 1 << 30, 120, 1 << 29, 4096} {
		c := Config{Archives: []Archive{{1, count}}, Method: Last}
		w.preparePositions(c, 1000)
		for round := range 3 {
			w.updates = w.updates[:0]
			w.preparePositions(c, 1000)
			for i := range 1000 {
				k := key{0, int64(i) % count}
				d := Delta{Archive: 0, Slot: k.slot, Order: uint64(round + 1), Sample: Sample{Bits: uint64(i)}}
				w.replace(k, d)
				got, ok := w.lookup(k)
				if !ok || got != d {
					t.Fatalf("count%d round%d slot%d: got%v", count, round, i, got)
				}
			}
		}
		if count > 1<<18 && cap(w.positions) > 8192 {
			t.Fatal("sparse retention allocated by slot count", cap(w.positions))
		}
		if n := testing.AllocsPerRun(100, func() {
			w.updates = w.updates[:0]
			w.preparePositions(c, 1000)
			for i := range 1000 {
				k := key{0, int64(i) % count}
				w.replace(k, Delta{Archive: 0, Slot: k.slot})
				w.lookup(k)
			}
		}); n != 0 {
			t.Fatalf("hot positions allocate: %g", n)
		}
	}
}
