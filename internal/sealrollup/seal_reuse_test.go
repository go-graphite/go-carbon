package sealrollup

import (
	"math"
	"reflect"
	"testing"
)

func TestPlanIntoExactAndReuse(t *testing.T) {
	for _, method := range []Method{Average, Sum, Last, Min, Max} {
		c := Config{Archives: []Archive{{Step: 1, Count: 120}, {Step: 10, Count: 24}}, Method: method, XFilesFactor: 0}
		p, err := Prepare(c)
		if err != nil {
			t.Fatal(err)
		}
		batches := []Batch{{Order: 1, Now: 1000, Points: []Sample{{Time: 999, Bits: math.Float64bits(0), Sequence: 1}, {Time: 998, Bits: 1 << 63, Sequence: 2}, {Time: 999, Bits: 0x7ff8000000000001, Sequence: 3}}}}
		want, err := p.Plan(Frontier{}, nil, nil, batches)
		if err != nil {
			t.Fatal(err)
		}
		records := make([]Record, 0, 128)
		var w Workspace
		run := func() {
			got, e := p.PlanInto(records, Frontier{}, nil, nil, batches, &w)
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("borrowed plan differs")
			}
			records = got.Records
		}
		run()
		// The comparison above allocates reflect state; measure just the real API.
		n := testing.AllocsPerRun(20, func() {
			got, e := p.PlanInto(records, Frontier{}, nil, nil, batches, &w)
			if e != nil {
				t.Fatal(e)
			}
			records = got.Records
		})
		if n != 0 {
			t.Fatalf("PlanInto allocated %.0f times", n)
		}
	}
}
