package sealrollup

import "testing"

// A live name usually has one new fine sample, before its next coarse close.
func BenchmarkClosedPlanNotDue(b *testing.B) {
	p, err := Prepare(config("1s:1d,1m:30d,1h:1y,1d:10y", Average, .5))
	if err != nil {
		b.Fatal(err)
	}
	prior := Frontier{Order: 10, MaxTime: 3601, Policy: p.Digest()}
	batches := []Batch{{Order: 11, Now: 3603, Points: []Sample{sample(3602, 17, 1)}}}
	var w Workspace
	var records []Record
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, _, err := p.PlanClosedInto(records[:0], prior, 3601, 0, nil, nil, batches, &w)
		if err != nil {
			b.Fatal(err)
		}
		records = plan.Records
	}
}

func BenchmarkKeepRecentNotDue(b *testing.B) {
	p, err := Prepare(config("1s:1d,1m:30d,1h:1y,1d:10y", Average, .5))
	if err != nil {
		b.Fatal(err)
	}
	s := p.NewSlots()
	s.KeepRecent(3601)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.KeepRecent(3602)
	}
}

// Compare both routes in the same process to limit shared-machine noise.
func BenchmarkClosedPlanCompare(b *testing.B) {
	for _, shape := range []struct {
		name, ladder string
		stamp, now   int64
	}{
		{"single", "1s:1d", 3602, 3603},
		{"open", "1s:1d,1m:30d,1h:1y,1d:10y", 3602, 3603},
		{"close", "1s:1d,1m:30d,1h:1y,1d:10y", 3660, 3661},
		{"late", "1s:1d,1m:30d,1h:1y,1d:10y", 3599, 3603},
	} {
		p, err := Prepare(config(shape.ladder, Average, .5))
		if err != nil {
			b.Fatal(err)
		}
		prior := Frontier{Order: 10, MaxTime: 3601, Policy: p.Digest()}
		batches := []Batch{{Order: 11, Now: shape.now, Points: []Sample{sample(shape.stamp, 17, 1)}}}
		for _, fast := range []bool{false, true} {
			name, plan := "full", p.planClosedFull
			if fast {
				name, plan = "skip", p.PlanClosedInto
			}
			b.Run(shape.name+"/"+name, func(b *testing.B) {
				var w Workspace
				var records []Record
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					got, _, err := plan(records[:0], prior, 3601, 0, nil, nil, batches, &w)
					if err != nil {
						b.Fatal(err)
					}
					records = got.Records
				}
			})
		}
	}
}
