package v2core

import (
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func BenchmarkSealPlanNotDue(b *testing.B) { benchmarkSealPlanNotDue(b, false) }

func BenchmarkSealPlanCommitNotDue(b *testing.B) { benchmarkSealPlanNotDue(b, true) }

func benchmarkSealPlanNotDue(b *testing.B, commit bool) {
	shape := sealCPUShape{"not-due", []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}, sealrollup.Average, 1, 1}
	r, batches, points := sealCPUFixture(b, shape, 4096)
	r.now = func() time.Time { return time.Unix(3603, 0) }
	for i := range batches {
		batches[i].Points[0].Time = 3601
		batches[i].Clock = 3602
	}
	plan, err := r.planParts(SealInput{Batches: batches})
	if err != nil {
		b.Fatal(err)
	}
	plan.Commit()
	for i := range batches {
		batches[i].Points[0].Time = 3602
		batches[i].Clock = 3603
		batches[i].Order += 4096
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if commit {
			for j := range batches {
				batches[j].Order += 4096
			}
		}
		plan, err := r.planParts(SealInput{Batches: batches})
		if err != nil {
			b.Fatal(err)
		}
		if commit {
			plan.Commit()
		} else {
			plan.Abort()
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*points), "ns/point")
}
