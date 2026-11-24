package v2core

import (
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	v2part "github.com/go-graphite/go-carbon/internal/v2part"
)

// BenchmarkRollupFreshPlanning plans a seal whose names are all new to the
// owner, so every point takes the duplicate-name checks (installed and pending
// name index). The plan is aborted, so each iteration sees the names fresh.
func BenchmarkRollupFreshPlanning(b *testing.B) {
	for _, names := range []int{4096, 65536} {
		b.Run(fmt.Sprint(names), func(b *testing.B) {
			r := newCoreRollups()
			r.parallelism = 1
			batches := make([]Batch, names)
			for i := range batches {
				id := uint64(i + 1)
				p := RollupPolicy{Name: v2part.ID(fmt.Sprintf("containers.k8s.prod.cluster-%02d.namespace-%03d.pod-%06x.cpu", i%13, i%251, names-i)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}}}}
				raw, err := EncodeRollupPolicy(p)
				if err != nil {
					b.Fatal(err)
				}
				m, err := r.metric(id, raw)
				if err != nil {
					b.Fatal(err)
				}
				batches[i] = Batch{ID: id, Registered: m, Order: 1, Clock: 1000, Points: []Row{{Time: 999, Seq: 1}}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				plan, err := r.plan(SealInput{Batches: batches})
				if err != nil {
					b.Fatal(err)
				}
				plan.Abort()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*names), "ns/point")
		})
	}
}
