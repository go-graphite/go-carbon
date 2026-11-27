package v2core

import (
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A live seal touches many already registered names, usually with few samples
// each. Keep registration/startup out of the measured planning allocation cost.
func BenchmarkRollupRegisteredPlanning(b *testing.B) {
	const names = 4096
	r := newCoreRollups()
	r.parallelism = 1
	batches := make([]Batch, names)
	for i := range batches {
		id := uint64(i + 1)
		p := RollupPolicy{Name: v2part.ID(fmt.Sprintf("registered.namespace.metric.%08d", names-i)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}, {Step: 60, Count: 1440}}}}
		raw, err := EncodeRollupPolicy(p)
		if err != nil {
			b.Fatal(err)
		}
		m, err := r.metric(id, raw)
		if err != nil {
			b.Fatal(err)
		}
		rollupTestInstall(r, id, m, freshMetadata)
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
}
