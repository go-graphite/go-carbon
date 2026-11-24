package v2core

import (
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// A new name must not make a sparse seal sort the historical catalog.
func BenchmarkRollupSparseCatalog(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			r := newCoreRollups()
			r.parallelism = 1
			var batches []Batch
			for i := 0; i <= size; i++ {
				id := uint64(i + 1)
				p := RollupPolicy{Name: v2part.ID(fmt.Sprintf("sparse.metric.%08d", i)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}}
				encoded, err := EncodeRollupPolicy(p)
				if err != nil {
					b.Fatal(err)
				}
				if i < size {
					m, err := r.metric(id, encoded)
					if err != nil {
						b.Fatal(err)
					}
					rollupTestInstall(r, id, m, freshMetadata)
				}
				if i < 31 || i == size {
					batches = append(batches, Batch{ID: id, Policy: encoded, Order: 1, Clock: 1000, Points: []Row{{Time: 999, Seq: 1}}})
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p, err := r.plan(SealInput{Batches: batches})
				if err != nil {
					b.Fatal(err)
				}
				p.Abort()
			}
		})
	}
}
