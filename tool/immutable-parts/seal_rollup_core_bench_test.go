package main

import (
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"testing"
)

// Actual core queues + original batch hook + atomic raw/archive/source-frontier
// ingest. One producer, one owner/Pebble per allocated core. No durable input
// log is timed here; -benchtime=100x is a finite seal cost, not sustained proof.
func BenchmarkCoreRollupSeal(b *testing.B) {
	for _, density := range []int{1, 4, 16} {
		for _, archives := range []int{1, 4} {
			b.Run(fmt.Sprintf("density%d/archives%d", density, archives), func(b *testing.B) {
				cfg := coreConfig{Shards: runtime.GOMAXPROCS(0), Producers: 1, MaxRows: 1 << 18, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "core-rollup-benchmark"}
				e, _, err := openCoreRollups(b.TempDir(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				inputs := atomicBenchInput(256, density, archives)
				batches := make([]coreBatch, len(inputs))
				for i, in := range inputs {
					policy, err := json.Marshal(in.Config)
					if err != nil {
						b.Fatal(err)
					}
					batches[i] = coreBatch{ID: in.ID, Policy: policy, Points: make([]row, density)}
				}
				b.ReportAllocs()
				b.ResetTimer()
				start := atomicCPU()
				for n := 0; n < b.N; n++ {
					order := uint64(n + 1)
					advanceAtomicInputs(inputs, order, density)
					for i, in := range inputs {
						batch := &batches[i]
						batch.Order = order
						batch.Clock = in.Batches[0].Now
						for j, p := range in.Batches[0].Points {
							batch.Points[j] = row{ID: in.ID, Time: p.Time, Bits: p.Bits, Seq: p.Sequence}
						}
					}
					if err = e.submitBatches(0, batches, order, true); err != nil {
						b.Fatal(err)
					}
				}
				if err = e.close(); err != nil {
					b.Fatal(err)
				}
				cpu := atomicCPU() - start
				b.StopTimer()
				points := float64(b.N * 256 * density)
				b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
				b.ReportMetric(float64(cpu)/points, "cpu-ns/point")
			})
		}
	}
}

// Fixed production-like sparse cardinality as cores change: 262144 metrics,
// one original point per metric per seal, and all four archives. Large owner
// files avoid shrinking the workload to a few metrics as owners increase.
// Three finite seals include close; this is not sustained compaction proof.
func BenchmarkCoreRollupSparse(b *testing.B) {
	const metrics = 1 << 18
	cores := runtime.GOMAXPROCS(0)
	cfg := coreConfig{Shards: cores, Producers: 1, MaxRows: 1 << 18, PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "sparse-rollup-benchmark"}
	e, _, err := openCoreRollups(b.TempDir(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	inputs := atomicBenchInput(metrics, 1, 4)
	policy, err := json.Marshal(inputs[0].Config)
	if err != nil {
		b.Fatal(err)
	}
	batches := make([]coreBatch, metrics)
	for i := range batches {
		batches[i] = coreBatch{ID: uint64(i), Policy: policy, Points: make([]row, 1)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	start := atomicCPU()
	for n := 0; n < b.N; n++ {
		order := uint64(n + 1)
		now := int64(1794700800 + n)
		for i := range batches {
			batch := &batches[i]
			batch.Order = order
			batch.Clock = now
			batch.Points[0] = row{ID: uint64(i), Time: now, Bits: math.Float64bits(float64(n % 97)), Seq: order}
		}
		if err = e.submitBatches(0, batches, order, true); err != nil {
			b.Fatal(err)
		}
	}
	if err = e.close(); err != nil {
		b.Fatal(err)
	}
	cpu := atomicCPU() - start
	b.StopTimer()
	points := float64(b.N * metrics)
	b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
	b.ReportMetric(float64(cpu)/points, "cpu-ns/point")
}
