package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
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

// Fixed point count at each supplied original-apply density. One producer per
// eight owners fills and retains compact original batches, then submits a single
// seal barrier to its disjoint owner group. CPU includes generation, compression,
// expansion, routing, core seal, rollups, publication and close. Journal I/O is excluded.
// These are finite cost samples, never a settled-compaction qualification.
func BenchmarkCoreRollupProductionDensity(b *testing.B) {
	density, err := strconv.Atoi(os.Getenv("SEAL_BENCH_DENSITY"))
	if err != nil || (density != 24 && density != 118 && density != 513) {
		b.Skip("set SEAL_BENCH_DENSITY to 24, 118 or 513")
	}
	metrics := max(1, (1<<21)/density)
	if s := os.Getenv("SEAL_BENCH_METRICS"); s != "" {
		metrics, err = strconv.Atoi(s)
		if err != nil || metrics < 1 {
			b.Fatal("invalid SEAL_BENCH_METRICS")
		}
	}
	cores := runtime.GOMAXPROCS(0)
	producers := max(1, cores/8)
	compact := os.Getenv("SEAL_BENCH_COMPACT") != "0"
	cfg := coreConfig{Shards: cores, Producers: producers, MaxRows: max(density, 2*metrics*density/cores), PartitionSeconds: 3600, AppendParts: true, JournalIdentity: "production-density-cost"}
	policyLimit, err := sealrollup.NewHeadPolicy(20_000_000, uint64(cfg.MaxRows)*uint64(cores)*96, 96, cores)
	if err != nil {
		b.Fatal(err)
	}
	e, _, err := openCoreRollupsWithHeadPolicy(b.TempDir(), cfg, policyLimit)
	if err != nil {
		b.Fatal(err)
	}
	inputs := atomicBenchInput(1, 1, 4)
	policy, err := json.Marshal(inputs[0].Config)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([][]uint64, producers)
	for id := range metrics {
		p := coreShard(uint64(id), cores) % producers
		ids[p] = append(ids[p], uint64(id))
	}
	jobs := make([]chan int, producers)
	errs := make(chan error, producers)
	headBytes := make([]int, producers)
	var workers sync.WaitGroup
	for p := range producers {
		jobs[p] = make(chan int)
		workers.Go(func() {
			h := compactRollupHead{batches: make([]compactRollupBatch, 0, len(ids[p]))}
			scratch := make([]row, density)
			rawRows := []row(nil)
			rawBatches := []coreBatch(nil)
			if !compact {
				rawRows = make([]row, len(ids[p])*density)
				rawBatches = make([]coreBatch, len(ids[p]))
				for i, id := range ids[p] {
					rawBatches[i] = coreBatch{ID: id, Policy: policy, Points: rawRows[i*density : (i+1)*density]}
				}
			}
			for n := range jobs[p] {
				order := uint64(n + 1)
				now := int64(1794700800 + (n+1)*density)
				var failure error
				for i, id := range ids[p] {
					points := scratch
					if !compact {
						points = rawBatches[i].Points
					}
					for j := range points {
						points[j] = row{ID: id, Time: now - int64(density) + int64(j), Bits: math.Float64bits(float64((n + j) % 97)), Seq: uint64(j + 1)}
					}
					if compact {
						failure = h.append(coreBatch{ID: id, Order: order, Clock: now, Policy: policy, Points: points})
						if failure != nil {
							break
						}
					} else {
						rawBatches[i].Order, rawBatches[i].Clock = order, now
					}
				}
				batches := rawBatches
				if compact && failure == nil {
					headBytes[p] = h.residentBytes()
					batches, failure = h.decode()
				} else if !compact {
					headBytes[p] = len(rawRows)*int(unsafe.Sizeof(row{})) + len(rawBatches)*int(unsafe.Sizeof(coreBatch{}))
				}
				if failure == nil {
					failure = e.submitRollupGroup(p, batches, order)
				}
				h.reset()
				errs <- failure
			}
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	start := atomicCPU()
	for n := 0; n < b.N; n++ {
		for _, job := range jobs {
			job <- n
		}
		for range producers {
			if err := <-errs; err != nil {
				b.Fatal(err)
			}
		}
	}
	for _, job := range jobs {
		close(job)
	}
	workers.Wait()
	if err = e.close(); err != nil {
		b.Fatal(err)
	}
	cpu := atomicCPU() - start
	b.StopTimer()
	points := float64(b.N * metrics * density)
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	runtime.KeepAlive(e)
	held := 0
	for _, n := range headBytes {
		held += n
	}
	b.ReportMetric(points/b.Elapsed().Seconds(), "points/s")
	b.ReportMetric(float64(cpu)/points, "cpu-ns/point")
	b.ReportMetric(float64(held)/float64(metrics*density), "head-B/point")
	b.ReportMetric(float64(memory.HeapAlloc), "postseal-heap-bytes")
	b.ReportMetric(float64(density), "points/metric")
	b.ReportMetric(float64(producers), "producers")

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
