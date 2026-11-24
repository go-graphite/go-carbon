package v2core

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// sealCPUShape models the two production seal shapes: many single-archive
// names with few points per seal (containers), and multi-archive names
// (maggdqs). Names share long prefixes and arrive in random order.
type sealCPUShape struct {
	name     string
	archives []sealrollup.Archive
	method   sealrollup.Method
	points   int
	step     int64
}

var sealCPUShapes = []sealCPUShape{
	{"single1s", []sealrollup.Archive{{Step: 1, Count: 86400}}, sealrollup.Last, 3, 1},
	{"single10s", []sealrollup.Archive{{Step: 10, Count: 8640}}, sealrollup.Last, 1, 10},
	{"multi", []sealrollup.Archive{{Step: 10, Count: 8640}, {Step: 60, Count: 10080}, {Step: 600, Count: 52560}}, sealrollup.Average, 2, 10},
}

func sealCPUFixture(tb testing.TB, shape sealCPUShape, names int) (*coreRollups, []Batch, int) {
	r := newCoreRollups()
	r.parallelism = 1
	rng := rand.New(rand.NewSource(int64(names)))
	batches := make([]Batch, names)
	points := 0
	for i := range batches {
		id := uint64(i + 1)
		name := fmt.Sprintf("containers.k8s.prod.cluster-%02d.namespace-%03d.pod-%06x.container.cpu_usage_seconds_total", i%7, (i/7)%311, i)
		p := RollupPolicy{Name: v2part.ID(name), Group: "test", Config: sealrollup.Config{Method: shape.method, Archives: shape.archives}}
		raw, err := EncodeRollupPolicy(p)
		if err != nil {
			tb.Fatal(err)
		}
		m, err := r.metric(id, raw)
		if err != nil {
			tb.Fatal(err)
		}
		rollupTestInstall(r, id, m, freshMetadata)
		rows := make([]Row, shape.points)
		for j := range rows {
			rows[j] = Row{Time: 100000 + int64(j)*shape.step, Bits: uint64(rng.Int63()), Seq: uint64(j + 1)}
		}
		points += len(rows)
		batches[i] = Batch{ID: id, Registered: m, Order: uint64(i + 1), Clock: 100000 + int64(shape.points)*shape.step, Points: rows}
	}
	rng.Shuffle(len(batches), func(i, j int) { batches[i], batches[j] = batches[j], batches[i] })
	for i := range batches {
		batches[i].Order = uint64(i + 1)
	}
	return r, batches, points
}

// BenchmarkSealCPU reports seal planning and archive part encoding per point.
func BenchmarkSealCPU(b *testing.B) {
	for _, shape := range sealCPUShapes {
		sizes := []int{100000}
		if os.Getenv("SEAL_CPU_BIG") != "" {
			// Production owners hold ~180k names each while 64 owners share
			// the caches; 1M names approximates that cache pressure locally.
			sizes = append(sizes, 1000000)
		}
		for _, names := range sizes {
			r, batches, points := sealCPUFixture(b, shape, names)
			b.Run(fmt.Sprintf("plan/%s/%d", shape.name, names), func(b *testing.B) {
				heap := sealPlanRetainedHeap(b, r, batches)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					plan, err := r.planParts(SealInput{Batches: batches})
					if err != nil {
						b.Fatal(err)
					}
					plan.Abort()
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*points), "ns/point")
				b.ReportMetric(float64(heap)/float64(points), "heap-B/point")
			})
			b.Run(fmt.Sprintf("part/%s/%d", shape.name, names), func(b *testing.B) {
				dir := b.TempDir()
				plan, err := r.planParts(SealInput{Batches: batches})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for f, part := range plan.Parts {
						part.Spec.Source.Incarnation, part.Spec.Source.Stream = v2part.PartID{1}, "bench"
						if _, err := writePlannedRollupTuples(filepath.Join(dir, fmt.Sprintf("p%d-%d.sst", i, f)), part); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				plan.Abort()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*points), "ns/point")
			})
		}
	}
}

// sealPlanRetainedHeap is the live heap a fresh planner keeps after one seal
// plan of batches: its reusable stage, record, tuple/key and arena capacity.
func sealPlanRetainedHeap(b *testing.B, r *coreRollups, batches []Batch) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	plan, err := r.planParts(SealInput{Batches: batches})
	if err != nil {
		b.Fatal(err)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	plan.Abort()
	runtime.KeepAlive(plan)
	return after.HeapAlloc - min(after.HeapAlloc, before.HeapAlloc)
}
