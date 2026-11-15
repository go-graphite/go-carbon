package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func atomicBenchInput(metrics, density, archives int) []atomicRollupInput {
	c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}, Method: sealrollup.Average, XFilesFactor: 0}
	c.Archives = c.Archives[:archives]
	inputs := make([]atomicRollupInput, metrics)
	for id := range inputs {
		inputs[id] = atomicRollupInput{ID: uint64(id), Config: c, Batches: []sealrollup.Batch{{Points: make([]sealrollup.Sample, density)}}}
	}
	return inputs
}
func advanceAtomicInputs(inputs []atomicRollupInput, order uint64, density int) {
	for _, in := range inputs {
		b := &in.Batches[0]
		b.Order = order
		b.Now = 1794700800 + int64(order)*int64(density)
		for j := range b.Points {
			b.Points[j] = sealrollup.Sample{Time: b.Now - int64(density) + int64(j), Bits: math.Float64bits(float64((order + uint64(j)) % 97)), Sequence: uint64(j + 1)}
		}
	}
}
func atomicCPU() int64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1000
}

// This is the atomic adapter's complete seal path, including original raw
// serialization, coarse computation, frontier, publication, flush and close.
// It excludes input-log append/routing and is not the production daemon engine.
// Use an owned private TMPDIR on the benchmark host, -cpu=1,2,4,8 and -benchtime
// fixed at a sufficient seal count. Each goroutine owns one DB/view, no lock.
func BenchmarkAtomicSeal(b *testing.B) {
	for _, density := range []int{1, 4, 16} {
		for _, archives := range []int{1, 4} {
			b.Run(fmt.Sprintf("density%d/archives%d", density, archives), func(b *testing.B) {
				root := b.TempDir()
				var owners atomic.Uint64
				var written, committed atomic.Uint64
				var failure atomic.Pointer[error]
				const metrics = 256
				b.ReportAllocs()
				b.ResetTimer()
				start := atomicCPU()
				b.RunParallel(func(pb *testing.PB) {
					id := owners.Add(1)
					dir := filepath.Join(root, fmt.Sprintf("owner%d", id))
					if err := os.Mkdir(dir, 0750); err != nil {
						failure.Store(&err)
						return
					}
					db, err := pebble.Open(filepath.Join(dir, "db"), &pebble.Options{MemTableSize: 4 << 20, MaxConcurrentCompactions: func() int { return 1 }})
					if err != nil {
						failure.Store(&err)
						return
					}
					defer func() {
						if err := db.Close(); err != nil {
							failure.Store(&err)
						}
					}()
					o, err := openAtomicRollupOwner(db, dir)
					if err != nil {
						failure.Store(&err)
						return
					}
					inputs := atomicBenchInput(metrics, density, archives)
					order := uint64(0)
					for pb.Next() {
						order++
						advanceAtomicInputs(inputs, order, density)
						n, e := o.Seal(inputs)
						if e != nil {
							failure.Store(&e)
							return
						}
						written.Add(n)
						committed.Add(metrics * uint64(density))
					}
					if err := db.Flush(); err != nil {
						failure.Store(&err)
					}
				})
				cpu := atomicCPU() - start
				b.StopTimer()
				if err := failure.Load(); err != nil {
					b.Fatal(*err)
				}
				count := float64(committed.Load())
				b.ReportMetric(count/b.Elapsed().Seconds(), "points/s")
				b.ReportMetric(float64(cpu)/count, "cpu-ns/point")
				b.ReportMetric(float64(written.Load())/count, "sst-B/point")
				b.ReportMetric(float64(runtime.GOMAXPROCS(0)), "cores")
			})
		}
	}
}
