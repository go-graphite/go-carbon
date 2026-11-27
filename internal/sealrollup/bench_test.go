package sealrollup

import (
	"fmt"
	"syscall"
	"testing"
)

func cpuNanos() int64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + (int64(r.Utime.Usec)+int64(r.Stime.Usec))*1000
}

// Baseline and rollups use identical inputs, fine-slot publication and sorting.
// CPU is process user+system, measured inside the timed region; use an isolated
// pinned process with -cpu=1. This measures the reducer, not disk compaction.
func BenchmarkSealReduce(b *testing.B) {
	for _, rows := range []int{1, 60, 3600} {
		for method := Average; method <= First; method++ {
			for _, archives := range []int{1, 4} {
				b.Run(fmt.Sprintf("rows%d/method%d/archives%d", rows, method, archives), func(b *testing.B) {
					c := config("1s:1d,1m:30d,1h:1y,1d:10y", method, 0)
					c.Archives = c.Archives[:archives]
					var points []Sample
					for i := 0; i < rows; i++ {
						points = append(points, sample(1794700800+int64(i), float64(i%97), uint64(i)))
					}
					state := map[key]Sample{}
					input := []Batch{{Order: 1, Now: 1794700800 + int64(rows), Points: points}}
					ds, err := Reduce(c, nil, input)
					if err != nil {
						b.Fatal(err)
					}
					install(state, ds)
					b.ReportAllocs()
					b.ResetTimer()
					start := cpuNanos()
					for i := 0; i < b.N; i++ {
						input[0].Order = uint64(i + 2)
						ds, err := Reduce(c, view(state), input)
						if err != nil {
							b.Fatal(err)
						}
						install(state, ds)
					}
					cpu := cpuNanos() - start
					b.StopTimer()
					b.ReportMetric(float64(cpu)/float64(b.N*rows), "cpu-ns/point")
					b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "points/s")
				})
			}
		}
	}
}

// Each parallel worker owns its view and buffers. Run with -cpu=1,2,4,8 and a
// disjoint eight-CPU taskset to expose scaling without adding storage effects.
func BenchmarkOwnerScaling(b *testing.B) {
	c := config("1s:1d,1m:30d,1h:1y,1d:10y", Average, 0)
	var points []Sample
	for i := 0; i < 3600; i++ {
		points = append(points, sample(1794700800+int64(i), float64(i%97), uint64(i)))
	}
	b.ReportAllocs()
	b.ResetTimer()
	start := cpuNanos()
	b.RunParallel(func(pb *testing.PB) {
		state := map[key]Sample{}
		input := []Batch{{Order: 1, Now: 1794704400, Points: points}}
		for pb.Next() {
			input[0].Order++
			ds, err := Reduce(c, view(state), input)
			if err != nil {
				panic(err)
			}
			install(state, ds)
		}
	})
	cpu := cpuNanos() - start
	b.StopTimer()
	b.ReportMetric(float64(cpu)/float64(b.N*len(points)), "cpu-ns/point")
	b.ReportMetric(float64(b.N*len(points))/b.Elapsed().Seconds(), "points/s")
}
