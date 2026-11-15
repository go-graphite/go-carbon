package main

import (
	"fmt"
	"math"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Run explicitly on a private filesystem. This is the opt-in archive SST seal
// path, including publication and flush, rather than only reducer arithmetic.
// Raw seal and input-log frontier coupling remain a separate integration gate.
func BenchmarkArchiveSeal(b *testing.B) {
	for _, archives := range []int{1, 4} {
		b.Run(fmt.Sprintf("archives%d", archives), func(b *testing.B) {
			dir := b.TempDir()
			db, err := pebble.Open(filepath.Join(dir, "db"), &pebble.Options{MemTableSize: 4 << 20, MaxConcurrentCompactions: func() int { return 1 }})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			c := sealrollup.Config{Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}, Method: sealrollup.Average, XFilesFactor: 0}
			c.Archives = c.Archives[:archives]
			const metrics = 128
			const points = 60
			inputs := make([]sealArchiveInput, metrics)
			states := make([]map[[2]int64]sealrollup.Sample, metrics)
			for id := range inputs {
				states[id] = map[[2]int64]sealrollup.Sample{}
				state := states[id]
				inputs[id] = sealArchiveInput{ID: uint64(id), Config: c, View: func(a int, s int64) (sealrollup.Sample, bool) { p, ok := state[[2]int64{int64(a), s}]; return p, ok }, Batches: []sealrollup.Batch{{Order: 1, Now: 1794704400}}}
				for j := 0; j < points; j++ {
					inputs[id].Batches[0].Points = append(inputs[id].Batches[0].Points, sealrollup.Sample{Time: 1794700800 + int64(j), Bits: math.Float64bits(float64(j)), Sequence: uint64(j + 1)})
				}
			}
			var before, after syscall.Rusage
			var bytes uint64
			b.ReportAllocs()
			b.ResetTimer()
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			for i := 0; i < b.N; i++ {
				for j := range inputs {
					inputs[j].Batches[0].Order = uint64(i + 1)
				}
				n, ds, err := sealArchiveParts(db, dir, 0, uint64(i+1), inputs, true)
				if err != nil {
					b.Fatal(err)
				}
				bytes += n
				for _, r := range ds {
					states[r.ID][[2]int64{int64(r.Delta.Archive), r.Delta.Slot}] = r.Delta.Sample
				}
			}
			if err := db.Flush(); err != nil {
				b.Fatal(err)
			}
			_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			b.StopTimer()
			cpu := ((after.Utime.Sec+after.Stime.Sec)-(before.Utime.Sec+before.Stime.Sec))*1e9 + ((int64(after.Utime.Usec)+int64(after.Stime.Usec))-(int64(before.Utime.Usec)+int64(before.Stime.Usec)))*1000
			b.ReportMetric(float64(cpu)/float64(b.N*metrics*points), "cpu-ns/point")
			b.ReportMetric(float64(b.N*metrics*points)/b.Elapsed().Seconds(), "points/s")
			b.ReportMetric(float64(bytes)/float64(b.N*metrics*points), "sst-B/point")
		})
	}
}
