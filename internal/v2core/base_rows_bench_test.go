package v2core

import (
	"fmt"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// One iteration is one 10 s seal of 2048 names with one point per second.
// Bytes are staged seal SSTs plus Pebble flush/compaction writes, per point.
func BenchmarkOmitBaseRowsSeal(b *testing.B) {
	const names, perSeal = 2048, 10
	for _, corpus := range []struct{ name, def string }{{"single", "10s:1d"}, {"multi", "10s:6h,1m:7d,1h:1y"}} {
		for _, omit := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/omit=%v", corpus.name, omit), func(b *testing.B) {
				policies := make([][]byte, names)
				for i := range policies {
					_, policies[i] = rollupTestPolicy(&testing.T{}, fmt.Sprintf("bench.%s.m%05d", corpus.name, i), corpus.def, sealrollup.Average, .5)
				}
				cfg := adoptConfig()
				cfg.MaxRows = 1 << 20
				cfg.SealInterval = time.Hour
				cfg.KeepBaseRows = !omit
				e, err := openCore(b.TempDir(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				defer func() { _ = e.close() }()
				clock := int64(1801958400)
				batches := make([]coreBatch, names)
				rows := make([]row, names*perSeal)
				cpu := func() float64 {
					var u syscall.Rusage
					_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
					return float64(u.Utime.Nano() + u.Stime.Nano())
				}
				b.ResetTimer()
				cpuStart := cpu()
				for n := 0; n < b.N; n++ {
					clock += perSeal
					order := uint64(n + 1)
					for i := range batches {
						r := rows[i*perSeal : (i+1)*perSeal]
						for j := range r {
							r[j] = row{ID: uint64(i + 1), Time: clock - perSeal + int64(j), Bits: math.Float64bits(float64(i*j%97) / 4), Seq: uint64(j + 1)}
						}
						batches[i] = coreBatch{ID: uint64(i + 1), Policy: policies[i], Order: order, Clock: clock, Points: r}
					}
					if err = e.submitBatches(0, batches, order, true); err != nil {
						b.Fatal(err)
					}
				}
				points := float64(b.N * names * perSeal)
				o := e.owners[0]
				if err = o.db.Flush(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				b.ReportMetric((cpu()-cpuStart)/points, "cpu-ns/point")
				m := o.db.Metrics()
				var lsm uint64
				for _, level := range m.Levels {
					lsm += level.BytesFlushed + level.BytesCompacted
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/points, "ns/point")
				b.ReportMetric(float64(o.bytes.Load())/points, "sealB/point")
				b.ReportMetric(float64(lsm)/points, "lsmB/point")
				b.ReportMetric(float64(m.DiskSpaceUsage())/points, "diskB/point")
			})
		}
	}
}
