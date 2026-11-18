package v2core

// BenchmarkSealPerName measures the real core seal path (openCore,
// submitBatches, Pebble ingest, KeepBaseRows=false) in steady state. Arm
// "fine" truncates the schema to its first archive. LD_PERSEAL forces the
// points per name per seal (production names mostly carry one).

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func BenchmarkSealPerName(b *testing.B) {
	names := 1024
	if v := os.Getenv("LD_NAMES"); v != "" {
		fmt.Sscan(v, &names)
	}
	warm := 6
	ladders := []string{"1s:1d,1m:30d,1h:1y,1d:10y", "10s:2d,1m:14d,30m:2y", "1m:30d,1h:1y,1d:10y"}
	if v := os.Getenv("LD_LADDERS"); v != "" {
		ladders = strings.Split(v, ";")
	}
	arms := []string{"full"}
	if v := os.Getenv("LD_ARM"); v != "" {
		arms = []string{v}
	}
	for _, ladder := range ladders {
		for _, arm := range arms {
			def := ladder
			if arm == "fine" {
				def = strings.Split(ladder, ",")[0]
			}
			b.Run(fmt.Sprintf("%s/%s", ladder, arm), func(b *testing.B) {
				p0, _ := rollupTestPolicy(&testing.T{}, "probe", def, sealrollup.Average, 0)
				step := p0.Config.Archives[0].Step
				span := max(int64(10), step)
				perSeal := int(span / step)
				if v := os.Getenv("LD_PERSEAL"); v != "" {
					fmt.Sscan(v, &perSeal)
					span = int64(perSeal) * step
				}
				policies := make([][]byte, names)
				for i := range policies {
					_, policies[i] = rollupTestPolicy(&testing.T{}, fmt.Sprintf("containers.k8s.prod.cluster-%02d.ns-%03d.pod-%06x.cpu", i%7, (i/7)%311, i), def, sealrollup.Average, 0)
				}
				cfg := adoptConfig()
				cfg.MaxRows = max(1<<20, names*perSeal)
				cfg.SealInterval = time.Hour
				cfg.KeepBaseRows = false
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
				seal := func(n int) {
					clock += span
					order := uint64(n + 1)
					for i := range batches {
						r := rows[i*perSeal : (i+1)*perSeal]
						for j := range r {
							r[j] = row{ID: uint64(i + 1), Time: clock - span + int64(j)*step, Bits: math.Float64bits(float64((i*31+n*7+j)%997) / 4), Seq: uint64(j + 1)}
						}
						batches[i] = coreBatch{ID: uint64(i + 1), Policy: policies[i], Order: order, Clock: clock, Points: r}
					}
					if err = e.submitBatches(0, batches, order, true); err != nil {
						b.Fatal(err)
					}
				}
				// Warm-up registers names and fills KeepRecent before timing.
				for n := 0; n < warm; n++ {
					seal(n)
				}
				var m0, m1 runtime.MemStats
				runtime.ReadMemStats(&m0)
				b.ResetTimer()
				cpuStart := cpu()
				for n := warm; n < warm+b.N; n++ {
					seal(n)
				}
				points := float64(b.N * names * perSeal)
				o := e.owners[0]
				if err = o.db.Flush(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				runtime.ReadMemStats(&m1)
				b.ReportMetric((cpu()-cpuStart)/points, "cpu-ns/point")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/points, "ns/point")
				b.ReportMetric(float64(m1.Mallocs-m0.Mallocs)/points, "allocs/point")
				b.ReportMetric(float64(m1.TotalAlloc-m0.TotalAlloc)/points, "B/point")
				b.ReportMetric(float64(o.bytes.Load())/points, "sealB/point")
				b.ReportMetric(float64(o.db.Metrics().DiskSpaceUsage())/points, "diskB/point")
			})
		}
	}
}
