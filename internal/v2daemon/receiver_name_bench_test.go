package v2daemon

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// prodLikeNames returns n distinct names of 280 bytes, the prod mean.
func prodLikeNames(n, offset int) []string {
	out := make([]string, n)
	for i := range out {
		base := fmt.Sprintf("app.region%03d.cluster.component.host%08d.requests.duration.", (offset+i)%997, offset+i)
		out[i] = base + strings.Repeat("x", 280-len(base))
	}
	return out
}

// BenchmarkApplicationOwnerAppend measures the L3 receiver adapter: "new"
// registers 1,024 new names per op; "known" appends 1,024 points for
// registered names per op (the steady-state hot path). Journal sync is outside
// the timer.
func BenchmarkApplicationOwnerAppend(b *testing.B) {
	const batch = 1024
	b.Run("new", func(b *testing.B) {
		s, resolve := openNameShareStore(b, 1)
		o := &applicationOwner{p: s.Producers[0], resolve: resolve}
		data := make([]points.Point, batch)
		for i := range data {
			data[i] = points.Point{Timestamp: 1000, Value: float64(i)}
		}
		inputs := make([][]string, b.N)
		for i := range inputs {
			_, inputs[i] = batchNames(prodLikeNames(batch, i*batch))
		}
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if err := o.Append(inputs[i], data, nil, 1010); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		inputs = nil
		runtime.GC()
		runtime.ReadMemStats(&m1)
		names := float64(b.N * batch)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/names, "ns/name")
		b.ReportMetric(float64(m1.Mallocs-m0.Mallocs)/names, "allocs/name")
		b.ReportMetric(float64(int64(m1.HeapAlloc)-int64(m0.HeapAlloc))/names, "live-B/name")
		if err := o.Drain(); err != nil {
			b.Fatal(err)
		}
	})
	b.Run("known", func(b *testing.B) {
		s, resolve := openNameShareStore(b, 1)
		o := &applicationOwner{p: s.Producers[0], resolve: resolve}
		data := make([]points.Point, batch)
		for i := range data {
			data[i] = points.Point{Timestamp: 1000, Value: float64(i)}
		}
		const sets = 16
		inputs := make([][]string, sets)
		for i := range inputs {
			_, inputs[i] = batchNames(prodLikeNames(batch, i*batch))
			if err := o.Append(inputs[i], data, nil, 1010); err != nil {
				b.Fatal(err)
			}
		}
		if err := o.Drain(); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if err := o.Append(inputs[i%sets], data, nil, 1010); err != nil {
				b.Fatal(err)
			}
			if i%sets == sets-1 {
				b.StopTimer()
				if err := o.Drain(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/point")
	})
}
