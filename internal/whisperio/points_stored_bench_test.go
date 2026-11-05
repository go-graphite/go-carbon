//go:build darwin || linux

package whisperio

import (
	"context"
	"fmt"
	"math"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Measure complete read-only cold-pack passes, including catalog traversal and
// checkpoints. The fixture mixes sparse and dense native ranges; a long write
// window keeps every iteration on the same descriptor-reading path.
func BenchmarkColdPackPointsStored(b *testing.B) {
	ctx := context.Background()
	db, err := store.Open(b.TempDir(), coldOptions(&ContinuationCodec{}))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	const metrics = 64
	points := make([]store.Point, 1800)
	for i := range points {
		points[i] = store.Point{Timestamp: coldBase + int64(i*60), Value: math.Sin(float64(i))}
	}
	for i := range metrics {
		name := fmt.Sprintf("bench.%03d", i)
		if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 60, Count: 10080}, {Step: 3600, Count: 8760}}, AggregationMethod: store.Average}); err != nil {
			b.Fatal(err)
		}
		n := 64
		if i%8 == 0 {
			n = len(points)
		}
		if err := db.UpdateMany(ctx, name, points[:n]); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: 100 * 365 * 24 * time.Hour, CPUFraction: 1, BytesPerSecond: 1 << 40, MaxCompactionDebt: 1 << 40}); err != nil {
		b.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		b.Fatal(err)
	}
	var before, after syscall.Rusage
	b.ReportAllocs()
	b.ResetTimer()
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	for range b.N {
		for {
			done, err := db.ColdPackStep(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if done {
				break
			}
		}
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	b.StopTimer()
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec)*1e9 + float64(r.Utime.Usec+r.Stime.Usec)*1e3
	}
	b.ReportMetric((cpu(after)-cpu(before))/float64(b.N*metrics), "cpu-ns/metric")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*metrics), "ns/metric")
	if db.ColdPackStats().Blocks != 0 {
		b.Fatal("benchmark rewrote hot ranges")
	}
}
