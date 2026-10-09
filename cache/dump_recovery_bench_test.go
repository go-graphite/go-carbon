package cache

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func BenchmarkDumpPointsRecovery(b *testing.B) {
	const metrics, perMetric = 2000000, 11
	c := New()
	c.SetMaxSize(uint64(metrics*perMetric + 1))
	for i := 0; i < metrics; i++ {
		name := fmt.Sprintf("servers.host%05d.app.component.metric_%d.count", i%50000, i)
		p := &points.Points{Metric: name}
		for j := 0; j < perMetric; j++ {
			p.Data = append(p.Data, points.Point{Value: float64(i * j), Timestamp: 1700000000 + int64(j*60)})
		}
		c.Add(p)
	}
	for _, segments := range []int{1, 8, 16} {
		wal := false
		b.Run(fmt.Sprintf("segments=%d", segments), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				dir := b.TempDir()
				builder := recovery.NewConcurrentBuilder(nil, 1)
				builder.Reserve(int(c.Len()))
				dump, _ := recovery.NewWriter(filepath.Join(dir, "cache.bin"), 0, 1<<20, builder)
				xlog, _ := recovery.NewWriter(filepath.Join(dir, "input.bin"), 1, 4096, builder)
				var stop atomic.Bool
				done := make(chan int)
				go func() {
					count := 0
					for wal && !stop.Load() {
						_ = xlog.WritePoints(points.OnePoint(fmt.Sprintf("incoming.metric.%d", count%500000), 1, 1700000000))
						count++
					}
					done <- count
				}()
				start := time.Now()
				var err error
				if segments == 1 {
					err = c.DumpPoints(dump.WritePoints)
				} else {
					err = dump.WriteSegments(segments, func(seg int, emit func(*points.Points) error) error {
						return c.DumpShards(seg*ShardCount/segments, (seg+1)*ShardCount/segments, emit)
					})
				}
				if err != nil {
					b.Fatal(err)
				}
				if _, err := dump.Close(); err != nil {
					b.Fatal(err)
				}
				elapsed := time.Since(start)
				stop.Store(true)
				walPoints := <-done
				_, _ = xlog.Close()
				b.ReportMetric(float64(elapsed.Nanoseconds())/metrics, "ns/metric")
				b.ReportMetric(float64(walPoints)/elapsed.Seconds(), "walpts/s")
			}
		})
	}
}
