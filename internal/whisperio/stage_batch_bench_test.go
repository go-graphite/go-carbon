//go:build unix

package whisperio

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
)

func BenchmarkStageBatch(b *testing.B) {
	for _, count := range []int{32, 210} {
		for _, size := range []int{8} {
			b.Run(fmt.Sprintf("points=%d/group=%d", count, size), func(b *testing.B) {
				ctx := context.Background()
				now := time.Unix(1790000000, 0)
				db := stateCacheStore(b, b.TempDir(), true, 128<<20, func() time.Time { return now })
				defer db.Close()
				updates := make([]store.MetricUpdate, 8)
				for i := range updates {
					name := fmt.Sprintf("group.%d", i)
					if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: percallSchemas[0].retentions, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
						b.Fatal(err)
					}
					updates[i] = store.MetricUpdate{Name: name, Points: make([]store.Point, count)}
				}
				base := now.Unix()
				var scratch stagetime.Update
				apply := func(i, n int) {
					start := time.Now()
					ctx, timer := stagetime.StartReusable(ctx, start, &scratch)
					defer timer.FinishUpdate(start)
					stamp := base + int64(i/8+1)*int64(count)
					now = time.Unix(stamp+1, 0)
					group := updates[i%8 : i%8+n]
					for index, update := range group {
						for j := range update.Points {
							update.Points[j] = store.Point{Timestamp: stamp - int64(count-j), Value: float64((i/8+i%8+index+j)%97) + .125}
						}
					}
					if size == 1 {
						if err := db.UpdateMany(ctx, group[0].Name, group[0].Points); err != nil {
							b.Fatal(err)
						}
					} else if err := db.UpdateBatch(ctx, group, nil); err != nil {
						b.Fatal(err)
					}
				}
				for i := 0; i < 128; i += size {
					apply(i, size)
				}
				var before, after syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &before)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					apply(i*size+128, size)
				}
				b.StopTimer()
				syscall.Getrusage(syscall.RUSAGE_SELF, &after)
				cpu := func(u syscall.Rusage) float64 {
					return float64(u.Utime.Sec+u.Stime.Sec)*1e6 + float64(u.Utime.Usec+u.Stime.Usec)
				}
				b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-us/batch")
				b.ReportMetric((cpu(after)-cpu(before))/float64(b.N*size*count), "CPU-s/Mpoints")
			})
		}
	}
}
