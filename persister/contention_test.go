//go:build linux || darwin

package persister

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/contentiontest"
	"github.com/go-graphite/go-carbon/receiver/parse"
)

const contentionWriters = 256

// The receiver's real plaintext parser -> cache -> persister -> Pebble, with
// bounded resident data. Each worker owns one metric, with ordinary hash collisions.
func contentionPipeline(t testing.TB) func(int, int) {
	t.Helper()
	c := cache.New()
	c.SetMaxSize(0)
	now := time.Unix(1790000000, 0)
	db, err := openTestStore(t, t.TempDir(), store.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	p := NewWhisper("", nil, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
	p.SetMetricStore(db)
	lines := make([][]byte, contentionWriters)
	names := make([]string, contentionWriters)
	for i := range names {
		names[i] = fmt.Sprintf("contention.pipeline.%04d", i)
		lines[i] = fmt.Appendf(nil, "%s 17.25 1789999990", names[i])
		_, err := db.Create(context.Background(), store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Last})
		if err != nil {
			t.Fatal(err)
		}
	}
	return func(w, round int) {
		for range 32 {
			name, value, ts, err := parse.PlainLine(lines[w])
			if err != nil {
				t.Error(err)
				return
			}
			c.AddPoint(name, value, ts)
		}
		p.store(names[w])
	}
}

func TestPersisterContentionBudget(t *testing.T) {
	if os.Getenv("GO_CARBON_CONTENTION") != "1" {
		t.Skip("set GO_CARBON_CONTENTION=1")
	}
	contentiontest.Run(t, contentionWriters, 128, 48, contentionPipeline(t))
}

func BenchmarkContentionPipeline(b *testing.B) {
	previous := runtime.SetMutexProfileFraction(-1)
	defer runtime.SetMutexProfileFraction(previous)
	for pair := range 5 {
		for _, fraction := range []int{0, 1000, 1000, 0, 0, 10000, 10000, 0} {
			b.Run(fmt.Sprintf("pair%d/fraction%d", pair, fraction), func(b *testing.B) {
				runtime.SetMutexProfileFraction(fraction)
				run := contentionPipeline(b)
				var worker atomic.Int64
				cpu := func() int64 {
					var u syscall.Rusage
					if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
						b.Fatal(err)
					}
					return (int64(u.Utime.Sec)+int64(u.Stime.Sec))*1e9 + (int64(u.Utime.Usec)+int64(u.Stime.Usec))*1000
				}
				b.SetParallelism(32) // 256 producers with -cpu=8
				b.ReportAllocs()
				b.ResetTimer()
				started := cpu()
				b.RunParallel(func(pb *testing.PB) {
					w := int(worker.Add(1)-1) % contentionWriters
					for pb.Next() {
						run(w, 0)
					}
				})
				b.StopTimer()
				b.ReportMetric(float64(cpu()-started)/float64(b.N*32), "cpu-ns/point")
			})
		}
	}
}
