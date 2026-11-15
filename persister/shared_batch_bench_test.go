package persister

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

// Drive the worker's batch path: cache pop, continuation codec, shared commit,
// cache confirmation and timing, with the same reusable timer as a worker.
func BenchmarkSharedBatchTimings(b *testing.B) {
	for _, count := range []int{32, 210} {
		b.Run(fmt.Sprintf("points=%d", count), func(b *testing.B) {
			update := sharedTimingWorkload(b, count)
			for i := range 16 {
				update(i)
			}
			var before, after syscall.Rusage
			syscall.Getrusage(syscall.RUSAGE_SELF, &before)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				update(i + 16)
			}
			b.StopTimer()
			syscall.Getrusage(syscall.RUSAGE_SELF, &after)
			cpu := func(u syscall.Rusage) float64 {
				return float64(u.Utime.Sec+u.Stime.Sec)*1e6 + float64(u.Utime.Usec+u.Stime.Usec)
			}
			b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-us/batch")
			b.ReportMetric((cpu(after)-cpu(before))/float64(b.N*8*count), "CPU-s/Mpoints")
		})
	}
}

func sharedTimingWorkload(tb testing.TB, count int) func(int) {
	tb.Helper()
	now := time.Unix(1790000000, 0)
	db, err := store.Open(tb.TempDir(), store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, TimeBuckets: true, Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: 8 << 20, MemTableSize: 4 << 20, MetadataCacheSize: 128 << 20})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	c := cache.New()
	p := NewWhisper(tb.TempDir(), nil, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
	p.SetMetricStore(db)
	names := make([]string, 8)
	for i := range names {
		names[i] = fmt.Sprintf("batch.timing.%d", i)
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			tb.Fatal(err)
		}
	}
	var scratch stagetime.Update
	update := func(round int) {
		now = now.Add(time.Duration(count) * time.Second)
		for _, name := range names {
			batch := &points.Points{Metric: name, Data: make([]points.Point, count)}
			for j := range batch.Data {
				batch.Data[j] = points.Point{Timestamp: now.Unix() - int64(count-j), Value: float64((round+j)%97) + .125}
			}
			c.Add(batch)
		}
		p.storeSharedBatchTimed(names, &scratch)
	}
	return update
}
