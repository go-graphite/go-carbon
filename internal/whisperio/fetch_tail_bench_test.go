package whisperio

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// BenchmarkFetchTail reads metrics from many goroutines while a writer keeps
// appending to every metric and flushing, so flushes, compactions and version
// changes run throughout, as on a busy store. It reports the fetch latency
// percentiles, which those background operations stretch when fetches wait
// for Pebble's DB mutex.
func BenchmarkFetchTail(b *testing.B) {
	const metrics = 1000
	ctx := context.Background()
	stamp := atomic.Int64{}
	stamp.Store(1_792_368_000)
	clock := func() time.Time { return time.Unix(stamp.Load()+1, 0) }
	db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, Continuation: &ContinuationCodec{ReuseBlockKeys: true},
		Now: clock, SyncInterval: time.Hour, CacheSize: 256 << 20, MemTableSize: 4 << 20, BloomFilterBits: 10,
		L0TargetFileSize: 256 << 10, TargetFileSize: 512 << 10, LBaseMaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	name := func(i int) string { return fmt.Sprintf("bench.tail.%04d", i) }
	for i := range metrics {
		if _, err := db.Create(ctx, store.MetricConfig{Name: name(i), Retentions: []store.Retention{{Step: 1, Count: 172800}, {Step: 60, Count: 40320}},
			AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			b.Fatal(err)
		}
	}
	round := func(points, count int) {
		input := make([]store.Point, points)
		now := stamp.Add(int64(points))
		for i := range count {
			for j := range input {
				input[j] = store.Point{Timestamp: now - int64(points-j), Value: float64((i + j) % 97)}
			}
			if err := db.UpdateMany(ctx, name(i), input); err != nil {
				b.Error(err)
				return
			}
		}
	}
	for range 3 {
		round(1200, metrics)
	}
	for range 20 {
		round(60, metrics)
		if err := db.Flush(); err != nil {
			b.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// A small flush per round keeps flushes and compactions going.
			round(10, 20)
			_ = db.Flush()
		}
	}()

	// Half the CPUs fetch, leaving the rest to the writer and compactions, so
	// latencies show waits rather than an oversubscribed run queue.
	workers := max(1, runtime.GOMAXPROCS(0)/2)
	latencies := make([]time.Duration, b.N)
	var next atomic.Int64
	before := db.Stats()
	b.ResetTimer()
	var fetchers sync.WaitGroup
	for w := range workers {
		fetchers.Add(1)
		go func() {
			defer fetchers.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for {
				n := next.Add(1) - 1
				if n >= int64(b.N) {
					return
				}
				now := int(stamp.Load())
				t0 := time.Now()
				if _, err := db.Fetch(ctx, name(rng.Intn(metrics)), now-3600, now); err != nil {
					b.Error(err)
					return
				}
				latencies[n] = time.Since(t0)
			}
		}()
	}
	fetchers.Wait()
	b.StopTimer()
	close(stop)
	writer.Wait()
	after := db.Stats()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if n := len(latencies); n > 0 {
		for _, q := range []struct {
			unit string
			at   int
		}{{"p50-µs", n / 2}, {"p99-µs", n * 99 / 100}, {"p99.9-µs", n * 999 / 1000}, {"max-µs", n - 1}} {
			b.ReportMetric(float64(latencies[q.at].Microseconds()), q.unit)
		}
	}
	b.ReportMetric(float64(after.Flushes-before.Flushes), "flushes")
	b.ReportMetric(float64(after.Compactions-before.Compactions), "compactions")
}
