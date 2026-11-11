package chunkstore_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// Run with -benchtime=1x -count=3 -cpu=8. The disk fixture reuses the bounded
// native-copy test's physical continuation history, with 32 overlapping names
// of 2M points each on both sides. Fixture setup and bit-exact Snapshot/Fetch
// checks are outside the timer. The total scratch budget stays at 8 GiB.
// "retained" forces GC while sampling and must not be used for throughput;
// "throughput" leaves GC alone. All fixture directories use b.TempDir.
func BenchmarkNativeCombineWorkers(b *testing.B) {
	const count, nameCount = 2_000_000, 32
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(2*count, 0) }
	open := func(b *testing.B, workers int) (*store.Store, func()) {
		b.Helper()
		s, err := store.Open(b.TempDir(), store.Options{NativeCombineWorkers: workers, NativeCombineMemory: 8 << 30,
			ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, Now: clock, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		if err != nil {
			b.Fatal(err)
		}
		s.StopBackground()
		closed := false
		closeStore := func() {
			if !closed {
				closed = true
				if err := s.Close(); err != nil {
					b.Error(err)
				}
			}
		}
		b.Cleanup(closeStore)
		return s, closeStore
	}
	source, _ := open(b, 2)
	names := make([]string, nameCount)
	for i := range names {
		names[i] = fmt.Sprintf("heavy.history.%02d", i)
		cfg := store.MetricConfig{Name: names[i], AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 1, Count: count}}}
		if err := seedLargeContinuation(source, cfg, clock); err != nil {
			b.Fatal(err)
		}
	}
	pin, err := source.PinNativeSource()
	if err != nil {
		b.Fatal(err)
	}
	defer pin.Close()
	for _, workers := range []int{1, 2, 4, 8} {
		for _, mode := range []string{"throughput", "retained"} {
			b.Run(fmt.Sprintf("workers=%d/%s", workers, mode), func(b *testing.B) {
				var elapsed time.Duration
				var peakDelta uint64
				b.StopTimer()
				for range b.N {
					target, closeTarget := open(b, workers)
					for _, name := range names {
						cfg := store.MetricConfig{Name: name, AggregationMethod: store.Sum, Retentions: []store.Retention{{Step: 1, Count: count}}}
						if err := seedLargeContinuation(target, cfg, clock); err != nil {
							b.Fatal(err)
						}
						if err := target.UpdateMany(ctx, name, []store.Point{{Timestamp: count, Value: 42}}); err != nil {
							b.Fatal(err)
						}
					}
					runtime.GC()
					var before runtime.MemStats
					runtime.ReadMemStats(&before)
					var peak atomic.Uint64
					peak.Store(before.HeapAlloc)
					stop, stopped := make(chan struct{}), make(chan struct{})
					go func() {
						defer close(stopped)
						if mode != "retained" {
							return
						}
						ticker := time.NewTicker(20 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-stop:
								return
							case <-ticker.C:
								runtime.GC()
								var ms runtime.MemStats
								runtime.ReadMemStats(&ms)
								if ms.HeapAlloc > peak.Load() {
									peak.Store(ms.HeapAlloc)
								}
							}
						}
					}()
					input, output := io.Pipe()
					exported := make(chan error, 1)
					request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "donor", End: nameCount, Names: names, Combine: true}
					b.StartTimer()
					start := time.Now()
					go func() { err := pin.Write(ctx, names, output); output.CloseWithError(err); exported <- err }()
					receipt, err := target.NativeImport(ctx, request, input)
					input.CloseWithError(err)
					exportErr := <-exported
					elapsed += time.Since(start)
					b.StopTimer()
					close(stop)
					<-stopped
					peakDelta = max(peakDelta, peak.Load()-before.HeapAlloc)
					if err != nil || exportErr != nil {
						b.Fatal(err, exportErr)
					}
					if receipt.CopiedCount != nameCount || receipt.DeferredCount != 0 || len(receipt.Skipped) != 0 {
						b.Fatal(receipt)
					}
					// Check every output bit; target wins the first point's tie.
					for _, name := range names {
						snap, err := target.Snapshot(ctx, name)
						if err != nil {
							b.Fatal(err)
						}
						if len(snap.Archives) != 1 || len(snap.Archives[0].Points) != count {
							b.Fatal("lost physical history")
						}
						for i, p := range snap.Archives[0].Points {
							want := float64(1)
							if i == 0 {
								want = 42
							}
							if p.Timestamp != int64(count+i) || math.Float64bits(p.Value) != math.Float64bits(want) {
								b.Fatalf("changed output %s point %d", name, i)
							}
						}
						for _, from := range []int{count, count + count/2, 2*count - 256} {
							series, err := target.Fetch(ctx, name, from, from+128)
							if err != nil || series == nil {
								b.Fatal("Fetch", err)
							}
							for i, v := range series.Values {
								ts := series.FromTime + i*series.Step
								if ts < count || ts >= 2*count {
									continue
								}
								want := float64(1)
								if ts == count {
									want = 42
								}
								if math.Float64bits(v) != math.Float64bits(want) {
									b.Fatalf("changed Fetch %s point %d", name, ts)
								}
							}
						}
					}
					// Close each iteration rather than accumulating open stores.
					closeTarget()
				}
				if mode == "throughput" {
					b.ReportMetric(float64(nameCount*b.N)/elapsed.Seconds(), "names/s")
				}
				if mode == "retained" {
					b.ReportMetric(float64(peakDelta), "peak-retained-B")
				}
			})
		}
	}
}
