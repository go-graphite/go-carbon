package carbonserver

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	runtimeMetrics "runtime/metrics"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
)

// BenchmarkSealedRender uses the actual v9 cold packer, Pebble fetch, Whisper
// archive selection, HTTP render handler and protobuf serialization. Eight
// series return about 1.2 MB/render, near the supplied production replay's
// 1.7 MB mean. The deterministic counter/decimal/random mix is synthetic;
// production value distributions were not captured. Response caching is off.
// Readers are closed-loop; p99 includes scheduler/GC delays but no arrival queue.
func BenchmarkSealedRender(b *testing.B) {
	const now, count, metrics = 1700100000, 16384, 8
	const first = now - count - 3600
	ctx := context.Background()
	db, err := store.Open(b.TempDir(), store.Options{ExactBlocks: true, TimeBuckets: true,
		Continuation: &whisperio.ContinuationCodec{}, CacheSize: 16 << 20,
		MetadataCacheSize: 4 << 20, MemTableSize: 4 << 20,
		Now: func() time.Time { return time.Unix(now, 0) }})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(73))
	want := make(map[string][]float64)
	for m := range metrics {
		name := fmt.Sprintf("sealed.render.m%d", m)
		_, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 86400}}, AggregationMethod: store.Average})
		if err != nil {
			b.Fatal(err)
		}
		points := make([]store.Point, count)
		values := make([]float64, count)
		for i := range points {
			v := float64(i*123 + i/97)
			switch m % 3 {
			case 1:
				v = float64(rng.Intn(100000)) / 100
			case 2:
				v = math.Float64frombits(0x3ff0000000000000 | rng.Uint64()>>12)
			}
			points[i] = store.Point{Timestamp: first + 1 + int64(i), Value: v}
			values[i] = v
		}
		want[name] = values
		if err := db.UpdateMany(ctx, name, points); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "exact", WriteWindow: time.Second,
		CPUFraction: 1, BytesPerSecond: 1 << 40}); err != nil {
		b.Fatal(err)
	}
	for steps := 0; ; steps++ {
		done, err := db.ColdPackStep(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if done {
			break
		}
		if steps > 1000 {
			b.Fatal("cold pack failed to finish")
		}
	}
	if db.ColdPackStats().Points == 0 {
		b.Fatal("fixture has no sealed points")
	}
	b.Logf("packed: %+v", db.ColdPackStats())
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "off"}); err != nil {
		b.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		b.Fatal(err)
	}
	l := NewCarbonserverListener(cache.New().Get)
	l.logger, l.accessLogger = zap.NewNop(), zap.NewNop()
	l.SetWhisperData(b.TempDir())
	l.SetMaxGlobs(100)
	l.SetMaxMetricsRendered(metrics)
	l.SetTrigramIndex(true)
	l.SetMetricStore(db)
	if err := l.RefreshMetricStoreIndex(); err != nil {
		b.Fatal(err)
	}
	l.SetQueryCacheEnabled(false)
	url := fmt.Sprintf("/render/?format=protobuf&target=sealed.render.*&from=%d&until=%d", first, first+count)
	render := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		l.renderHandler(w, httptest.NewRequest(http.MethodGet, url, http.NoBody))
		if w.Code != http.StatusOK {
			b.Errorf("render status %d: %s", w.Code, w.Body.String())
		}
		return w
	}
	// Check every returned bit before timing, including native-block boundaries.
	w := render()
	var response protov2.MultiFetchResponse
	if err := response.UnmarshalVT(w.Body.Bytes()); err != nil {
		b.Fatal(err)
	}
	if len(response.Metrics) != metrics {
		b.Fatalf("got %d metrics", len(response.Metrics))
	}
	for _, metric := range response.Metrics {
		values := want[metric.Name]
		if len(metric.Values) != count || metric.StartTime != first+1 || len(values) != count {
			b.Fatalf("bad range for %s: start %d count %d", metric.Name, metric.StartTime, len(metric.Values))
		}
		for i, v := range metric.Values {
			if metric.IsAbsent[i] || math.Float64bits(v) != math.Float64bits(values[i]) {
				b.Fatalf("%s point %d differs", metric.Name, i)
			}
		}
	}
	for _, readers := range []int{1, 8} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			for range readers {
				render()
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			latencies := make([]int64, b.N)
			var next atomic.Int64
			var wg sync.WaitGroup
			b.ReportAllocs()
			b.SetBytes(int64(w.Body.Len()))
			gcCPU := []runtimeMetrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
			runtimeMetrics.Read(gcCPU)
			gcBefore := gcCPU[0].Value.Float64()
			cpu := processCPUTime(b)
			b.ResetTimer()
			for range readers {
				wg.Go(func() {
					for {
						i := int(next.Add(1) - 1)
						if i >= b.N {
							return
						}
						start := time.Now()
						render()
						latencies[i] = time.Since(start).Nanoseconds()
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			cpu = processCPUTime(b) - cpu
			runtime.ReadMemStats(&after)
			slices.Sort(latencies)
			b.ReportMetric(float64(cpu)/float64(b.N), "cpu-ns/op")
			b.ReportMetric(float64(latencies[len(latencies)/2]), "p50-ns")
			b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100]), "p99-ns")
			b.ReportMetric(float64(after.NumGC-before.NumGC)/float64(b.N), "GC/op")
			b.ReportMetric(float64(after.PauseTotalNs-before.PauseTotalNs)/float64(b.N), "GC-pause-ns/op")
			runtimeMetrics.Read(gcCPU)
			b.ReportMetric((gcCPU[0].Value.Float64()-gcBefore)*1e9/float64(b.N), "GC-cpu-ns/op")
		})
	}
}
