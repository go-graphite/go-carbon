package carbonserver

// The benchmark retains the initial optimistic prefix experiment for comparison
// with the production moving-window oracle below. Fixtures and counting I/O
// stay in tests; no production request trace is available locally.

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

type rangeScreenCodec struct {
	*whisperio.ContinuationCodec
	count        bool
	bytes, calls uint64
}

type rangeScreenIO struct {
	store.ContinuationIO
	c *rangeScreenCodec
}

func (io rangeScreenIO) Get(key []byte) ([]byte, error) {
	b, err := io.ContinuationIO.Get(key)
	io.c.calls++
	io.c.bytes += uint64(len(b))
	return b, err
}
func (io rangeScreenIO) Probe(key []byte) ([]byte, error) {
	b, err := io.ContinuationIO.(store.ContinuationProber).Probe(key)
	io.c.calls++
	io.c.bytes += uint64(len(b))
	return b, err
}
func (io rangeScreenIO) Visit(prefix []byte, fn func([]byte, []byte) error) error {
	return io.ContinuationIO.Visit(prefix, func(k, v []byte) error {
		io.c.calls++
		io.c.bytes += uint64(len(v))
		return fn(k, v)
	})
}
func (io rangeScreenIO) TimeBuckets() bool {
	return io.ContinuationIO.(store.ContinuationTimeBuckets).TimeBuckets()
}
func (c *rangeScreenCodec) Fetch(ctx context.Context, kv store.ContinuationIO, m store.Metadata, from, until int, now func() time.Time) (*store.Series, error) {
	if c.count {
		kv = rangeScreenIO{kv, c}
	}
	return c.ContinuationCodec.Fetch(ctx, kv, m, from, until, now)
}

const rangeScreenNow = 1790000000

func newRangeScreenFixture(t testing.TB, step int, cacheBytes int64) (*CarbonserverListener, *store.Store, *rangeScreenCodec, []string, *atomic.Int64) {
	t.Helper()
	stamp := &atomic.Int64{}
	stamp.Store(rangeScreenNow)
	codec := &rangeScreenCodec{ContinuationCodec: &whisperio.ContinuationCodec{ReuseBlockKeys: true}}
	db, err := store.Open(t.TempDir(), store.Options{
		ExactBlocks: true, Continuation: codec, Now: func() time.Time { return time.Unix(stamp.Load(), 0) },
		SyncInterval: time.Hour, CacheSize: cacheBytes, MetadataCacheSize: 4 << 20, MemTableSize: 2 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	l := NewCarbonserverListener(func(string) []points.Point { return nil })
	l.logger, l.accessLogger = zap.NewNop(), zap.NewNop()
	l.SetMetricStore(db)
	rng := rand.New(rand.NewSource(17))
	names := make([]string, 16)
	for i := range names {
		name := fmt.Sprintf("dashboard.service.requests.by_host.app-%03d.count", i)
		names[i] = name
		_, err := db.Create(context.Background(), store.MetricConfig{Name: name,
			Retentions:        []store.Retention{{Step: step, Count: 86400 / step}, {Step: step * 60, Count: 10080}},
			AggregationMethod: store.Average, XFilesFactor: .5})
		if err != nil {
			t.Fatal(err)
		}
		// Two hours, updated in the store's 960s writeout batches. Counters,
		// decimals, noisy gauges and gaps mirror the render codec fixtures.
		var input []store.Point
		for stamp := rangeScreenNow - 7200; stamp < rangeScreenNow; stamp += step {
			v := float64(stamp % 10000)
			switch i % 4 {
			case 1:
				v = float64(stamp%1000) / 10
			case 2:
				v = rng.Float64() * 100
			case 3:
				if rng.Intn(5) == 0 {
					continue
				}
			}
			input = append(input, store.Point{Timestamp: int64(stamp), Value: v})
			if len(input)*step >= 960 {
				if err := db.UpdateMany(context.Background(), name, input); err != nil {
					t.Fatal(err)
				}
				input = input[:0]
			}
		}
		if len(input) > 0 {
			if err := db.UpdateMany(context.Background(), name, input); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	return l, db, codec, names, stamp
}

func rangeScreenFetch(l *CarbonserverListener, name string, prefix response, from, until int32) (response, error) {
	// Prefix stops on the store's interval grid. Whisper's Interval rounds
	// exclusive from up by one step; subtract it to fetch the adjacent slot.
	tail, err := l.fetchSingleMetric(name, "alias", int32(prefix.StopTime-prefix.StepTime), until)
	if err != nil {
		return response{}, err
	}
	if tail.StepTime != prefix.StepTime || tail.StartTime != prefix.StopTime {
		return response{}, fmt.Errorf("archive/grid changed: prefix=%d/%d tail=%d/%d", prefix.StopTime, prefix.StepTime, tail.StartTime, tail.StepTime)
	}
	values := make([]float64, len(prefix.Values)+len(tail.Values))
	copy(values, prefix.Values)
	copy(values[len(prefix.Values):], tail.Values)
	tail.StartTime, tail.Values = prefix.StartTime, values
	tail.RequestStartTime, tail.RequestStopTime = int64(from), int64(until)
	return tail, nil
}

var rangeScreenSink []byte

func BenchmarkRenderRangeScreen(b *testing.B) {
	for _, step := range []int{1, 60} {
		l, db, codec, names, _ := newRangeScreenFixture(b, step, 8<<20)
		const from, until = int32(rangeScreenNow - 3600), int32(rangeScreenNow)
		for _, tailSeconds := range []int{360, 1020, 1860} {
			prefixes := make([]response, len(names))
			for i, name := range names {
				full, err := l.fetchSingleMetric(name, "alias", from, until)
				if err != nil {
					b.Fatal(err)
				}
				n := len(full.Values) - tailSeconds/step
				prefixes[i] = full
				prefixes[i].Values = append([]float64(nil), full.Values[:n]...)
				prefixes[i].StopTime = full.StartTime + int64(n*step)
			}
			for _, cached := range []bool{false, true} {
				b.Run(fmt.Sprintf("step%d/tail%d/cached%v", step, tailSeconds, cached), func(b *testing.B) {
					// Validate the complete wire response against full fetch, including
					// NaN bits/absence, metadata, alignment and request timestamps.
					for i, name := range names {
						want, err := l.fetchSingleMetric(name, "alias", from, until)
						if err != nil {
							b.Fatal(err)
						}
						got, err := rangeScreenFetch(l, name, prefixes[i], from, until)
						if err != nil {
							b.Fatal(err)
						}
						if want.StartTime != got.StartTime || want.StopTime != got.StopTime || len(want.Values) != len(got.Values) {
							b.Fatal("shape differs")
						}
						for j, v := range want.Values {
							if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
								b.Fatal("values differ", i, j)
							}
						}
					}
					fetch := func(i int) response {
						var r response
						var err error
						if cached {
							r, err = rangeScreenFetch(l, names[i], prefixes[i], from, until)
						} else {
							r, err = l.fetchSingleMetric(names[i], "alias", from, until)
						}
						if err != nil {
							b.Fatal(err)
						}
						return r
					}
					codec.bytes, codec.calls, codec.count = 0, 0, true
					for i := range names {
						fetch(i)
					}
					bytesRead, calls := codec.bytes/uint64(len(names)), codec.calls/uint64(len(names))
					codec.count = false
					latencies := make([]int64, b.N)
					before := db.Stats().FS
					var cpuBefore, cpuAfter syscall.Rusage
					_ = syscall.Getrusage(syscall.RUSAGE_SELF, &cpuBefore)
					b.ReportAllocs()
					b.ResetTimer()
					for n := range b.N {
						start := time.Now()
						r := fetch(n % len(names))
						wire, err := r.proto3().MarshalVT()
						if err != nil {
							b.Fatal(err)
						}
						rangeScreenSink = wire
						latencies[n] = time.Since(start).Nanoseconds()
					}
					b.StopTimer()
					_ = syscall.Getrusage(syscall.RUSAGE_SELF, &cpuAfter)
					cpuNS := (cpuAfter.Utime.Nano() + cpuAfter.Stime.Nano()) - (cpuBefore.Utime.Nano() + cpuBefore.Stime.Nano())
					b.ReportMetric(float64(cpuNS)/float64(b.N), "cpu-ns/op")
					after := db.Stats().FS
					sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
					b.ReportMetric(float64(bytesRead), "encoded_B/series")
					b.ReportMetric(float64(calls), "KV/series")
					b.ReportMetric(float64(after.Bytes[store.FSRead][store.FSTable]-before.Bytes[store.FSRead][store.FSTable])/float64(b.N), "table_B/op")
					b.ReportMetric(float64(latencies[len(latencies)/2]), "p50-ns")
					b.ReportMetric(float64(latencies[len(latencies)*99/100]), "p99-ns")
					b.ReportMetric(float64(len(rangeScreenSink)), "wire_B/series")
				})
			}
		}
	}
}

// Exercise 31 actual 60s refreshes through the production listener. The two
// independent runs must emit identical protobufs, including recent-write
// invalidation at minute 16 and a historical correction at minute 24.
func TestRenderRangeMovingWindowScreen(t *testing.T) {
	for _, step := range []int{1, 60} {
		for _, cacheBytes := range []int64{8 << 20, 32 << 10} {
			var oracle [][]byte
			for _, cached := range []bool{false, true} {
				t.Run(fmt.Sprintf("step%d/cache%d/cached%v", step, cacheBytes, cached), func(t *testing.T) {
					l, db, codec, names, stamp := newRangeScreenFixture(t, step, cacheBytes)
					if err := l.SetQueryRangeCache(cached, 1, 30*time.Minute); err != nil {
						t.Fatal(err)
					}
					if c := l.queryRangeCache.Load(); c != nil {
						c.now = func() time.Time { return time.Unix(stamp.Load(), 0) }
					}
					var got [][]byte
					var latency []int64
					var cpuTotal int64
					var allocated, allocs, tableBytes uint64
					codec.bytes, codec.calls, codec.count = 0, 0, true
					for refresh := range 31 {
						until := int32(rangeScreenNow - 1800 + refresh*60)
						from := until - 3600
						stamp.Store(int64(until))
						if refresh == 16 || refresh == 24 {
							age := 300
							if refresh == 24 {
								age = 2700
							}
							for _, name := range names {
								if err := db.UpdateMany(context.Background(), name, []store.Point{{Timestamp: int64(until) - int64(age), Value: 12345 + float64(refresh)}}); err != nil {
									t.Fatal(err)
								}
							}
						}
						beforeFS := db.Stats().FS
						var beforeMem, afterMem runtime.MemStats
						runtime.ReadMemStats(&beforeMem)
						var before, after syscall.Rusage
						_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
						for _, name := range names {
							start := time.Now()
							r, err := l.fetchSingleMetric(name, "alias", from, until)
							if err != nil {
								t.Fatal(err)
							}
							wire, err := r.proto3().MarshalVT()
							if err != nil {
								t.Fatal(err)
							}
							latency = append(latency, time.Since(start).Nanoseconds())
							got = append(got, wire)
						}
						_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
						cpuTotal += after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
						runtime.ReadMemStats(&afterMem)
						allocated += afterMem.TotalAlloc - beforeMem.TotalAlloc
						allocs += afterMem.Mallocs - beforeMem.Mallocs
						afterFS := db.Stats().FS
						tableBytes += afterFS.Bytes[store.FSRead][store.FSTable] - beforeFS.Bytes[store.FSRead][store.FSTable]
					}
					codec.count = false
					if cached {
						if len(got) != len(oracle) {
							t.Fatal("response count differs")
						}
						for i, wire := range got {
							if !bytes.Equal(wire, oracle[i]) {
								t.Fatalf("response differs at refresh=%d series=%d", i/len(names), i%len(names))
							}
						}
						if l.metrics.RangeCacheHit != 416 || l.metrics.RangeCacheMiss != 80 {
							t.Fatalf("hits=%d misses=%d", l.metrics.RangeCacheHit, l.metrics.RangeCacheMiss)
						}
					} else {
						oracle = got
					}
					sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
					n := float64(len(got))
					t.Logf("requests=%d hits=%d misses=%d cpu_ns/series=%.0f B/series=%.0f allocs/series=%.1f encoded_B/series=%.1f KV/series=%.1f table_B/series=%.1f p50_ns=%d p99_ns=%d", len(got), l.metrics.RangeCacheHit, l.metrics.RangeCacheMiss, float64(cpuTotal)/n, float64(allocated)/n, float64(allocs)/n, float64(codec.bytes)/n, float64(codec.calls)/n, float64(tableBytes)/n, latency[len(latency)/2], latency[len(latency)*99/100])
				})
			}
		}
	}
}
