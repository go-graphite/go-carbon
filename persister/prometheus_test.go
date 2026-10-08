package persister

import (
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	whisper "github.com/go-graphite/go-whisper"
)

// TestPrometheusPersisterReplacement preserves histogram observations across
// replacement persisters, including the wrapped registerer used by the app.
func TestPrometheusPersisterReplacement(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		name := "plain"
		if wrapped {
			name = "wrapped"
		}
		t.Run(name, func(t *testing.T) {
			registry := prometheus.NewPedanticRegistry()
			var registerer prometheus.Registerer = registry
			if wrapped {
				registerer = prometheus.WrapRegistererWithPrefix("carbon_",
					prometheus.WrapRegistererWith(prometheus.Labels{"instance": "test"}, registry))
			}
			var created time.Time
			for i := 1; i <= 3; i++ {
				p := new(Whisper)
				p.InitPrometheus(registerer)
				p.prometheus.outOfOrderWriteLags.observePoints(
					[]*whisper.TimeSeriesPoint{{Time: 0}}, time.Unix(int64(i), 0))
				families, err := registry.Gather()
				if err != nil {
					t.Fatal(err)
				}
				if len(families) != 1 || len(families[0].Metric) != 1 {
					t.Fatalf("unexpected metric families: %v", families)
				}
				histogram := families[0].Metric[0].GetHistogram()
				if histogram.GetSampleCount() != uint64(i) || histogram.GetSampleSum() != float64(i*(i+1)/2) {
					t.Fatalf("observations reset on replacement %d: %v", i, histogram)
				}
				if histogram.GetCreatedTimestamp() == nil {
					t.Fatal("histogram is missing created timestamp")
				}
				if i == 1 {
					created = histogram.GetCreatedTimestamp().AsTime()
					continue
				}
				if !histogram.GetCreatedTimestamp().AsTime().Equal(created) {
					t.Fatal("histogram creation timestamp changed on replacement")
				}
			}
		})
	}
}

func TestWriteLagHistogramMatchesPrometheus(t *testing.T) {
	actualRegistry := prometheus.NewPedanticRegistry()
	actual := newWriteLagHistogram()
	actualRegistry.MustRegister(actual)

	wantRegistry := prometheus.NewPedanticRegistry()
	want := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "out_of_order_write_lag_exp",
		Help:    "Lag for incoming datapoints (exponential buckets)",
		Buckets: prometheus.ExponentialBuckets(.001, 2, 30),
	})
	wantRegistry.MustRegister(want)

	const baseUnix = 1_700_000_000
	observe := func(now time.Time) {
		points := []*whisper.TimeSeriesPoint{{Time: baseUnix}}
		actual.observePoints(points, now)
		want.Observe(now.Sub(time.Unix(baseUnix, 0)).Seconds())
	}
	observe(time.Unix(baseUnix, -int64(time.Hour)))
	observe(time.Unix(baseUnix, 0))
	observe(time.Unix(baseUnix, int64(time.Hour)))
	for i := 0; i < writeLagHistogramBucketCount; i++ {
		bound := time.Millisecond << i
		observe(time.Unix(baseUnix, int64(bound-time.Nanosecond)))
		observe(time.Unix(baseUnix, int64(bound)))
		observe(time.Unix(baseUnix, int64(bound+time.Nanosecond)))
	}

	// Empty write batches must not add an observation.
	actual.observePoints(nil, time.Unix(1_700_000_000, 0))

	now := time.Unix(1_700_000_000, 0)
	points := []*whisper.TimeSeriesPoint{
		{Time: int(now.Add(-365 * 24 * time.Hour).Unix())},
		{Time: int(now.Add(time.Hour).Unix())},
	}
	actual.observePoints(points, now)
	for _, point := range points {
		want.Observe(now.Sub(time.Unix(int64(point.Time), 0)).Seconds())
	}

	actualFamily, actualHistogram := gatherOneHistogram(t, actualRegistry)
	wantFamily, wantHistogram := gatherOneHistogram(t, wantRegistry)
	if actualFamily.GetHelp() != wantFamily.GetHelp() {
		t.Fatalf("help = %q, want %q", actualFamily.GetHelp(), wantFamily.GetHelp())
	}
	assertHistogramEqual(t, actualHistogram, wantHistogram)
}

func TestWriteLagHistogramConcurrentBatches(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	histogram := newWriteLagHistogram()
	registry.MustRegister(histogram)

	now := time.Unix(1_700_000_000, 0)
	points := []*whisper.TimeSeriesPoint{
		{Time: int(now.Add(-2 * time.Second).Unix())},
		{Time: int(now.Unix())},
		{Time: int(now.Add(3 * time.Second).Unix())},
	}
	const workers = 8
	const batchesPerWorker = 100

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			<-start
			for range batchesPerWorker {
				histogram.observePoints(points, now)
			}
		})
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	var previousCount uint64
	for {
		_, snapshot := gatherOneHistogram(t, registry)
		assertHistogramSnapshot(t, snapshot)
		if snapshot.GetSampleCount() < previousCount {
			t.Fatal("sample count decreased between scrapes")
		}
		previousCount = snapshot.GetSampleCount()
		select {
		case <-done:
			goto finished
		default:
		}
	}

finished:
	_, snapshot := gatherOneHistogram(t, registry)
	assertHistogramSnapshot(t, snapshot)
	if got, want := snapshot.GetSampleCount(), uint64(workers*batchesPerWorker*len(points)); got != want {
		t.Fatalf("sample count = %d, want %d", got, want)
	}
	if got, want := snapshot.GetSampleSum(), float64(-workers*batchesPerWorker); got != want {
		t.Fatalf("sample sum = %v, want %v", got, want)
	}
}

func TestPrometheusPersisterBatch(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	p := new(Whisper)
	p.InitPrometheus(registry)
	base := time.Now().Unix()
	points := []*whisper.TimeSeriesPoint{{Time: int(base - 20)}, {Time: int(base + 20)}}
	before := time.Now()
	p.registerOutOfOrderWriteLags(points)
	after := time.Now()
	_, histogram := gatherOneHistogram(t, registry)
	if histogram.GetSampleCount() != 2 {
		t.Fatalf("sample count = %d, want 2", histogram.GetSampleCount())
	}
	for _, bucket := range histogram.Bucket {
		want := uint64(1) // The future point belongs in every cumulative bucket.
		if bucket.GetUpperBound() >= 32.768 {
			want = 2
		}
		if bucket.GetCumulativeCount() != want {
			t.Fatalf("bucket %v = %d, want %d", bucket.GetUpperBound(), bucket.GetCumulativeCount(), want)
		}
	}
	minSum := 2 * before.Sub(time.Unix(base, 0)).Seconds()
	maxSum := 2 * after.Sub(time.Unix(base, 0)).Seconds()
	if sum := histogram.GetSampleSum(); sum < minSum-1e-12 || sum > maxSum+1e-12 {
		t.Fatalf("sample sum = %v, want [%v, %v]", sum, minSum, maxSum)
	}
}

func BenchmarkWriteLagHistogramBatch(b *testing.B) {
	now := time.Unix(1_700_000_000, 0)
	for _, size := range []int{1, 40} {
		points := profileShapedWriteLagPoints(now, size)
		for _, parallel := range []bool{false, true} {
			mode := "serial"
			if parallel {
				mode = "parallel"
			}
			b.Run("official_per_point/size_"+strconv.Itoa(size)+"/"+mode, func(b *testing.B) {
				histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
					Name: "write_lag_benchmark", Help: "Write lag benchmark", Buckets: writeLagHistogramBuckets,
				})
				b.ReportAllocs()
				b.ResetTimer()
				observe := func() {
					for _, point := range points {
						histogram.Observe(now.Sub(time.Unix(int64(point.Time), 0)).Seconds())
					}
				}
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							observe()
						}
					})
					return
				}
				for range b.N {
					observe()
				}
			})
			b.Run("batch/size_"+strconv.Itoa(size)+"/"+mode, func(b *testing.B) {
				histogram := newWriteLagHistogram()
				b.ReportAllocs()
				b.ResetTimer()
				observe := func() { histogram.observePoints(points, now) }
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							observe()
						}
					})
					return
				}
				for range b.N {
					observe()
				}
			})
		}
	}
}

func profileShapedWriteLagPoints(now time.Time, size int) []*whisper.TimeSeriesPoint {
	ages := make([]time.Duration, 0, 40)
	for _, sample := range []struct {
		age   time.Duration
		count int
	}{
		{age: 12 * time.Second, count: 4},
		{age: 24 * time.Second, count: 16},
		{age: 48 * time.Second, count: 19},
		{age: 96 * time.Second, count: 1},
	} {
		for range sample.count {
			ages = append(ages, sample.age)
		}
	}
	points := make([]*whisper.TimeSeriesPoint, size)
	for i := range points {
		points[i] = &whisper.TimeSeriesPoint{Time: int(now.Add(-ages[i%len(ages)]).Unix())}
	}
	return points
}

func gatherOneHistogram(t testing.TB, registry *prometheus.Registry) (*dto.MetricFamily, *dto.Histogram) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 1 || len(families[0].Metric) != 1 || families[0].Metric[0].GetHistogram() == nil {
		t.Fatalf("unexpected metric families: %v", families)
	}
	return families[0], families[0].Metric[0].GetHistogram()
}

func assertHistogramEqual(t testing.TB, got, want *dto.Histogram) {
	t.Helper()
	if got.GetSampleCount() != want.GetSampleCount() {
		t.Fatalf("sample count = %d, want %d", got.GetSampleCount(), want.GetSampleCount())
	}
	if delta := math.Abs(got.GetSampleSum() - want.GetSampleSum()); delta > math.Max(1, math.Abs(want.GetSampleSum()))*1e-12 {
		t.Fatalf("sample sum = %v, want %v", got.GetSampleSum(), want.GetSampleSum())
	}
	if len(got.Bucket) != len(want.Bucket) {
		t.Fatalf("bucket count = %d, want %d", len(got.Bucket), len(want.Bucket))
	}
	for i := range got.Bucket {
		if got.Bucket[i].GetUpperBound() != want.Bucket[i].GetUpperBound() || got.Bucket[i].GetCumulativeCount() != want.Bucket[i].GetCumulativeCount() {
			t.Fatalf("bucket %d = (%v, %d), want (%v, %d)", i, got.Bucket[i].GetUpperBound(), got.Bucket[i].GetCumulativeCount(), want.Bucket[i].GetUpperBound(), want.Bucket[i].GetCumulativeCount())
		}
	}
}

func assertHistogramSnapshot(t testing.TB, histogram *dto.Histogram) {
	t.Helper()
	var previous uint64
	for _, bucket := range histogram.Bucket {
		if got := bucket.GetCumulativeCount(); got < previous || got > histogram.GetSampleCount() {
			t.Fatalf("inconsistent bucket count %d after %d samples", got, histogram.GetSampleCount())
		}
		previous = bucket.GetCumulativeCount()
	}
}
