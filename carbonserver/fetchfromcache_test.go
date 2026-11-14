package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-whisper"
	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	pickle "github.com/lomik/og-rek"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	cacheOnlySchema = "1s:1d,1m:30d,1h:1y,1d:10y"
	cacheOnlyMetric = "moved.cache.only"
	// cacheOnlyClockShift moves the synctest clock (midnight) off every
	// archive boundary.
	cacheOnlyClockShift = 37*time.Hour + 1234*time.Second
)

// cacheOnlyRanges are the request spans of the four archives of
// cacheOnlySchema with the step a stored series answers them with.
var cacheOnlyRanges = []struct {
	name string
	span int
	step int
}{
	{"1h", 3600, 1},
	{"7d", 7 * 86400, 60},
	{"90d", 90 * 86400, 3600},
	{"2y", 2 * 365 * 86400, 86400},
}

var cacheOnlyMethods = []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.Min, whisper.Max, whisper.Last}

func fixedConfigRetriever(t testing.TB, schema string, method whisper.AggregationMethod, xff float64) *wspConfigTestRetriever {
	t.Helper()
	retentions, err := whisper.ParseRetentionDefs(schema)
	if err != nil {
		t.Fatal(err)
	}
	return &wspConfigTestRetriever{
		getRetentionsFunc: func(string) (whisper.Retentions, bool) { return retentions, true },
		getAggrConfFunc:   func(string) (whisper.AggregationMethod, float64, bool) { return method, xff, true },
	}
}

// newCacheOnlyListener serves cacheOnlyMetric from cached alone, with no
// stored series in dir.
func newCacheOnlyListener(t testing.TB, dir string, cached []points.Point, method whisper.AggregationMethod, xff float64) *CarbonserverListener {
	t.Helper()
	l := NewCarbonserverListener(func(name string) []points.Point {
		if name == cacheOnlyMetric {
			return cached
		}
		return nil
	})
	l.logger, l.accessLogger = zap.NewNop(), zap.NewNop()
	l.SetWhisperData(dir)
	l.SetCacheGetMetricsFunc(func() []map[string]struct{} { return nil })
	l.SetConfigRetriever(fixedConfigRetriever(t, cacheOnlySchema, method, xff))
	return l
}

// cacheOnlyWhisperPath is where the stored series of cacheOnlyMetric lives.
func cacheOnlyWhisperPath(t testing.TB, dir string) string {
	t.Helper()
	path := filepath.Join(dir, strings.ReplaceAll(cacheOnlyMetric, ".", "/")+".wsp")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// sameSeries requires bit-identical series and returns the number of present
// values.
func sameSeries(t *testing.T, label string, got, want response) int {
	t.Helper()
	if got.StartTime != want.StartTime || got.StopTime != want.StopTime || got.StepTime != want.StepTime {
		t.Fatalf("%s: got start=%d stop=%d step=%d, want start=%d stop=%d step=%d", label,
			got.StartTime, got.StopTime, got.StepTime, want.StartTime, want.StopTime, want.StepTime)
	}
	if got.ConsolidationFunc != want.ConsolidationFunc || got.XFilesFactor != want.XFilesFactor {
		t.Fatalf("%s: got consolidation %q/%v, want %q/%v", label, got.ConsolidationFunc, got.XFilesFactor, want.ConsolidationFunc, want.XFilesFactor)
	}
	if got.RequestStartTime != want.RequestStartTime || got.RequestStopTime != want.RequestStopTime {
		t.Fatalf("%s: got request range %d-%d, want %d-%d", label, got.RequestStartTime, got.RequestStopTime, want.RequestStartTime, want.RequestStopTime)
	}
	if len(got.Values) != len(want.Values) {
		t.Fatalf("%s: got %d values, want %d", label, len(got.Values), len(want.Values))
	}
	present := 0
	for i := range want.Values {
		g, w := got.Values[i], want.Values[i]
		if math.IsNaN(g) && math.IsNaN(w) {
			continue
		}
		if math.Float64bits(g) != math.Float64bits(w) {
			t.Fatalf("%s: value %d (ts %d) = %v (%#x), want %v (%#x)", label, i, want.StartTime+int64(i)*want.StepTime, g, math.Float64bits(g), w, math.Float64bits(w))
		}
		present++
	}
	return present
}

// A cache-only metric is answered at the step of the archive covering the
// range, aligned like a stored series, instead of at the finest step.
func TestCacheOnlyStepFollowsArchive(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(cacheOnlyClockShift)
		now := int(time.Now().Unix())
		cached := []points.Point{{Timestamp: int64(now - 90), Value: 1}, {Timestamp: int64(now - 30), Value: 2}}
		l := newCacheOnlyListener(t, t.TempDir(), cached, whisper.Average, 0)
		for _, tc := range cacheOnlyRanges {
			for _, edge := range []struct{ from, until int }{{0, 0}, {13, -7}} {
				from, until := now-tc.span+edge.from, now+edge.until
				r, err := l.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", int32(from), int32(until))
				if err != nil {
					t.Fatal(tc.name, err)
				}
				start, stop := interval(from, tc.step), interval(until, tc.step)
				if r.StepTime != int64(tc.step) || r.StartTime != int64(start) || r.StopTime != int64(stop) || len(r.Values) != (stop-start)/tc.step {
					t.Fatalf("%s %+v: got start=%d stop=%d step=%d values=%d, want %d %d %d %d", tc.name, edge,
						r.StartTime, r.StopTime, r.StepTime, len(r.Values), start, stop, tc.step, (stop-start)/tc.step)
				}
				if r.RequestStartTime != int64(from) || r.RequestStopTime != int64(until) || r.ConsolidationFunc != "Average" {
					t.Fatalf("%s: request range %d-%d consolidation %q", tc.name, r.RequestStartTime, r.RequestStopTime, r.ConsolidationFunc)
				}
			}
		}
		// Ranges a stored series cannot answer are refused the same way.
		for _, tc := range []struct{ from, until int }{{now + 10, now + 20}, {now - 11*365*86400, now - 10*365*86400 - 10}} {
			if _, err := l.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", int32(tc.from), int32(tc.until)); err == nil || err.Error() != "time range not found" {
				t.Fatalf("range %+v: err=%v, want time range not found", tc, err)
			}
		}
	})
}

// Coarser buckets hold the stored rollup: each archive aggregates the occupied
// slots of the finer one with the metric's method, gated by xFilesFactor.
func TestCacheOnlyAggregation(t *testing.T) {
	t.Parallel()
	nan := math.NaN()
	type want struct {
		minutes  [4]float64 // minutes A-D at the 7d step
		hour     float64    // at the 90d step
		day      float64    // at the 2y step
		consFunc string
	}
	cases := []struct {
		method whisper.AggregationMethod
		xff    float64
		want   want
	}{
		// A=[1,2,3] B=[10] C=[0..29] (30 of 60) D=[100..128] (29 of 60).
		// Minutes roll up into the hour, the hour into the day.
		{whisper.Average, 0, want{[4]float64{2, 10, 14.5, 114}, 35.125, 35.125, "Average"}},
		{whisper.Sum, 0, want{[4]float64{6, 10, 435, 3306}, 3757, 3757, "Sum"}},
		{whisper.Min, 0, want{[4]float64{1, 10, 0, 100}, 0, 0, "Min"}},
		{whisper.Max, 0, want{[4]float64{3, 10, 29, 128}, 128, 128, "Max"}},
		{whisper.Last, 0, want{[4]float64{3, 10, 29, 128}, 128, 128, "Last"}},
		// 3/60 passes 0.05, 1/60 does not; the hour has 3 of 60 minutes,
		// the day 1 of 24 hours.
		{whisper.Average, 0.05, want{[4]float64{2, nan, 14.5, 114}, 43.5, nan, "Average"}},
		{whisper.Sum, 0.05, want{[4]float64{6, nan, 435, 3306}, 3747, nan, "Sum"}},
		{whisper.Min, 0.05, want{[4]float64{1, nan, 0, 100}, 0, nan, "Min"}},
		{whisper.Max, 0.05, want{[4]float64{3, nan, 29, 128}, 128, nan, "Max"}},
		{whisper.Last, 0.05, want{[4]float64{3, nan, 29, 128}, 128, nan, "Last"}},
		// Only C reaches half its seconds; one minute of 60 fails the hour.
		{whisper.Average, 0.5, want{[4]float64{nan, nan, 14.5, nan}, nan, nan, "Average"}},
		{whisper.Sum, 0.5, want{[4]float64{nan, nan, 435, nan}, nan, nan, "Sum"}},
		{whisper.Min, 0.5, want{[4]float64{nan, nan, 0, nan}, nan, nan, "Min"}},
		{whisper.Max, 0.5, want{[4]float64{nan, nan, 29, nan}, nan, nan, "Max"}},
		{whisper.Last, 0.5, want{[4]float64{nan, nan, 29, nan}, nan, nan, "Last"}},
	}
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(cacheOnlyClockShift)
		now := int(time.Now().Unix())
		hour := now - now%3600 - 2*3600 // a complete hour inside the 1s archive
		a, b, c, d := hour, hour+60, hour+120, hour+180
		var cached []points.Point
		add := func(ts int, v float64) { cached = append(cached, points.Point{Timestamp: int64(ts), Value: v}) }
		add(b+5, 10)
		add(a+20, 3) // arrival order does not matter
		add(a, 1)
		add(a+10, 2)
		for i := range 30 {
			add(c+i, float64(i))
		}
		for i := range 29 {
			add(d+i, float64(100+i))
		}
		for _, tc := range cases {
			l := newCacheOnlyListener(t, t.TempDir(), cached, tc.method, tc.xff)
			label := fmt.Sprintf("%s xff=%v", tc.method, tc.xff)
			at := func(span int, ts int) (float64, response) {
				t.Helper()
				r, err := l.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", int32(now-span), int32(now))
				if err != nil {
					t.Fatal(label, err)
				}
				return r.Values[(int64(ts)-r.StartTime)/r.StepTime], r
			}
			same := func(what string, got, want float64) {
				t.Helper()
				if !(math.IsNaN(got) && math.IsNaN(want)) && got != want {
					t.Fatalf("%s %s = %v, want %v", label, what, got, want)
				}
			}
			for i, ts := range []int{a, b, c, d} {
				got, r := at(7*86400, ts)
				same(fmt.Sprintf("minute %c", 'A'+i), got, tc.want.minutes[i])
				if r.ConsolidationFunc != tc.want.consFunc || r.XFilesFactor != float32(tc.xff) {
					t.Fatalf("%s: consolidation %q xff %v", label, r.ConsolidationFunc, r.XFilesFactor)
				}
			}
			got, _ := at(90*86400, hour)
			same("hour", got, tc.want.hour)
			got, _ = at(2*365*86400, hour-hour%86400)
			same("day", got, tc.want.day)
			// The finest archive keeps every cached second.
			got, r := at(4*3600, c+7)
			same("second", got, 7)
			if r.StepTime != 1 {
				t.Fatalf("%s: 4h step %d", label, r.StepTime)
			}
		}
	})
}

// Within one slot the stored series keeps the point with the latest timestamp,
// and the later of equal timestamps, whatever order the points arrived in.
func TestCacheOnlySlotOwner(t *testing.T) {
	t.Parallel()
	retentions, err := whisper.ParseRetentionDefs("1m:1d,1h:30d")
	if err != nil {
		t.Fatal(err)
	}
	const now = 1_700_000_000
	minute := now - now%60 - 600
	cached := []points.Point{
		{Timestamp: int64(minute + 30), Value: 5},
		{Timestamp: int64(minute + 10), Value: 7}, // older timestamp, later arrival
		{Timestamp: int64(minute + 120), Value: 1},
		{Timestamp: int64(minute + 120), Value: 2}, // duplicate: later arrival wins
	}
	s, err := cacheOnlySeries(cached, retentions, whisper.Last, 0, now-3600, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.values[(minute-s.from)/s.step]; got != 5 {
		t.Fatalf("out-of-order slot = %v, want 5", got)
	}
	if got := s.values[(minute+120-s.from)/s.step]; got != 2 {
		t.Fatalf("duplicate slot = %v, want 2", got)
	}
	if s.cached != len(cached) {
		t.Fatalf("cached points in range = %d, want %d", s.cached, len(cached))
	}
}

// A stored series with newer cached points keeps its existing answer: the
// cache is merged at the finest step only, last point winning.
func TestCacheOnlyMixedStoredAndCacheUnchanged(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(cacheOnlyClockShift)
		now := int(time.Now().Unix())
		dir := t.TempDir()
		retentions, err := whisper.ParseRetentionDefs(cacheOnlySchema)
		if err != nil {
			t.Fatal(err)
		}
		w, err := whisper.Create(cacheOnlyWhisperPath(t, dir), retentions, whisper.Sum, 0)
		if err != nil {
			t.Fatal(err)
		}
		var stored []*whisper.TimeSeriesPoint
		for ts := now - 1800; ts < now-600; ts += 7 {
			stored = append(stored, &whisper.TimeSeriesPoint{Time: ts, Value: float64(ts % 97)})
		}
		if err := w.UpdateMany(stored); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		cached := []points.Point{{Timestamp: int64(now - 300), Value: 1}, {Timestamp: int64(now - 299), Value: 2}, {Timestamp: int64(now - 300), Value: 3}}
		l := newCacheOnlyListener(t, dir, cached, whisper.Average, 0.5)
		for _, tc := range cacheOnlyRanges {
			from := now - tc.span
			got, err := l.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", int32(from), int32(now))
			if err != nil {
				t.Fatal(tc.name, err)
			}
			// Oracle: the stored archive, overlaid by the cache when that
			// archive is the finest one.
			w, err := whisper.Open(cacheOnlyWhisperPath(t, dir))
			if err != nil {
				t.Fatal(err)
			}
			ts, err := w.Fetch(from, now)
			w.Close()
			if err != nil {
				t.Fatal(err)
			}
			want := response{StartTime: int64(ts.FromTime()), StopTime: int64(ts.UntilTime()), StepTime: int64(ts.Step()), Values: ts.Values(),
				ConsolidationFunc: "Sum", RequestStartTime: int64(from), RequestStopTime: int64(now)}
			if tc.step == 1 {
				for _, p := range cached {
					want.Values[(p.Timestamp-want.StartTime)/want.StepTime] = p.Value
				}
			}
			if want.StepTime != int64(tc.step) {
				t.Fatalf("%s: stored step %d", tc.name, want.StepTime)
			}
			sameSeries(t, "mixed "+tc.name, got, want)
		}
	})
}

// cacheOnlyFixture is a cache made of a long-lived metric's recent points:
// dense recent seconds, sparse days, some points older than the 1s archive
// (written straight to the 1m one), duplicates, and out-of-order arrival.
func cacheOnlyFixture(now int) []points.Point {
	rng := rand.New(rand.NewSource(int64(now)))
	var pts []points.Point
	add := func(ts int) {
		v := float64(rng.Intn(1_000_000))/7 - 30000
		pts = append(pts, points.Point{Timestamp: int64(ts), Value: v})
	}
	for ts := now - 2*3600; ts <= now; ts += 1 + rng.Intn(3) {
		add(ts)
	}
	for ts := now - 4*86400; ts < now-2*3600; ts += 20 + rng.Intn(40) {
		add(ts)
	}
	for ts := now - 86400 - 1800; ts < now-86400+600; ts++ { // across the 1s retention edge
		if rng.Intn(4) != 0 {
			add(ts)
		}
	}
	for range 50 { // duplicates of recent timestamps with new values
		add(int(pts[rng.Intn(len(pts))].Timestamp))
	}
	// Late points (older than their neighbours) arrive at random positions.
	rng.Shuffle(len(pts)/10, func(i, j int) { pts[i*10], pts[j*10] = pts[j*10], pts[i*10] })
	return pts
}

type cacheOnlyRequest struct{ from, until int }

func cacheOnlyRequests(now int) []cacheOnlyRequest {
	var requests []cacheOnlyRequest
	for _, tc := range cacheOnlyRanges {
		requests = append(requests, cacheOnlyRequest{now - tc.span, now}, cacheOnlyRequest{now - tc.span + 17, now - 5})
	}
	return requests
}

// Write the same points into each stored backend and require the cache-only
// answer to equal the stored one value for value, bit for bit.
func TestCacheOnlyMatchesStoredWhisper(t *testing.T) {
	t.Parallel()
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%v", compressed), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				time.Sleep(cacheOnlyClockShift)
				now := int(time.Now().Unix())
				cached := cacheOnlyFixture(now)
				retentions, err := whisper.ParseRetentionDefs(cacheOnlySchema)
				if err != nil {
					t.Fatal(err)
				}
				for _, method := range cacheOnlyMethods {
					for _, xff := range []float64{0, 0.02, 0.5} {
						label := fmt.Sprintf("%s xff=%v", method, xff)
						cacheOnly := newCacheOnlyListener(t, t.TempDir(), cached, method, xff)
						dir := t.TempDir()
						w, err := whisper.CreateWithOptions(cacheOnlyWhisperPath(t, dir), retentions, method, float32(xff),
							&whisper.Options{Compressed: compressed, OutOfOrder: compressed, Sparse: true})
						if err != nil {
							t.Fatal(err)
						}
						batch := make([]*whisper.TimeSeriesPoint, len(cached))
						for i, p := range cached {
							batch[i] = &whisper.TimeSeriesPoint{Time: int(p.Timestamp), Value: p.Value}
						}
						if err := w.UpdateMany(batch); err != nil {
							t.Fatal(err)
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
						stored := newCacheOnlyListener(t, dir, nil, method, xff)
						for _, req := range cacheOnlyRequests(now) {
							want, err := stored.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", int32(req.from), int32(req.until))
							if err != nil {
								t.Fatal(label, err)
							}
							got, err := cacheOnly.fetchSingleMetric(context.Background(), cacheOnlyMetric, "", int32(req.from), int32(req.until))
							if err != nil {
								t.Fatal(label, err)
							}
							if n := sameSeries(t, fmt.Sprintf("%s %+v", label, req), got, want); n == 0 && xff == 0 {
								t.Fatalf("%s %+v: no values compared", label, req)
							}
						}
					}
				}
			})
		})
	}
}

func TestCacheOnlyMatchesSharedStore(t *testing.T) {
	t.Parallel()
	const now = 1_700_000_000 + 1234
	for _, mode := range []string{"chunks", "exact", "continuation"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			options := store.Options{Now: func() time.Time { return time.Unix(now, 0) }}
			switch mode {
			case "exact":
				options.ExactBlocks = true
			case "continuation":
				options.ExactBlocks, options.TimeBuckets, options.Continuation = true, true, &whisperio.ContinuationCodec{}
			}
			db, err := store.Open(t.TempDir(), options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			retentions, err := whisper.ParseRetentionDefs(cacheOnlySchema)
			if err != nil {
				t.Fatal(err)
			}
			configured := make([]store.Retention, len(retentions))
			for i, r := range retentions {
				configured[i] = store.Retention{Step: r.SecondsPerPoint(), Count: r.NumberOfPoints()}
			}
			cached := cacheOnlyFixture(now)
			input := make([]store.Point, len(cached))
			for i, p := range cached {
				input[i] = store.Point{Timestamp: p.Timestamp, Value: p.Value}
			}
			for _, method := range cacheOnlyMethods {
				for _, xff := range []float64{0, 0.02, 0.5} {
					label := fmt.Sprintf("%s xff=%v", method, xff)
					name := fmt.Sprintf("%s.%s.x%v", cacheOnlyMetric, method, xff*100)
					cache := []points.Point(nil)
					l := NewCarbonserverListener(func(string) []points.Point { return cache })
					l.logger, l.accessLogger = zap.NewNop(), zap.NewNop()
					l.SetWhisperData(t.TempDir())
					l.SetCacheGetMetricsFunc(func() []map[string]struct{} { return nil })
					l.SetConfigRetriever(fixedConfigRetriever(t, cacheOnlySchema, method, xff))
					l.SetMetricStore(db)

					// Not in the store yet: answered from the cache.
					cache = cached
					var cacheOnly []response
					for _, req := range cacheOnlyRequests(now) {
						r, err := l.fetchSingleMetric(context.Background(), name, "", int32(req.from), int32(req.until))
						if err != nil {
							t.Fatal(label, err)
						}
						cacheOnly = append(cacheOnly, r)
					}
					cache = nil
					if _, err := db.Create(ctx, store.MetricConfig{Name: name, Retentions: configured,
						AggregationMethod: store.AggregationMethod(method), XFilesFactor: float32(xff)}); err != nil {
						t.Fatal(err)
					}
					if err := db.UpdateMany(ctx, name, input); err != nil {
						t.Fatal(err)
					}
					for i, req := range cacheOnlyRequests(now) {
						want, err := l.fetchSingleMetric(context.Background(), name, "", int32(req.from), int32(req.until))
						if err != nil {
							t.Fatal(label, err)
						}
						if n := sameSeries(t, fmt.Sprintf("%s %+v", label, req), cacheOnly[i], want); n == 0 && xff == 0 {
							t.Fatalf("%s %+v: no values compared", label, req)
						}
					}
				}
			}
		})
	}
}

// renderTestStream collects a gRPC render stream.
type renderTestStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*protov2.FetchResponse
}

func (s *renderTestStream) Context() context.Context { return s.ctx }
func (s *renderTestStream) Send(r *protov2.FetchResponse) error {
	s.responses = append(s.responses, r)
	return nil
}

// Every render entry point reaches the cache-only answer: HTTP json,
// protobuf (v2), carbonapi_v3_pb multi-fetch and pickle, and the gRPC stream.
func TestCacheOnlyRenderEntryPoints(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(cacheOnlyClockShift)
		now := int(time.Now().Unix())
		minute := now - now%60 - 120
		cached := []points.Point{{Timestamp: int64(minute + 1), Value: 4}, {Timestamp: int64(minute + 2), Value: 8}}
		l := newCacheOnlyListener(t, t.TempDir(), cached, whisper.Average, 0)
		l.SetMaxGlobs(10)
		l.SetMaxMetricsRendered(10)
		l.SetTrieIndex(true)
		l.SetTrigramIndex(false)
		l.SetConcurrentIndex(true)
		l.SetRealtimeIndex(10)
		trie := newTrie(".wsp", 0, nil)
		l.UpdateFileIndex(&fileIndex{trieIdx: trie})
		l.insertRealtimeMetric(trie, cacheOnlyMetric)
		from, until := now-7*86400, now
		want := response{StartTime: int64(interval(from, 60)), StopTime: int64(interval(until, 60)), StepTime: 60}
		check := func(format string, name string, start, stop, step int64, values []float64) {
			t.Helper()
			if name != cacheOnlyMetric || start != want.StartTime || stop != want.StopTime || step != want.StepTime ||
				len(values) != int((stop-start)/step) || values[(int64(minute)-start)/step] != 6 {
				t.Fatalf("%s: name=%q start=%d stop=%d step=%d values=%d, want start=%d stop=%d step=60 and 6 at %d",
					format, name, start, stop, step, len(values), want.StartTime, want.StopTime, minute)
			}
		}
		render := func(format string, body []byte) []byte {
			t.Helper()
			q := url.Values{"format": {format}, "target": {cacheOnlyMetric}, "from": {fmt.Sprint(from)}, "until": {fmt.Sprint(until)}}
			method := http.MethodGet
			if body != nil {
				method = http.MethodPost
			}
			req := httptest.NewRequest(method, "/render/?"+q.Encode(), bytes.NewReader(body))
			rec := httptest.NewRecorder()
			l.renderHandler(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: status %d: %s", format, rec.Code, rec.Body)
			}
			return rec.Body.Bytes()
		}

		for _, format := range []string{"protobuf", "carbonapi_v2_pb", "json"} {
			var m protov2.MultiFetchResponse
			raw := render(format, nil)
			var err error
			if format == "json" {
				err = protojson.Unmarshal(raw, &m)
			} else {
				err = m.UnmarshalVT(raw)
			}
			if err != nil || len(m.Metrics) != 1 {
				t.Fatalf("%s: %v %d", format, err, len(m.Metrics))
			}
			r := m.Metrics[0]
			check(format, r.Name, int64(r.StartTime), int64(r.StopTime), int64(r.StepTime), r.Values)
		}

		body, err := (&protov3.MultiFetchRequest{Metrics: []*protov3.FetchRequest{{Name: cacheOnlyMetric, PathExpression: cacheOnlyMetric,
			StartTime: int64(from), StopTime: int64(until)}}}).MarshalVT()
		if err != nil {
			t.Fatal(err)
		}
		var m3 protov3.MultiFetchResponse
		if err := m3.UnmarshalVT(render("carbonapi_v3_pb", body)); err != nil || len(m3.Metrics) != 1 {
			t.Fatalf("carbonapi_v3_pb: %v %d", err, len(m3.Metrics))
		}
		r3 := m3.Metrics[0]
		check("carbonapi_v3_pb", r3.Name, r3.StartTime, r3.StopTime, r3.StepTime, r3.Values)
		if r3.ConsolidationFunc != "Average" || r3.RequestStartTime != int64(from) || r3.RequestStopTime != int64(until) {
			t.Fatalf("carbonapi_v3_pb metadata: %q %d %d", r3.ConsolidationFunc, r3.RequestStartTime, r3.RequestStopTime)
		}

		decoded, err := pickle.NewDecoder(bytes.NewReader(render("pickle", nil))).Decode()
		if err != nil {
			t.Fatal(err)
		}
		list, ok := decoded.([]interface{})
		if !ok || len(list) != 1 {
			t.Fatalf("pickle: %#v", decoded)
		}
		pm := list[0].(map[interface{}]interface{})
		pv := pm["values"].([]interface{})
		values := make([]float64, len(pv))
		for i, v := range pv {
			if f, ok := v.(float64); ok {
				values[i] = f
			} else {
				values[i] = math.NaN()
			}
		}
		check("pickle", pm["name"].(string), pm["start"].(int64), pm["end"].(int64), pm["step"].(int64), values)

		stream := &renderTestStream{ctx: context.Background()}
		if err := l.Render(&protov2.MultiFetchRequest{Metrics: []*protov2.FetchRequest{{Name: cacheOnlyMetric, StartTime: int32(from), StopTime: int32(until)}}}, stream); err != nil {
			t.Fatal(err)
		}
		if len(stream.responses) != 1 {
			t.Fatalf("grpc stream: %d responses", len(stream.responses))
		}
		rs := stream.responses[0]
		check("grpc", rs.Name, int64(rs.StartTime), int64(rs.StopTime), int64(rs.StepTime), rs.Values)
	})
}
