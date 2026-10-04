package persister

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"
)

const storageEpoch = 1700006400 // Divisible by every step used by these tests.

var storageBackends = []string{"classic", "cwhisper", "cwhisper-ooo"}

// Whisper exposes a process-wide clock. Keep these tests and benchmarks serial;
// the atomic clock also permits concurrent storage operations at a fixed epoch.
func storageTestClock(tb testing.TB) *atomic.Int64 {
	tb.Helper()
	now := new(atomic.Int64)
	now.Store(storageEpoch)
	previous := whisper.Now
	whisper.Now = func() time.Time { return time.Unix(now.Load(), 0) }
	tb.Cleanup(func() { whisper.Now = previous })
	return now
}

// Files follow the persister's open/update/close lifecycle.
type storageBackend struct {
	kind string
	dir  string
	now  *atomic.Int64
}

func newStorageBackend(tb testing.TB, kind string, now *atomic.Int64) *storageBackend {
	tb.Helper()
	s := &storageBackend{kind: kind, dir: tb.TempDir(), now: now}
	storageMust(tb, s.open())
	tb.Cleanup(func() { storageMust(tb, s.close()) })
	return s
}

func (*storageBackend) open() error  { return nil }
func (*storageBackend) close() error { return nil }

func (s *storageBackend) reopen() error {
	if err := s.close(); err != nil {
		return err
	}
	return s.open()
}

func (s *storageBackend) options() *whisper.Options {
	return &whisper.Options{Sparse: true, FLock: true,
		Compressed: s.kind != "classic", OutOfOrder: s.kind == "cwhisper-ooo"}
}

func (s *storageBackend) path(name string) string {
	return filepath.Join(s.dir, name+".wsp")
}

type storageMetricConfig struct {
	Name              string
	Retentions        []whisper.Retention
	AggregationMethod whisper.AggregationMethod
	XFilesFactor      float32
}

func storageConfig(name, retentions string, method whisper.AggregationMethod, xff float32) storageMetricConfig {
	c := storageMetricConfig{Name: name, AggregationMethod: method, XFilesFactor: xff}
	for _, r := range whisper.MustParseRetentionDefs(retentions) {
		c.Retentions = append(c.Retentions, *r)
	}
	return c
}

func (s *storageBackend) create(c storageMetricConfig) error {
	w, err := whisper.CreateWithOptions(s.path(c.Name), whisper.NewRetentionsNoPointer(c.Retentions), c.AggregationMethod, c.XFilesFactor, s.options())
	if err != nil {
		return err
	}
	return w.Close()
}

func (s *storageBackend) update(name string, input []whisper.TimeSeriesPoint) error {
	// Each engine gets the original input order, including duplicates, even if
	// its implementation mutates the slice while sorting or aligning points.
	values := slices.Clone(input)
	w, err := whisper.OpenWithOptions(s.path(name), s.options())
	if err != nil {
		return err
	}
	ptrs := make([]*whisper.TimeSeriesPoint, len(values))
	for i := range values {
		ptrs[i] = &values[i]
	}
	return errors.Join(w.UpdateMany(ptrs), w.Close())
}

type storageSeries struct {
	from, until, step int
	values            []float64
}

func (s *storageBackend) fetch(name string, from, until int) (*storageSeries, error) {
	w, err := whisper.OpenWithOptions(s.path(name), s.options())
	if err != nil {
		return nil, err
	}
	v, err := w.Fetch(from, until)
	err = errors.Join(err, w.Close())
	if err != nil || v == nil {
		return nil, err
	}
	return &storageSeries{v.FromTime(), v.UntilTime(), v.Step(), v.Values()}, nil
}

func (s *storageBackend) metadata(name string) (storageMetricConfig, error) {
	w, err := whisper.OpenWithOptions(s.path(name), s.options())
	if err != nil {
		return storageMetricConfig{}, err
	}
	c := storageMetricConfig{Name: name, Retentions: w.Retentions(), AggregationMethod: w.AggregationMethod(), XFilesFactor: w.XFilesFactor()}
	return c, w.Close()
}

func (s *storageBackend) compact(names []string) error {
	if s.kind != "cwhisper-ooo" {
		return nil
	}
	for _, name := range names {
		w, err := whisper.OpenWithOptions(s.path(name), s.options())
		if err != nil {
			return err
		}
		if err := errors.Join(w.MergeOutOfOrder(), w.Close()); err != nil {
			return err
		}
	}
	return nil
}

func storageMust(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

func storageSeriesDiff(want, got *storageSeries) string {
	if want == nil || got == nil {
		if want != got {
			return fmt.Sprintf("series presence: classic=%v candidate=%v", want != nil, got != nil)
		}
		return ""
	}
	if want.from != got.from || want.until != got.until || want.step != got.step || len(want.values) != len(got.values) {
		return fmt.Sprintf("grid: classic=(%d,%d,%d,%d) candidate=(%d,%d,%d,%d)", want.from, want.until, want.step, len(want.values), got.from, got.until, got.step, len(got.values))
	}
	for i, v := range want.values {
		actual := got.values[i]
		// Compression must preserve finite values bit for bit, including -0.
		// Missing slots are NaNs; their payload is not part of Fetch's contract.
		if math.IsNaN(v) && math.IsNaN(actual) {
			continue
		}
		if math.Float64bits(v) != math.Float64bits(actual) {
			return fmt.Sprintf("timestamp=%d step=%d classic=%g (%016x) candidate=%g (%016x)", want.from+i*want.step, want.step, v, math.Float64bits(v), actual, math.Float64bits(actual))
		}
	}
	return ""
}

func storageCompare(tb testing.TB, oracle, candidate *storageBackend, c storageMetricConfig, stage string) {
	tb.Helper()
	now := int(oracle.now.Load())
	queries := [][2]int{{now + 1, now + 10}, {now, now - 1}}
	for _, r := range c.Retentions {
		age, step := r.MaxRetention(), r.SecondsPerPoint()
		queries = append(queries,
			[2]int{now - age, now}, [2]int{now - age + 1, now - step},
			[2]int{now - age - 1, now},
			[2]int{now - age, now + step})
	}
	for _, q := range queries {
		want, wantErr := oracle.fetch(c.Name, q[0], q[1])
		got, gotErr := candidate.fetch(c.Name, q[0], q[1])
		if (wantErr == nil) != (gotErr == nil) {
			tb.Fatalf("%s %s query=%v: classic error=%v candidate error=%v", candidate.kind, stage, q, wantErr, gotErr)
		}
		if wantErr != nil {
			// Only deliberately invalid intervals may fail on both engines.
			if q[0] <= q[1] {
				tb.Fatalf("%s query=%v: classic=%v candidate=%v", stage, q, wantErr, gotErr)
			}
			continue
		}
		if diff := storageSeriesDiff(want, got); diff != "" {
			tb.Fatalf("%s %s metric=%s query=%v: %s", candidate.kind, stage, c.Name, q, diff)
		}
	}
	m, err := candidate.metadata(c.Name)
	storageMust(tb, err)
	if m.Name != c.Name || m.AggregationMethod != c.AggregationMethod || m.XFilesFactor != c.XFilesFactor || !whisper.NewRetentionsNoPointer(m.Retentions).Equal(whisper.NewRetentionsNoPointer(c.Retentions)) {
		tb.Fatalf("%s %s metadata: got=%+v want=%+v", candidate.kind, stage, m, c)
	}
}

func storageCompareStage(t *testing.T, oracle, candidate *storageBackend, c storageMetricConfig, stage string) {
	t.Helper()
	// A fetch mismatch must not prevent later writes/restarts/compactions from
	// running: those stages may reveal a different persistence failure.
	t.Run(stage, func(t *testing.T) { storageCompare(t, oracle, candidate, c, stage) })
}

type storageWrite struct {
	advance int
	points  []whisper.TimeSeriesPoint
}

type storageTrace struct {
	name       string
	retentions string
	late       bool
	writes     []storageWrite
}

func storagePoints(start, count, step int) []whisper.TimeSeriesPoint {
	points := make([]whisper.TimeSeriesPoint, count)
	for i := range points {
		points[i] = whisper.TimeSeriesPoint{Time: start + i*step, Value: float64((i*17)%31 - 15)}
	}
	return points
}

func storageTraces() []storageTrace {
	dense := storagePoints(storageEpoch-180, 180, 1)
	ordered := storageTrace{name: "ordered", retentions: "1s:5m,10s:1h,60s:6h"}
	for i := 0; i < len(dense); i += 30 {
		ordered.writes = append(ordered.writes, storageWrite{points: dense[i : i+30]})
	}
	shuffled := slices.Clone(dense)
	rand.New(rand.NewSource(4271)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	late := ordered
	late.name, late.late, late.writes, late.retentions = "late-holes", true, nil, "1s:5m"
	var holes, primary []whisper.TimeSeriesPoint
	for i, p := range dense {
		if i%13 == 0 {
			holes = append(holes, p)
		} else {
			primary = append(primary, p)
		}
	}
	late.writes = []storageWrite{{points: primary}, {points: holes}}
	lateRollup := late
	lateRollup.name, lateRollup.retentions = "late-holes-rollup", ordered.retentions
	corrections := ordered
	corrections.name, corrections.late = "corrections-and-retry", true
	corrections.retentions = "1s:5m"
	corrections.writes = []storageWrite{{points: dense}, {points: []whisper.TimeSeriesPoint{
		{Time: storageEpoch - 170, Value: 101}, {Time: storageEpoch - 169, Value: -203},
	}}, {points: []whisper.TimeSeriesPoint{{Time: storageEpoch - 170, Value: 101}, {Time: storageEpoch - 169, Value: -203}}}}
	correctionsRollup := corrections
	correctionsRollup.name, correctionsRollup.retentions = "corrections-rollup", ordered.retentions
	wrap := storageTrace{name: "ring-wrap", retentions: "1s:1m,10s:10m,60s:1h"}
	for i := 0; i < 8; i++ {
		wrap.writes = append(wrap.writes, storageWrite{advance: 30, points: storagePoints(storageEpoch+i*30, 30, 1)})
	}
	singleWrap := wrap
	singleWrap.name, singleWrap.retentions = "ring-wrap-single", "1s:1m"
	return []storageTrace{
		{name: "empty", retentions: ordered.retentions}, ordered,
		{name: "shuffled-single-batch", retentions: ordered.retentions, writes: []storageWrite{{points: shuffled}}},
		{name: "duplicate-in-batch", retentions: "10s:1h,60s:6h", writes: []storageWrite{{points: []whisper.TimeSeriesPoint{
			{Time: storageEpoch - 90, Value: 1}, {Time: storageEpoch - 90, Value: 2},
			{Time: storageEpoch - 89, Value: 3}, {Time: storageEpoch - 80, Value: 4},
		}}}},
		{name: "sparse-xff", retentions: ordered.retentions, writes: []storageWrite{
			{points: storagePoints(storageEpoch-180, 4, 1)},
			{points: storagePoints(storageEpoch-170, 5, 1)},
			{points: storagePoints(storageEpoch-160, 6, 1)},
			{points: storagePoints(storageEpoch-150, 10, 1)},
		}},
		late, lateRollup, corrections, correctionsRollup,
		{name: "historical-coarse-correction", retentions: "1s:1m,10s:10m,60s:1h", late: true, writes: []storageWrite{
			{points: storagePoints(storageEpoch-540, 42, 10)},
			{points: []whisper.TimeSeriesPoint{{Time: storageEpoch - 480, Value: 123}}},
		}},
		{name: "retention-boundaries", retentions: "1s:1m,10s:10m,60s:1h", writes: []storageWrite{{points: []whisper.TimeSeriesPoint{
			{Time: storageEpoch - 3601, Value: 1}, {Time: storageEpoch - 3600, Value: 2},
			{Time: storageEpoch - 601, Value: 3}, {Time: storageEpoch - 600, Value: 4},
			{Time: storageEpoch - 61, Value: 5}, {Time: storageEpoch - 60, Value: 6},
			{Time: storageEpoch - 59, Value: 7}, {Time: storageEpoch, Value: 8},
			{Time: storageEpoch + 1, Value: 9},
		}}}}, wrap, singleWrap,
		{name: "ring-wrap-partial-block", retentions: "1s:10m", writes: []storageWrite{
			{points: storagePoints(storageEpoch-600, 600, 1)},
			{advance: 8, points: storagePoints(storageEpoch, 8, 1)},
		}},
		{name: "expiry-and-future", retentions: "1s:1m", writes: []storageWrite{
			{points: []whisper.TimeSeriesPoint{{Time: storageEpoch - 61, Value: 1}, {Time: storageEpoch - 60, Value: 2}, {Time: storageEpoch - 1, Value: 3}, {Time: storageEpoch + 1, Value: 4}}},
			{advance: 2}, {advance: 120},
		}},
		{name: "float-bits", retentions: "1s:5m", writes: []storageWrite{{points: []whisper.TimeSeriesPoint{
			{Time: storageEpoch - 10, Value: math.Copysign(0, -1)},
			{Time: storageEpoch - 9, Value: math.SmallestNonzeroFloat64},
			{Time: storageEpoch - 8, Value: math.MaxFloat64},
			{Time: storageEpoch - 7, Value: math.Inf(1)},
			{Time: storageEpoch - 6, Value: math.Inf(-1)},
			{Time: storageEpoch - 5, Value: math.NaN()},
			{Time: storageEpoch - 4, Value: math.Nextafter(1, 2)},
		}}}},
	}
}

// TestStorageParity is deliberately strict for OOO and Pebble. A difference is
// a regression to investigate, not an automatically accepted engine exception.
func TestStorageParity(t *testing.T) {
	now := storageTestClock(t)
	for _, kind := range storageBackends[1:] {
		t.Run(kind, func(t *testing.T) {
			for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.Last, whisper.Max, whisper.Min, whisper.First} {
				for _, xff := range []float32{0, 0.5, 1} {
					t.Run(fmt.Sprintf("%s/xff=%g", method, xff), func(t *testing.T) {
						for _, trace := range storageTraces() {
							t.Run(trace.name, func(t *testing.T) {
								if kind == "cwhisper" && trace.late {
									t.Skip("plain cwhisper does not support late writes; covered by TestStorageCWhisperLateLimitation")
								}
								now.Store(storageEpoch)
								oracle := newStorageBackend(t, "classic", now)
								candidate := newStorageBackend(t, kind, now)
								c := storageConfig("metric", trace.retentions, method, xff)
								storageMust(t, oracle.create(c))
								storageMust(t, candidate.create(c))
								storageCompareStage(t, oracle, candidate, c, "empty")
								for i, w := range trace.writes {
									now.Add(int64(w.advance))
									storageMust(t, oracle.update(c.Name, w.points))
									storageMust(t, candidate.update(c.Name, w.points))
									storageCompareStage(t, oracle, candidate, c, fmt.Sprintf("write=%d", i))
								}
								storageMust(t, candidate.reopen())
								storageCompareStage(t, oracle, candidate, c, "reopened")
								storageMust(t, candidate.compact([]string{c.Name}))
								storageMust(t, candidate.reopen())
								storageCompareStage(t, oracle, candidate, c, "compacted-and-reopened")
							})
						}
					})
				}
			}
		})
	}
}

func TestStorageCWhisperLateLimitation(t *testing.T) {
	now := storageTestClock(t)
	c := storageConfig("metric", "1s:5m", whisper.Average, 0.5)
	for _, kind := range storageBackends {
		t.Run(kind, func(t *testing.T) {
			s := newStorageBackend(t, kind, now)
			storageMust(t, s.create(c))
			storageMust(t, s.update(c.Name, storagePoints(storageEpoch-100, 80, 1)))
			late := storageEpoch - 110
			storageMust(t, s.update(c.Name, []whisper.TimeSeriesPoint{{Time: late, Value: 42}}))
			storageMust(t, s.reopen())
			v, err := s.fetch(c.Name, late-1, late)
			storageMust(t, err)
			if v == nil || len(v.values) != 1 {
				t.Fatalf("missing query grid: %+v", v)
			}
			if kind == "cwhisper" {
				if !math.IsNaN(v.values[0]) {
					t.Fatalf("known late-write limitation changed: got %g; revisit parity exclusions", v.values[0])
				}
			} else if v.values[0] != 42 {
				t.Fatalf("lost late point: %g", v.values[0])
			}
			if kind == "cwhisper-ooo" {
				if _, err := os.Stat(whisper.OutOfOrderSidecarPath(s.path(c.Name))); err != nil {
					t.Fatalf("workload did not exercise the sidecar: %v", err)
				}
				storageMust(t, s.compact([]string{c.Name}))
				if _, err := os.Stat(whisper.OutOfOrderSidecarPath(s.path(c.Name))); !os.IsNotExist(err) {
					t.Fatalf("compaction left sidecar: %v", err)
				}
				after, err := s.fetch(c.Name, late-1, late)
				storageMust(t, err)
				if !reflect.DeepEqual(after, v) {
					t.Fatalf("compaction changed late point: before=%+v after=%+v", v, after)
				}
			}
		})
	}
}
