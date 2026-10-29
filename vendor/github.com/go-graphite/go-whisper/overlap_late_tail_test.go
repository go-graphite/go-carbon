package whisper

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

// miscBatches emits the a sparse late-emitter workload: a 26 s emitter (about
// 37 points per 960 s writeout), with every lateEvery'th batch also carrying
// points 2-30 minutes late, behind the latest base slot already written.
func miscBatches(rng *rand.Rand, start, batches, lateEvery int) (flushAt []int, out [][]*TimeSeriesPoint) {
	ts := start
	for b := 0; b < batches; b++ {
		end := ts + 960
		var batch []*TimeSeriesPoint
		for t := ts; t < end; t += 26 {
			batch = append(batch, &TimeSeriesPoint{Time: t, Value: float64(rng.Intn(1000))})
		}
		if lateEvery > 0 && b%lateEvery == 0 {
			for i := 0; i < 3; i++ {
				batch = append(batch, &TimeSeriesPoint{Time: ts - 120 - rng.Intn(1680), Value: float64(rng.Intn(1000))})
			}
		}
		flushAt = append(flushAt, end)
		out = append(out, batch)
		ts = end
	}
	return flushAt, out
}

// TestOverlapLateBatchesTailCorrect reproduces the sparse late-emitter replay loop:
// a replay encodes an unfinished hour, the hour completes behind the hourly
// watermark, and the next batch carries late points. Such batches declined the
// tail correction and replayed the whole file, encoding the next unfinished
// hour, so the metric replayed every hour indefinitely.
func TestOverlapLateBatchesTailCorrect(t *testing.T) {
	pt := func(ts int, v float64) *TimeSeriesPoint { return &TimeSeriesPoint{Time: ts, Value: v} }
	const now = 1_700_000_040
	for _, lateEvery := range []int{1, 4} {
		f := newShapeFile(t, "1m:1d,1h:7d,1d:30d", Average, 0)
		f.prefill("1m:1d,1h:7d,1d:30d", now)
		if r := f.write(now+5, []*TimeSeriesPoint{pt(now, 1), pt(now+30, 2)}); r.kind != OverlapFuture {
			t.Fatalf("seed replay: %v", r.kind)
		}
		before := overlapUpdates[OverlapBufferedRollup].Load()
		flushAt, batches := miscBatches(rand.New(rand.NewSource(int64(lateEvery))), now+25, 24, lateEvery)
		for i, b := range batches {
			if r := f.write(flushAt[i], b); r.kind != OverlapNone {
				t.Fatalf("late every %d: batch %d replayed (%v)", lateEvery, i, r.kind)
			}
		}
		if n := overlapUpdates[OverlapBufferedRollup].Load() - before; n != 0 {
			t.Fatalf("late every %d: %d buffered_rollup replays", lateEvery, n)
		}
	}
}

// TestOverlapLateTailDifferential runs the digest scenarios and the sparse shape
// in lockstep on three files: one with the late tail and propagated rollup
// corrections, one with the previous rules, and a classic (uncompressed) file,
// the semantics being preserved. After every update, and at the end after
// folding each sidecar into its main file, it compares every archive's Fetch,
// main file and sidecar merged as readers see them.
//
// The previous rules are not an exact oracle: their replays demote finished
// rollups into the sidecar, where they only fill holes. So the corrected file
// must match classic wherever the previous rules did, archive by archive at
// every step. Late points now follow the sidecar path of every other file;
// where the sidecar outlives the base retention, the read-time recompute can
// differ from classic in windows whose base slots have since been reused.
func TestOverlapLateTailDifferential(t *testing.T) {
	type scenario struct {
		name string
		run  func(f *lockstepFile)
	}
	var scenarios []scenario
	for i, sc := range digestScenarios() {
		if testing.Short() && i%3 != 0 {
			continue
		}
		sc, seed := sc, int64(i)
		scenarios = append(scenarios, scenario{name: fmt.Sprintf("%s %+v method=%d xff=%v", sc.schema, sc.e, sc.method, sc.xff), run: func(f *lockstepFile) {
			f.create(sc.schema, sc.method, sc.xff)
			f.prefill(sc.schema)
			e := sc.e
			e.minPoints, e.maxDelay = 960, 960
			duration := sc.days * 86400
			if duration == 0 {
				duration = 4 * 3600
			}
			flushAt, batches := e.batches(e.arrivals(rand.New(rand.NewSource(seed)), f.start, duration))
			for i, b := range batches {
				f.write(flushAt[i], b)
			}
		}})
	}
	for _, schema := range []string{"1m:1d,1h:7d,1d:30d", "10s:6h,1m:1d,10m:7d", "1m:14d,30m:2y"} {
		for _, method := range []AggregationMethod{Average, Sum, Max, Min, Last, First} {
			if testing.Short() && method != Average && method != Last {
				continue
			}
			schema, method := schema, method
			scenarios = append(scenarios, scenario{name: fmt.Sprintf("sparse %s method=%d", schema, method), run: func(f *lockstepFile) {
				f.create(schema, method, 0)
				f.prefill(schema)
				// A future point seeds the unfinished-window replay.
				f.write(f.start+5, []*TimeSeriesPoint{{Time: f.start, Value: 1}, {Time: f.start + 30, Value: 2}})
				flushAt, batches := miscBatches(rand.New(rand.NewSource(int64(method))), f.start+25, 72, 1)
				for i, b := range batches {
					f.write(flushAt[i], b)
				}
			}})
		}
	}

	var avoided, newOff, oldOff int
	for group := 0; group < 4; group++ {
		t.Run(fmt.Sprintf("group=%d", group), func(t *testing.T) {
			for i, sc := range scenarios {
				if i%4 != group {
					continue
				}
				t.Run(fmt.Sprintf("scenario=%d", i), func(t *testing.T) {
					f := &lockstepFile{t: t, name: sc.name, dir: t.TempDir(), start: 1_700_000_000}
					sc.run(f)
					f.compare("after sidecar merge", true)
					avoided += f.avoided
					newOff += f.newOff
					oldOff += f.oldOff
					if f.avoided > 0 || f.newOff > 0 || f.oldOff > 0 {
						t.Logf("%s: %d updates, %d replays avoided, values off classic summed over updates: %d, previous rules %d", sc.name, f.steps, f.avoided, f.newOff, f.oldOff)
					}
				})
			}
		})
	}
	if avoided == 0 {
		t.Fatal("no update avoided a replay: the scenarios do not exercise the late tail correction")
	}
	t.Logf("replays avoided %d; values off classic summed over updates %d, previous rules %d", avoided, newOff, oldOff)
}

// lockstepFile drives the corrected, previous-rule and classic files.
type lockstepFile struct {
	t       *testing.T
	name    string
	dir     string
	start   int
	now     int
	steps   int
	avoided int
	// newOff and oldOff count values differing from classic, summed over
	// archives and updates.
	newOff, oldOff int
}

func (f *lockstepFile) path(previous bool) string {
	if previous {
		return filepath.Join(f.dir, "previous.wsp")
	}
	return filepath.Join(f.dir, "corrected.wsp")
}

func (f *lockstepFile) classicPath() string { return filepath.Join(f.dir, "classic.wsp") }

func (f *lockstepFile) clock() func() time.Time {
	return func() time.Time { return time.Unix(int64(f.now), 0) }
}

func (f *lockstepFile) options() *Options {
	return &Options{Compressed: true, OutOfOrder: true, Now: f.clock()}
}

func (f *lockstepFile) create(schema string, method AggregationMethod, xff float32) {
	rets, err := ParseRetentionDefs(schema)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, path := range []string{f.path(false), f.path(true), f.classicPath()} {
		options := f.options()
		if path == f.classicPath() {
			options = &Options{Now: f.clock()}
		}
		w, err := CreateWithOptions(path, rets, method, xff, options)
		if err != nil {
			f.t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			f.t.Fatal(err)
		}
	}
	f.now = f.start
}

// prefill wraps the base archive like runDigestScenario.
func (f *lockstepFile) prefill(schema string) {
	rets, err := ParseRetentionDefs(schema)
	if err != nil {
		f.t.Fatal(err)
	}
	step := rets[0].SecondsPerPoint()
	history := rets[0].MaxRetention() + 2*step
	if history > 35*86400 {
		history = 35 * 86400
	}
	from := f.start - history
	chunk := 86400
	if rets[0].MaxRetention() < chunk {
		chunk = rets[0].MaxRetention() / 3
	}
	for ts := from - from%step; ts < f.start; ts += chunk {
		until := ts + chunk
		if until > f.start {
			until = f.start
		}
		f.write(until, seriesPoints(ts, until, step))
	}
}

// apply writes points to one file and returns the replays the update took.
func (f *lockstepFile) apply(path string, options *Options, previous bool, points []*TimeSeriesPoint) (replays uint64) {
	defer func(saved bool) { lateTailCorrection = saved }(lateTailCorrection)
	lateTailCorrection = !previous
	w, err := OpenWithOptions(path, options)
	if err != nil {
		f.t.Fatal(err)
	}
	input := make([]*TimeSeriesPoint, len(points))
	for i, p := range points {
		input[i] = &TimeSeriesPoint{Time: p.Time, Value: p.Value}
	}
	before := overlapCount()
	if err := w.UpdateMany(input); err != nil {
		f.t.Fatal(err)
	}
	replays = overlapCount() - before
	if err := w.Close(); err != nil {
		f.t.Fatal(err)
	}
	return replays
}

func (f *lockstepFile) write(now int, points []*TimeSeriesPoint) {
	f.t.Helper()
	f.now = now
	got := f.apply(f.path(false), f.options(), false, points)
	want := f.apply(f.path(true), f.options(), true, points)
	f.apply(f.classicPath(), &Options{Now: f.clock()}, false, points)
	if want > got {
		f.avoided += int(want - got)
	}
	f.steps++
	f.compare("", false)
}

// fetchAll returns every archive's Fetch over its retention, the start time
// first. merge folds a sidecar into the main file first.
func (f *lockstepFile) fetchAll(path string, options *Options, merge bool) [][]float64 {
	w, err := OpenWithOptions(path, options)
	if err != nil {
		f.t.Fatal(err)
	}
	if merge && w.OutOfOrderPath() != "" {
		if err := w.MergeOutOfOrder(); err != nil {
			f.t.Fatal(err)
		}
		w.Close()
		if w, err = OpenWithOptions(path, options); err != nil {
			f.t.Fatal(err)
		}
	}
	defer w.Close()
	var out [][]float64
	for _, r := range w.Retentions() {
		ts, err := w.Fetch(f.now-r.MaxRetention()+r.SecondsPerPoint(), f.now)
		if err != nil {
			f.t.Fatal(err)
		}
		out = append(out, append([]float64{float64(ts.FromTime())}, ts.Values()...))
	}
	return out
}

func sameValue(a, b float64) bool { return a == b || math.IsNaN(a) && math.IsNaN(b) }

func (f *lockstepFile) compare(when string, merge bool) {
	f.t.Helper()
	got := f.fetchAll(f.path(false), f.options(), merge)
	want := f.fetchAll(f.path(true), f.options(), merge)
	classic := f.fetchAll(f.classicPath(), &Options{Now: f.clock()}, false)
	for i := range classic {
		if len(got[i]) != len(classic[i]) || len(want[i]) != len(classic[i]) {
			f.t.Fatalf("%s update %d %s: archive %d lengths %d, previous %d, classic %d", f.name, f.steps, when, i, len(got[i]), len(want[i]), len(classic[i]))
		}
		var off, oldOff int
		for j := 1; j < len(classic[i]); j++ {
			if !sameValue(got[i][j], classic[i][j]) {
				off++
			}
			if !sameValue(want[i][j], classic[i][j]) {
				oldOff++
			}
		}
		f.newOff += off
		f.oldOff += oldOff
		if off > oldOff {
			for j := 1; j < len(classic[i]); j++ {
				if !sameValue(got[i][j], classic[i][j]) || !sameValue(want[i][j], classic[i][j]) {
					f.t.Logf("  archive %d value %d (from %v): corrected %v previous %v classic %v", i, j-1, classic[i][0], got[i][j], want[i][j], classic[i][j])
				}
			}
			f.t.Fatalf("%s update %d %s: archive %d differs from classic at %d values, previous rules %d", f.name, f.steps, when, i, off, oldOff)
		}
	}
}

// BenchmarkMiscLateBatches measures one persister update of the sparse shape
// (about 37 points per 960 s writeout, each batch with late points), including
// the replay or tail correction each completed hour needs. The previous rules
// replayed the whole file every hour; corrected, each hour is a tail rewrite.
func BenchmarkMiscLateBatches(b *testing.B) {
	for _, mode := range []struct {
		name     string
		previous bool
	}{{"previous", true}, {"corrected", false}} {
		b.Run(mode.name, func(b *testing.B) {
			defer func(saved bool) { lateTailCorrection = saved }(lateTailCorrection)
			lateTailCorrection = !mode.previous
			const now = 1_700_000_040
			f := newShapeFile(b, "1m:1d,1h:7d,1d:30d", Average, 0)
			f.prefill("1m:1d,1h:7d,1d:30d", now)
			f.write(now+5, []*TimeSeriesPoint{{Time: now, Value: 1}, {Time: now + 30, Value: 2}})
			rng := rand.New(rand.NewSource(1))
			ts := now + 25
			replays := overlapUpdates[OverlapBufferedRollup].Load()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				flushAt, batches := miscBatches(rng, ts, 1, 1)
				f.write(flushAt[0], batches[0])
				ts = flushAt[0]
			}
			b.StopTimer()
			b.ReportMetric(float64(overlapUpdates[OverlapBufferedRollup].Load()-replays)/float64(b.N), "replays/op")
		})
	}
}
