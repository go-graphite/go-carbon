package whisper

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// This file uses only APIs that predate the overlap replay changes, so the
// same digest can be computed on the old and new code: run
//
//	OVERLAP_DIGEST=1 go test -run TestOverlapReplayDigest -v
//
// on both and compare the printed digests. TestOverlapReplayDifferential
// compares the in-memory sidecar merge with the published one directly.

// atomicOSFileSystem is the local filesystem declaring atomic update commits,
// which selects the in-memory sidecar merge of the overlap replay.
type atomicOSFileSystem struct{}

func (atomicOSFileSystem) OpenFile(name string, flag int, perm os.FileMode) (StorageFile, error) {
	return os.OpenFile(name, flag, perm)
}
func (atomicOSFileSystem) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }
func (atomicOSFileSystem) Remove(name string) error              { return os.Remove(name) }
func (atomicOSFileSystem) Rename(from, to string) error          { return os.Rename(from, to) }
func (atomicOSFileSystem) AtomicStorageCommit() bool             { return true }

type digestScenario struct {
	schema string
	method AggregationMethod
	xff    float32
	e      emitterSim
	days   int
}

func digestScenarios() []digestScenario {
	var out []digestScenario
	schemas := []string{"1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y", "10s:6h,1m:1d,10m:7d", "1s:10m,1m:1h,10m:1d", "5m:1y", "1s:2d,1m:28d,1h:2y"}
	emitters := []emitterSim{
		{cadence: 10},
		{cadence: 10, jitter: 3, dupRate: .02, lateRate: .01},
		{cadence: 60, skew: 30},
		{cadence: 7, jitter: 2, dupRate: .05, lateRate: .02, skew: 2},
		{cadence: 1, dupRate: .01},
	}
	methods := []AggregationMethod{Average, Sum, Max, Last}
	for i, schema := range schemas {
		for j, e := range emitters {
			if e.cadence == 1 && !strings.HasPrefix(schema, "1s") {
				continue
			}
			days := 1
			if strings.HasPrefix(schema, "1s:2d") {
				days = 0 // a few hours keep this large file affordable
			}
			out = append(out, digestScenario{schema: schema, method: methods[(i+j)%len(methods)], xff: []float32{0, .5}[(i+j)%2], e: e, days: days})
		}
	}
	return out
}

type digestFile struct {
	t    testing.TB
	path string
	now  int
	fs   StorageFileSystem
	rng  *rand.Rand
}

func (f *digestFile) options() *Options {
	return &Options{Compressed: true, OutOfOrder: true, FileSystem: f.fs, Now: func() time.Time { return time.Unix(int64(f.now), 0) }}
}

func (f *digestFile) write(now int, points []*TimeSeriesPoint) {
	f.t.Helper()
	f.now = now
	w, err := OpenWithOptions(f.path, f.options())
	if err != nil {
		f.t.Fatal(err)
	}
	input := make([]*TimeSeriesPoint, len(points))
	for i, p := range points {
		input[i] = &TimeSeriesPoint{Time: p.Time, Value: p.Value}
	}
	if err := w.UpdateMany(input); err != nil {
		f.t.Fatal(err)
	}
	// Persister-like compaction: rate limited and gated on sidecar size.
	if path := w.OutOfOrderPath(); path != "" && f.rng.Intn(4) == 0 {
		if data, err := os.ReadFile(path); err == nil && nonZeroPages(data) >= 16 {
			if _, err := w.MergeOutOfOrderWithOptions(OutOfOrderMergeOptions{MaxPointAge: 5 * time.Minute, RetentionMargin: time.Minute}); err != nil {
				f.t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		f.t.Fatal(err)
	}
}

func nonZeroPages(data []byte) int {
	n := 0
	for off := 0; off < len(data); off += 4096 {
		end := off + 4096
		if end > len(data) {
			end = len(data)
		}
		for _, b := range data[off:end] {
			if b != 0 {
				n++
				break
			}
		}
	}
	return n
}

// state returns the main file and sidecar bytes and every archive's Fetch.
func (f *digestFile) state() []byte {
	f.t.Helper()
	var out []byte
	main, err := os.ReadFile(f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	out = append(out, main...)
	w, err := OpenWithOptions(f.path, f.options())
	if err != nil {
		f.t.Fatal(err)
	}
	defer w.Close()
	if path := w.OutOfOrderPath(); path != "" {
		side, err := os.ReadFile(path)
		if err != nil {
			f.t.Fatal(err)
		}
		out = append(out, "sidecar"...)
		out = append(out, side...)
	}
	for _, r := range w.Retentions() {
		ts, err := w.Fetch(f.now-r.MaxRetention()+r.SecondsPerPoint(), f.now)
		if err != nil {
			f.t.Fatal(err)
		}
		out = binary.LittleEndian.AppendUint64(out, uint64(ts.FromTime()))
		for _, v := range ts.Values() {
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(v))
		}
	}
	return out
}

// runDigestScenario replays one scenario and returns the per-update states.
func runDigestScenario(t testing.TB, dir string, sc digestScenario, seed int64, fs StorageFileSystem, each func(step int, state []byte)) {
	rets, err := ParseRetentionDefs(sc.schema)
	if err != nil {
		t.Fatal(err)
	}
	f := &digestFile{t: t, path: filepath.Join(dir, "m.wsp"), now: 1_700_000_000, fs: fs, rng: rand.New(rand.NewSource(seed))}
	w, err := CreateWithOptions(f.path, rets, sc.method, sc.xff, f.options())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	start := f.now
	// Wrap the base archive, capped for the long-retention schemas.
	step := rets[0].SecondsPerPoint()
	history := rets[0].MaxRetention() + 2*step
	if history > 35*86400 {
		history = 35 * 86400
	}
	from := start - history
	chunk := 86400
	if rets[0].MaxRetention() < chunk {
		chunk = rets[0].MaxRetention() / 3
	}
	for ts := from - from%step; ts < start; ts += chunk {
		until := ts + chunk
		if until > start {
			until = start
		}
		f.write(until, seriesPoints(ts, until, step))
	}
	rng := rand.New(rand.NewSource(seed))
	e := sc.e
	e.minPoints, e.maxDelay = 960, 960
	duration := sc.days * 86400
	if duration == 0 {
		duration = 4 * 3600
	}
	flushAt, batches := e.batches(e.arrivals(rng, start, duration))
	for i, b := range batches {
		f.write(flushAt[i], b)
		each(i, f.state())
	}
}

func TestOverlapReplayDigest(t *testing.T) {
	if os.Getenv("OVERLAP_DIGEST") == "" {
		t.Skip("set OVERLAP_DIGEST=1 to print old/new comparable digests")
	}
	for _, fs := range []StorageFileSystem{nil, atomicOSFileSystem{}} {
		for i, sc := range digestScenarios() {
			h := sha256.New()
			steps := 0
			trace := os.Getenv("OVERLAP_DIGEST_TRACE") != "" && strings.Contains(fmt.Sprintf("%T %s/%v/%d", fs, sc.schema, sc.e, sc.method), os.Getenv("OVERLAP_DIGEST_TRACE"))
			runDigestScenario(t, t.TempDir(), sc, int64(i), fs, func(step int, state []byte) {
				h.Write(state)
				steps++
				if trace {
					t.Logf("trace step=%d %x", step, sha256.Sum256(state))
				}
			})
			t.Logf("digest fs=%T %s/%v/%d steps=%d %x", fs, sc.schema, sc.e, sc.method, steps, h.Sum(nil)[:12])
		}
	}
}

func seriesPoints(from, until, step int) []*TimeSeriesPoint {
	var ps []*TimeSeriesPoint
	for ts := from; ts < until; ts += step {
		ps = append(ps, &TimeSeriesPoint{Time: ts, Value: float64(ts % 997)})
	}
	return ps
}

// emitterSim models one metric: an emitter with a cadence, timestamp jitter,
// delivery delay, occasional late deliveries and duplicate resends, cached and
// flushed in 960-point / 960-second batches.
type emitterSim struct {
	cadence, jitter int
	skew            int     // emitter clock offset added to every timestamp
	lateRate        float64 // share of points delivered 2-30 minutes late
	dupRate         float64 // share of points resent 1-600 seconds later
	minPoints       int
	maxDelay        int
}

type arrival struct {
	at int
	p  TimeSeriesPoint
}

func (e emitterSim) arrivals(rng *rand.Rand, start, duration int) []arrival {
	var out []arrival
	phase := rng.Intn(e.cadence)
	for ts := start + phase; ts < start+duration; ts += e.cadence {
		stamp := ts + e.skew
		if e.jitter > 0 {
			stamp += rng.Intn(2*e.jitter+1) - e.jitter
		}
		value := float64(rng.Intn(1000))
		delay := rng.Intn(5)
		if rng.Float64() < e.lateRate {
			delay = 120 + rng.Intn(1680)
		}
		out = append(out, arrival{at: ts + delay, p: TimeSeriesPoint{Time: stamp, Value: value}})
		if rng.Float64() < e.dupRate {
			out = append(out, arrival{at: ts + delay + 1 + rng.Intn(600), p: TimeSeriesPoint{Time: stamp, Value: value}})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].at < out[j].at })
	return out
}

// batches groups arrivals the way the persister's writeout coalescing does.
func (e emitterSim) batches(arrivals []arrival) (flushAt []int, batches [][]*TimeSeriesPoint) {
	var cur []*TimeSeriesPoint
	first := 0
	for _, a := range arrivals {
		if len(cur) > 0 && a.at-first >= e.maxDelay {
			flushAt = append(flushAt, first+e.maxDelay)
			batches = append(batches, cur)
			cur = nil
		}
		if len(cur) == 0 {
			first = a.at
		}
		p := a.p
		cur = append(cur, &p)
		if len(cur) >= e.minPoints {
			flushAt = append(flushAt, a.at)
			batches = append(batches, cur)
			cur = nil
		}
	}
	return flushAt, batches
}
