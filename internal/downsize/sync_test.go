package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSyncStore is one store: per name its archives and points per step.
type fakeSyncStore struct {
	archives map[string][]syncArchive
	points   map[string]map[int64]map[int64]float64 // name -> step -> ts -> value
}

type fakeSync struct {
	mu        sync.Mutex
	stores    map[string]*fakeSyncStore
	forbidden map[string]bool
	copies    []string // "source names..." per Copy call
	fetches   atomic.Int64
	failAfter int64 // cancel the run after this many fetches (0 = never)
	cancel    context.CancelFunc
	hide      map[string]bool // names the target's carbonserver does not index yet
	noDigest  map[string]bool // stores on a build without /v1/transfer/digest
	poison    string          // fetching this name always fails
	digested  atomic.Int64
}

const syncTestNow = int64(1_800_000_000)

var syncTestSchema = []syncArchive{{Step: 60, Points: 7 * 1440}, {Step: 3600, Points: 24 * 365}}

func newFakeSync() *fakeSync {
	return &fakeSync{stores: map[string]*fakeSyncStore{}, forbidden: map[string]bool{}, hide: map[string]bool{}, noDigest: map[string]bool{}}
}

func (f *fakeSync) store(origin string) *fakeSyncStore {
	s := f.stores[origin]
	if s == nil {
		s = &fakeSyncStore{archives: map[string][]syncArchive{}, points: map[string]map[int64]map[int64]float64{}}
		f.stores[origin] = s
	}
	return s
}

// put writes a full series for name on store: every step of every archive
// over its window, except timestamps in skip (holes).
func (f *fakeSync) put(origin, name string, schema []syncArchive, skip ...int64) {
	s := f.store(origin)
	s.archives[name] = schema
	s.points[name] = map[int64]map[int64]float64{}
	gaps := map[int64]bool{}
	for _, ts := range skip {
		gaps[ts] = true
	}
	for _, a := range schema {
		pts := map[int64]float64{}
		from, until := syncWindow(syncTestNow, a, 0)
		for ts := from - a.Step; ts < until+a.Step; ts += a.Step {
			if !gaps[ts] {
				pts[ts] = float64(ts % 997)
			}
		}
		s.points[name][a.Step] = pts
	}
}

func (f *fakeSync) List(_ context.Context, store, prefix, after string, limit int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for n := range f.store(store).archives {
		if n > after && strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names[:min(limit, 2, len(names))], nil // tiny pages exercise paging
}

func (f *fakeSync) Info(_ context.Context, store string, names []string) (map[string][]syncArchive, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]syncArchive{}
	for _, n := range names {
		if a, ok := f.store(store).archives[n]; ok && !(f.hide[n] && strings.HasPrefix(store, "http://target")) {
			out[n] = a
		}
	}
	return out, nil
}

func (f *fakeSync) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	if n := f.fetches.Add(1); f.failAfter > 0 && n >= f.failAfter {
		f.cancel()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.store(store)
	out := map[string]syncSeries{}
	for _, n := range names {
		if n == f.poison {
			return nil, errors.New("decode render: corrupt series " + n)
		}
		schema, ok := s.archives[n]
		if !ok {
			continue
		}
		var step int64
		for _, a := range schema { // carbonserver: first archive covering from
			if syncTestNow-a.Step*a.Points <= from {
				step = a.Step
				break
			}
		}
		if step == 0 {
			continue
		}
		start := (from + step - 1) / step * step
		var values []float64
		for ts := start; ts < until; ts += step {
			v, ok := s.points[n][step][ts]
			if !ok {
				v = math.NaN()
			}
			values = append(values, v)
		}
		out[n] = syncSeries{Start: start, Step: step, Values: values}
	}
	return out, nil
}

func (f *fakeSync) Digest(_ context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noDigest[store] {
		return nil, errSyncNoDigest
	}
	f.digested.Add(int64(len(names)))
	s := f.store(store)
	out := map[string][]syncDigest{}
	for _, n := range names {
		schema, ok := s.archives[n]
		if !ok {
			continue
		}
		for _, a := range schema {
			var ts []int64
			for t := range s.points[n][a.Step] {
				if t >= from && t < until {
					ts = append(ts, t)
				}
			}
			sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
			hash := uint64(14695981039346656037)
			for _, t := range ts {
				hash = (hash ^ uint64(t)) * 1099511628211
				hash = (hash ^ math.Float64bits(s.points[n][a.Step][t])) * 1099511628211
			}
			out[n] = append(out[n], syncDigest{Step: a.Step, Points: a.Points, Count: int64(len(ts)), Hash: strconv.FormatUint(hash, 16)})
		}
	}
	return out, nil
}

// Copy merges source points into the target; existing target points win.
func (f *fakeSync) Copy(_ context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forbidden[source] {
		return syncCopyResult{Forbidden: true}, nil
	}
	if !strings.HasSuffix(journal, ".jsonl") {
		return syncCopyResult{}, errors.New("journal must end in .jsonl")
	}
	f.copies = append(f.copies, source+" "+strings.Join(names, ","))
	src, dst := f.store(source), f.store(target)
	for _, n := range names {
		if _, ok := dst.archives[n]; !ok {
			dst.archives[n] = src.archives[n]
			dst.points[n] = map[int64]map[int64]float64{}
		}
		for step, pts := range src.points[n] {
			if dst.points[n][step] == nil {
				dst.points[n][step] = map[int64]float64{}
			}
			for ts, v := range pts {
				if _, ok := dst.points[n][step][ts]; !ok {
					dst.points[n][step][ts] = v
				}
			}
		}
	}
	return syncCopyResult{Copied: len(names)}, nil
}

const (
	syncT  = "http://target:4242"
	syncD1 = "http://donor1:4242"
	syncD2 = "http://donor2:4242"
	syncP  = "http://peer:4242"
)

// threeSources: donor1 holds names the target lacks, donor2 holds points the
// target lacks, the peer holds the only copy of one name and of some points.
func threeSources(t *testing.T) (*fakeSync, syncOptions, []int64) {
	f := newFakeSync()
	hole1 := (syncTestNow - 86400) / 60 * 60
	hole2 := (syncTestNow - 2*86400) / 60 * 60
	hourHole := (syncTestNow - 30*86400) / 3600 * 3600
	short := []syncArchive{{Step: 60, Points: 1440}}
	// target
	f.put(syncT, "a.clean", syncTestSchema)
	f.put(syncT, "b.holes", syncTestSchema, hole1, hole2, hourHole)
	f.put(syncT, "c.peerhole", syncTestSchema, hole1)
	// donor1: missing names and a clean replica
	f.put(syncD1, "a.clean", syncTestSchema)
	f.put(syncD1, "d.missing", syncTestSchema)
	f.put(syncD1, "s.short", short)
	// donor2: has the points b lacks
	f.put(syncD2, "b.holes", syncTestSchema)
	f.put(syncD2, "c.peerhole", syncTestSchema, hole1) // donor lacks the same point
	// peer: the only copy of e and of c's point
	f.put(syncP, "a.clean", syncTestSchema)
	f.put(syncP, "c.peerhole", syncTestSchema)
	f.put(syncP, "e.peeronly", syncTestSchema)
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2, syncP}, state: t.TempDir(), streams: 2, batch: 2, maxRounds: 4,
		now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	return f, o, []int64{hole1, hole2, hourHole}
}

func TestSyncThreeSourcesFillsEverything(t *testing.T) {
	f, o, holes := threeSources(t)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	dst := f.store(syncT)
	for _, n := range []string{"d.missing", "e.peeronly"} {
		if _, ok := dst.archives[n]; !ok {
			t.Fatalf("%s not copied", n)
		}
	}
	if _, ok := dst.archives["s.short"]; ok {
		t.Fatal("short-retention name copied")
	}
	if _, ok := dst.points["b.holes"][60][holes[0]]; !ok {
		t.Fatal("b minute hole not filled from donor2")
	}
	if _, ok := dst.points["b.holes"][3600][holes[2]]; !ok {
		t.Fatal("b hour hole not filled from donor2")
	}
	if _, ok := dst.points["c.peerhole"][60][holes[0]]; !ok {
		t.Fatal("c hole not filled from the peer")
	}
	got := strings.Join(f.copies, "|")
	for _, want := range []string{syncD1 + " d.missing", syncP + " e.peeronly", syncD2 + " b.holes", syncP + " c.peerhole"} {
		if !strings.Contains(got, want) {
			t.Fatalf("copies %q lack %q", got, want)
		}
	}
	// A second run over the same state is a no-op: everything is journaled.
	n := len(f.copies)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != n {
		t.Fatalf("resume repeated copies: %v", f.copies[n:])
	}
}

func TestSyncForbiddenPeerReportsResidual(t *testing.T) {
	f, o, holes := threeSources(t)
	f.forbidden[syncP] = true
	err := runSync(context.Background(), o, f)
	if !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("want residual error, got %v", err)
	}
	dst := f.store(syncT)
	if _, ok := dst.points["b.holes"][60][holes[0]]; !ok {
		t.Fatal("reachable donor holes must still be filled")
	}
	data, err := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"name":"e.peeronly"`, "missing_only_on_forbidden_source", `"name":"c.peerhole"`, "source_forbidden"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("residual %s lacks %s", data, want)
		}
	}
}

func TestSyncResumesAfterRestart(t *testing.T) {
	f, o, _ := threeSources(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel, f.failAfter = cancel, 6
	if err := runSync(ctx, o, f); err == nil {
		t.Fatal("interrupted run must fail")
	}
	copiesBefore := len(f.copies)
	if copiesBefore == 0 {
		t.Fatal("missing-name copy should have finished before the interruption")
	}
	f.failAfter = 0
	f.fetches.Store(0)
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	// Missing-name copies are not repeated after the restart.
	for _, c := range f.copies[copiesBefore:] {
		if strings.Contains(c, "d.missing") || strings.Contains(c, "e.peeronly") {
			t.Fatalf("resumed run recopied %q", c)
		}
	}
	data, err := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), `"kind":"discovered"`) != 1 {
		t.Fatal("discovery must run once across the restart")
	}
}

func TestSyncUnindexedNameRetriedNextRound(t *testing.T) {
	f, o, _ := threeSources(t)
	f.hide["d.missing"] = true // copied, but carbonserver indexes it late
	var calls atomic.Int64
	o.now = func() time.Time { // verify workers call this concurrently
		if calls.Add(1) > 3 {
			f.mu.Lock()
			delete(f.hide, "d.missing")
			f.mu.Unlock()
		}
		return time.Unix(syncTestNow, 0)
	}
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
}

func TestSyncCompareCountsOnlyTargetAbsence(t *testing.T) {
	src := syncSeries{Start: 0, Step: 60, Values: []float64{1, 2, math.NaN(), 4}}
	dst := syncSeries{Start: 0, Step: 60, Values: []float64{9, math.NaN(), math.NaN(), math.NaN()}}
	h, ok := syncCompare("x", "s", src, dst, 60, 0, 240)
	if !ok || h.Points != 2 || h.First != 60 || h.Last != 180 {
		t.Fatalf("got %+v", h)
	}
}

func TestSyncOptions(t *testing.T) {
	o := syncOptions{to: "http://t:4242", from: []string{"http://t:4242"}, state: "x", streams: 1, batch: 1, maxRounds: 1}
	if err := o.normalize(); err == nil {
		t.Fatal("source equal to target must be refused")
	}
	o.from = []string{"http://a:4242", "http://b:4242/"}
	if err := o.normalize(); err != nil {
		t.Fatal(err)
	}
}

// windowsAround covers the minute holes with two small windows.
func windowsAround(holes []int64) syncRanges {
	return syncRanges{{"w0", holes[0] - 600, holes[0] + 600}, {"w1", holes[1] - 600, holes[1] + 600}}
}

func TestSyncWindowsByDigest(t *testing.T) {
	f, o, holes := threeSources(t)
	o.windows, o.sampleEvery = windowsAround(holes), 0
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if f.digested.Load() == 0 {
		t.Fatal("digests not used")
	}
	dst := f.store(syncT)
	if _, ok := dst.points["b.holes"][60][holes[0]]; !ok {
		t.Fatal("b window hole not filled")
	}
	if _, ok := dst.points["c.peerhole"][60][holes[0]]; !ok {
		t.Fatal("c window hole not filled from the peer")
	}
	// a.clean matches by digest everywhere: never copied.
	if strings.Contains(strings.Join(f.copies, "|"), "a.clean") {
		t.Fatalf("clean name copied: %v", f.copies)
	}
}

func TestSyncWindowsWithoutDigestEndpoint(t *testing.T) {
	f, o, holes := threeSources(t)
	o.windows, o.sampleEvery = windowsAround(holes), 0
	f.noDigest[syncT], f.noDigest[syncD2] = true, true // older builds: point reads
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.store(syncT).points["b.holes"][60][holes[1]]; !ok {
		t.Fatal("b window hole not filled by the multi-fetch fallback")
	}
}

func TestSyncSkipsBadMetric(t *testing.T) {
	f, o, holes := threeSources(t)
	f.poison = "a.clean"
	err := runSync(context.Background(), o, f)
	if !errors.Is(err, ErrSyncResidual) {
		t.Fatalf("want residual for the skipped name, got %v", err)
	}
	if _, ok := f.store(syncT).points["b.holes"][60][holes[0]]; !ok {
		t.Fatal("other names must still be verified and filled")
	}
	data, _ := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if !strings.Contains(string(data), `"name":"a.clean"`) || !strings.Contains(string(data), "skipped: ") {
		t.Fatalf("residual %s", data)
	}
}

func TestSyncRangesFlag(t *testing.T) {
	var r syncRanges
	if err := r.Set("w00:100:200 300:400,x:5:6"); err != nil {
		t.Fatal(err)
	}
	if len(r) != 3 || r[0].Name != "w00" || r[1].From != 300 || r[2].Until != 6 {
		t.Fatalf("%+v", r)
	}
	if err := r.Set("9:1"); err == nil {
		t.Fatal("until <= from accepted")
	}
}

func TestSyncPrefixScopesTheRun(t *testing.T) {
	f, o, holes := threeSources(t)
	o.prefix = "b."
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	dst := f.store(syncT)
	if _, ok := dst.points["b.holes"][60][holes[0]]; !ok {
		t.Fatal("b hole not filled")
	}
	if _, ok := dst.archives["d.missing"]; ok {
		t.Fatal("name outside the prefix copied")
	}
	if _, ok := dst.points["c.peerhole"][60][holes[0]]; ok {
		t.Fatal("name outside the prefix filled")
	}
}

func TestSyncShortRetentionRecovery(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(fmt.Sprintf("include=%t", include), func(t *testing.T) {
			f := newFakeSync()
			schema := []syncArchive{{Step: 60, Points: 2 * 1440}}
			hole := (syncTestNow - 3600) / 60 * 60
			f.put(syncT, "existing", schema, hole)
			f.put(syncD1, "existing", schema)
			f.put(syncD1, "missing", schema)
			// Copying a peer must preserve a finite target value.
			f.store(syncT).points["existing"][60][hole-60] = 12345
			o := syncOptions{to: syncT, from: []string{syncD1}, state: t.TempDir(), streams: 2, batch: 2, maxRounds: 4,
				includeShort: include, windows: syncRanges{{Name: "gap", From: hole - 120, Until: hole + 120}},
				now: func() time.Time { return time.Unix(syncTestNow, 0) }}
			if err := runSync(context.Background(), o, f); err != nil {
				t.Fatal(err)
			}
			_, filled := f.store(syncT).points["existing"][60][hole]
			_, copied := f.store(syncT).archives["missing"]
			if filled != include || copied != include {
				t.Fatalf("filled=%t copied=%t, include=%t", filled, copied, include)
			}
			if got := f.store(syncT).points["existing"][60][hole-60]; got != 12345 {
				t.Fatalf("overwrote target value: %v", got)
			}
			if include {
				f.copies = nil
				o.state = t.TempDir()
				if err := runSync(context.Background(), o, f); err != nil {
					t.Fatal(err)
				}
				if len(f.copies) != 0 {
					t.Fatalf("fresh verification copied again: %v", f.copies)
				}
			}
		})
	}
}

// Every detail request must be in flight before any is allowed to finish.
// This catches serial fallback reads without depending on benchmark timing.
type rendezvousSync struct {
	*fakeSync
	entered atomic.Int64
	ready   chan struct{}
	want    int64
}

func (b *rendezvousSync) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	if b.entered.Add(1) == b.want {
		close(b.ready)
	}
	select {
	case <-b.ready:
		return b.fakeSync.Fetch(ctx, store, names, from, until)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestSyncWindowConcurrentDetailParity(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(strconv.FormatBool(fallback), func(t *testing.T) {
			f, o, holes := threeSources(t)
			f.noDigest[syncD2] = fallback
			f.noDigest[syncP] = fallback
			names := []string{"b.holes", "c.peerhole"}
			w := syncRange{"test", holes[0] - 600, holes[0] + 600}
			schemas := map[string][]syncArchive{}
			var want []syncHole
			for _, n := range names {
				schemas[n] = syncTestSchema
			}
			target, _ := f.Fetch(context.Background(), syncT, names, w.From, w.Until)
			for _, source := range []string{syncD2, syncP} {
				got, _ := f.Fetch(context.Background(), source, names, w.From, w.Until)
				if !fallback {
					sd, _ := f.Digest(context.Background(), source, names, w.From, w.Until)
					td, _ := f.Digest(context.Background(), syncT, names, w.From, w.Until)
					for _, n := range names {
						_, coarse := syncDigestMismatch(sd[n], td[n])
						for _, h := range coarse {
							h.Name, h.Source, h.Window = n, source, w.Name
							h.First, h.Last = w.From, w.Until-1
							want = append(want, h)
						}
					}
				}
				for _, n := range names {
					if h, ok := syncCompare(n, source, got[n], target[n], 60, w.From, w.Until); ok {
						h.Window = w.Name
						want = append(want, h)
					}
				}
			}
			// Legacy sources are read concurrently before deciding whether the
			// target needs a detail read. Digest mismatches fan out together.
			wantReads := int64(3)
			if fallback {
				wantReads = 2
			}
			backend := &rendezvousSync{fakeSync: f, ready: make(chan struct{}), want: wantReads}
			s := &syncer{o: o, b: backend}
			for _, n := range names {
				s.masks.Store(n, uint64(6))
			}
			var row syncRow
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := s.checkWindow(ctx, &row, names, schemas, w); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(row.Holes, want) {
				t.Fatalf("holes %+v, want %+v", row.Holes, want)
			}
		})
	}
}
