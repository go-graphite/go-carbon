package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type readyFixture struct {
	*fakeSync
	starts  map[string]float64
	policy  func(string, string, *readyPolicy)
	read    func(syncSeries) syncSeries
	process func(string)
}

func (f *readyFixture) Process(_ context.Context, host string, _ int) (float64, error) {
	if f.process != nil {
		f.process(host)
	}
	return f.starts[host], nil
}
func (f *readyFixture) Policies(ctx context.Context, host string, names []string) (map[string]readyPolicy, error) {
	info, e := f.Info(ctx, host, names)
	out := map[string]readyPolicy{}
	for n, a := range info {
		p := readyPolicy{"average", .5, append([]syncArchive(nil), a...)}
		if f.policy != nil {
			f.policy(host, n, &p)
		}
		out[n] = p
	}
	return out, e
}
func (f *readyFixture) Readback(ctx context.Context, _, name string, from, until int64) (syncSeries, error) {
	s, e := f.Fetch(ctx, syncT, []string{name}, from, until)
	v := s[name]
	if f.read != nil {
		v = f.read(v)
	}
	return v, e
}
func newReadyFixture(t *testing.T, n int) (*readyFixture, syncOptions, syncReadyOptions) {
	t.Helper()
	f := &readyFixture{fakeSync: newFakeSync(), starts: map[string]float64{}}
	schema := []syncArchive{{Step: 3600, Points: 24 * 10}}
	for _, host := range []string{syncT, syncD1, syncD2, syncP} {
		f.starts[host] = float64(syncTestNow - 86400)
		for i := 0; i < n; i++ {
			f.put(host, fmt.Sprintf("metric.%03d", i), schema)
		}
	}
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2, syncP}, state: t.TempDir(), streams: 2, batch: 50, fullHistory: true, sampleEvery: 1, maxRounds: 4, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	r := syncReadyOptions{carbonAPI: "http://read.example", metricsPort: 7007, period: syncRanges{{"period", syncTestNow - 86400, syncTestNow - 3600}}, gapsComplete: true}
	return f, o, r
}
func readReadyReport(t *testing.T, state string) readyReport {
	t.Helper()
	data, e := os.ReadFile(filepath.Join(state, "ready.json"))
	if e != nil {
		t.Fatal(e)
	}
	var r readyReport
	if e = json.Unmarshal(data, &r); e != nil {
		t.Fatal(e)
	}
	return r
}
func TestReadyGates(t *testing.T) {
	tests := []struct {
		name, want string
		change     func(*readyFixture, *syncOptions, *syncReadyOptions)
	}{
		{"clean", "", func(*readyFixture, *syncOptions, *syncReadyOptions) {}},
		{"aggregation", "policy mismatch", func(f *readyFixture, _ *syncOptions, _ *syncReadyOptions) {
			f.policy = func(h, n string, p *readyPolicy) {
				if h == syncT && n == "metric.001" {
					p.Aggregation = "sum"
				}
			}
		}},
		{"xff", "policy mismatch", func(f *readyFixture, _ *syncOptions, _ *syncReadyOptions) {
			f.policy = func(h, n string, p *readyPolicy) {
				if h == syncT && n == "metric.001" {
					p.XFilesFactor = .1
				}
			}
		}},
		{"retention", "policy mismatch", func(f *readyFixture, _ *syncOptions, _ *syncReadyOptions) {
			f.policy = func(h, n string, p *readyPolicy) {
				if h == syncT && n == "metric.001" {
					p.Archives[0].Points++
				}
			}
		}},
		{"minimum", "sample minimum", func(f *readyFixture, _ *syncOptions, _ *syncReadyOptions) {
			for _, h := range []string{syncT, syncD1, syncD2, syncP} {
				delete(f.store(h).archives, "metric.001")
			}
		}},
		{"source minimum", "sample minimum", func(f *readyFixture, _ *syncOptions, _ *syncReadyOptions) {
			for i := 0; i < 60; i++ {
				delete(f.store(syncP).archives, fmt.Sprintf("metric.%03d", i))
			}
		}},
		{"readback hole", "readback", func(f *readyFixture, _ *syncOptions, _ *syncReadyOptions) {
			f.read = func(s syncSeries) syncSeries {
				if len(s.Values) > 0 {
					s.Values[0] = math.NaN()
				}
				return s
			}
		}},
		{"readback value", "readback", func(f *readyFixture, _ *syncOptions, _ *syncReadyOptions) {
			f.read = func(s syncSeries) syncSeries {
				if len(s.Values) > 0 {
					s.Values[0] += 1
				}
				return s
			}
		}},
		{"open gap", "still open", func(_ *readyFixture, o *syncOptions, _ *syncReadyOptions) {
			o.windows = syncRanges{{"open", syncTestNow - 60, syncTestNow + 600}}
		}},
		{"uncovered period", "do not cover", func(_ *readyFixture, o *syncOptions, _ *syncReadyOptions) {
			o.windows = syncRanges{{"gap", syncTestNow - 6000, syncTestNow - 4000}}
		}},
		{"missing ledger", "gap-ledger-complete", func(_ *readyFixture, _ *syncOptions, r *syncReadyOptions) { r.gapsComplete = false }},
		{"missing read URL", "carbonapi-url", func(_ *readyFixture, _ *syncOptions, r *syncReadyOptions) { r.carbonAPI = "" }},
		{"verify opt out", "verification disabled", func(_ *readyFixture, o *syncOptions, _ *syncReadyOptions) { o.noVerify = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, o, r := newReadyFixture(t, 100)
			tt.change(f, &o, &r)
			err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) })
			report := readReadyReport(t, o.state)
			joined := strings.Join(report.Reasons, " ")
			if tt.want == "" {
				if err != nil || report.Verdict != "READY" {
					t.Fatalf("%v %+v", err, report)
				}
			} else if !errors.Is(err, ErrSyncNotReady) || report.Verdict != "NOT_READY" || !strings.Contains(joined, tt.want) {
				t.Fatalf("%v %+v want %s", err, report, tt.want)
			}
		})
	}
}
func TestReadyRestartDuringRunFillsAndAccepts(t *testing.T) {
	f, o, r := newReadyFixture(t, 100)
	// Model a target restart in the initial sync: new input points are absent on
	// target even though the pre-restart journal says verification is complete.
	timestamp := syncTestNow - 3600
	clock := syncTestNow - 7200
	o.now = func() time.Time { return time.Unix(clock, 0) }
	r.period[0].Until = syncTestNow - 10800
	err := runSyncReady(context.Background(), &o, r, f, func() error {
		if err := runSync(context.Background(), o, f); err != nil {
			return err
		}
		clock = syncTestNow
		f.starts[syncT]++
		delete(f.store(syncT).points["metric.001"][3600], timestamp)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if report.Restarts[0].From > timestamp || report.Restarts[0].Until <= timestamp {
		t.Fatal("restart input gap was not covered", report.Restarts)
	}
	if report.Verdict != "READY" || len(report.Restarts) != 1 || len(f.copies) == 0 {
		t.Fatalf("missing recovery: %+v copies=%v", report, f.copies)
	}
	if _, ok := f.store(syncT).points["metric.001"][3600][timestamp]; !ok {
		t.Fatal("restart hole not filled")
	}
	paths, _ := filepath.Glob(filepath.Join(o.state, "ready-recovery-*", "sync.jsonl"))
	if len(paths) != 1 {
		t.Fatal("no fresh recovery journal", paths)
	}
}
func TestReadyRestartSourceAndResumedProof(t *testing.T) {
	f, o, r := newReadyFixture(t, 100)
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
		t.Fatal(err)
	}
	f.starts[syncP]++
	ts := syncTestNow - 7200
	delete(f.store(syncT).points["metric.001"][3600], ts)
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
		t.Fatal(err)
	}
	if len(readReadyReport(t, o.state).Restarts) != 1 {
		t.Fatal("source restart missed")
	}
}
func TestReadyNoReadyAndFailureInvalidate(t *testing.T) {
	f, o, r := newReadyFixture(t, 100)
	r.noReady = true
	called := false
	if err := runSyncReady(context.Background(), &o, r, f, func() error { called = true; return nil }); err != nil || !called {
		t.Fatal(err)
	}
	if _, e := os.Stat(filepath.Join(o.state, "ready.json")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("opt-out published readiness")
	}
	r.noReady = false
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return errors.New("copy failed") }); err == nil {
		t.Fatal("failure passed")
	}
	if readReadyReport(t, o.state).Verdict != "NOT_READY" {
		t.Fatal("stale READY")
	}
}
func TestReadyProcessAndReadbackHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			fmt.Fprintln(w, `process_start_time_seconds{cluster="test",location="west"} 123.5`)
			return
		}
		if r.URL.Query().Get("noCache") != "true" || r.URL.Query().Get("target") != "metric" {
			t.Error(r.URL)
		}
		fmt.Fprint(w, `[{"target":"metric","datapoints":[[1,100],[null,160],[3,220]]}]`)
	}))
	defer srv.Close()
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Millisecond})
	u := strings.Split(srv.URL, ":")
	var port int
	fmt.Sscan(u[len(u)-1], &port)
	if start, e := b.Process(context.Background(), srv.URL, port); e != nil || start != 123.5 {
		t.Fatal(start, e)
	}
	s, e := b.Readback(context.Background(), srv.URL, "metric", 100, 280)
	if e != nil || s.Step != 60 || s.finite(160) || !s.finite(220) {
		t.Fatal(s, e)
	}
}
func TestSyncBlockExpandsScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "block.json")
	data := `{"target":"http://target:4242","donors":["http://donor:4242"],"peers":["http://peer:4242"],"state":"state","period":{"from":100,"until":200},"gaps":[{"name":"gap","from":110,"until":120}],"carbonapi_url":"https://read.example","gap_ledger_complete":true}`
	if e := os.WriteFile(path, []byte(data), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := parseSyncCommand([]string{"--block", path, "--streams", "2"})
	if e != nil {
		t.Fatal(e)
	}
	if len(c.sync.from) != 2 || len(c.sync.windows) != 2 || c.ready.noReady || len(c.ready.period) != 1 {
		t.Fatalf("%+v", c)
	}
	// Incomplete audits can still run sync, but cannot attest READY.
	if e := os.WriteFile(path, []byte(strings.Replace(data, `"gap_ledger_complete":true`, `"gap_ledger_complete":false`, 1)), 0600); e != nil {
		t.Fatal(e)
	}
	if incomplete, e := parseSyncCommand([]string{"--block", path}); e != nil || incomplete.ready.gapsComplete {
		t.Fatal("incomplete ledger must remain a READY blocker", e)
	}
	for _, args := range [][]string{{"--block", path, "--to", "http://other"}, {"--block", path, "--period", "1:2"}, {"--block", path, "--block", path}} {
		if _, e := parseSyncCommand(args); e == nil {
			t.Fatal("scope override accepted", args)
		}
	}
}

func TestReadyFreshChecksFillUnreceiptedHole(t *testing.T) {
	f, o, r := newReadyFixture(t, 100)
	// A clean old journal exists, but fresh reads see a late hole without a
	// process restart. READY must repair it rather than ask for another command.
	if e := runSync(context.Background(), o, f); e != nil {
		t.Fatal(e)
	}
	if e := atomicStateJSON(filepath.Join(o.state, "ready-processes.json"), map[string]readyProcess{syncT: {f.starts[syncT], syncTestNow}, syncD1: {f.starts[syncD1], syncTestNow}, syncD2: {f.starts[syncD2], syncTestNow}, syncP: {f.starts[syncP], syncTestNow}}); e != nil {
		t.Fatal(e)
	}
	delete(f.store(syncT).points["metric.001"][3600], syncTestNow-7200)
	if e := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); e != nil {
		t.Fatal(e)
	}
	paths, _ := filepath.Glob(filepath.Join(o.state, "ready-fill-*"))
	if len(paths) != 1 {
		t.Fatal("fresh parity did not fill", paths)
	}
}
func TestReadyUnboundResumeRevalidates(t *testing.T) {
	f, o, r := newReadyFixture(t, 100)
	if e := runSync(context.Background(), o, f); e != nil {
		t.Fatal(e)
	}
	if e := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); e != nil {
		t.Fatal(e)
	}
	paths, _ := filepath.Glob(filepath.Join(o.state, "ready-recovery-*"))
	if len(paths) != 1 {
		t.Fatal("unbound proof reused", paths)
	}
}
func TestReadyCompareMissingAndStep(t *testing.T) {
	f, o, _ := newReadyFixture(t, 1)
	name := "metric.000"
	ts := syncTestNow - 7200
	delete(f.store(syncT).points[name][3600], ts)
	row, e := readyCompare(context.Background(), f, syncP, o.to, name, ts, ts+3600, 3600)
	if e != nil || row.Missing != 1 || row.Names != 1 {
		t.Fatal(row, e)
	}
	row, e = readyCompare(context.Background(), f, syncP, o.to, name, ts, ts+3600, 60)
	if e != nil || row.StepMismatch != 1 {
		t.Fatal(row, e)
	}
}

func TestReadyPrerequisiteGates(t *testing.T) {
	_, base, rbase := newReadyFixture(t, 0)
	for _, tt := range []struct {
		name, want string
		change     func(*syncOptions, *syncReadyOptions)
	}{
		{"history disabled", "full-history", func(o *syncOptions, _ *syncReadyOptions) {
			o.windows = syncRanges{{"period", syncTestNow - 86400, syncTestNow - 3600}}
			o.fullHistory = false
		}},
		{"history sampling disabled", "sampled", func(o *syncOptions, _ *syncReadyOptions) {
			o.windows = syncRanges{{"period", syncTestNow - 86400, syncTestNow - 3600}}
			o.sampleEvery = 0
		}},
		{"history sampling partial", "sampled", func(o *syncOptions, _ *syncReadyOptions) {
			o.windows = syncRanges{{"period", syncTestNow - 86400, syncTestNow - 3600}}
			o.sampleEvery = 64
		}},
		{"period missing", "period", func(_ *syncOptions, r *syncReadyOptions) { r.period = nil }},
		{"period future", "still open", func(_ *syncOptions, r *syncReadyOptions) {
			r.period = syncRanges{{"period", syncTestNow - 60, syncTestNow + 60}}
		}},
		{"URL credentials", "without credentials", func(_ *syncOptions, r *syncReadyOptions) { r.carbonAPI = "http://user:pass@read.example" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o, r := base, rbase
			tt.change(&o, &r)
			if reasons := strings.Join(readyPrerequisites(o, r), " "); !strings.Contains(reasons, tt.want) {
				t.Fatal(reasons)
			}
		})
	}
}
func TestReadyUnavailableProcessPreventsCopyAndInvalidates(t *testing.T) {
	f, o, r := newReadyFixture(t, 0)
	f.starts[syncP] = 0
	if e := atomicStateJSON(filepath.Join(o.state, "ready.json"), readyReport{Verdict: "READY"}); e != nil {
		t.Fatal(e)
	}
	called := false
	e := runSyncReady(context.Background(), &o, r, f, func() error { called = true; return nil })
	if e == nil || called || readReadyReport(t, o.state).Verdict != "NOT_READY" {
		t.Fatal("missing identity accepted", e, called)
	}
}

func TestReadyWindowScopeRepairsEveryPeerAndPreservesOtherTimes(t *testing.T) {
	f, o, r := newReadyFixture(t, 100)
	o.fullHistory = false
	o.windows = syncRanges{{"early", syncTestNow - 8*3600, syncTestNow - 6*3600}, {"late", syncTestNow - 4*3600, syncTestNow - 2*3600}}
	r.scope, r.period = "windows", nil
	// The last replica alone has one recoverable point. A separate target hole
	// outside the declared windows must remain untouched.
	inside, outside := syncTestNow-3*3600, syncTestNow-12*3600
	for _, host := range []string{syncT, syncD1, syncD2} {
		delete(f.store(host).points["metric.001"][3600], inside)
	}
	delete(f.store(syncT).points["metric.001"][3600], outside)
	err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) })
	if err != nil {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if report.Verdict != "READY" || report.Scope != "windows" || report.UnrecoverablePoints != nil {
		t.Fatalf("%+v", report)
	}
	if _, ok := f.store(syncT).points["metric.001"][3600][inside]; !ok {
		t.Fatal("last peer's unique point was not recovered")
	}
	if _, ok := f.store(syncT).points["metric.001"][3600][outside]; ok {
		t.Fatal("window repair widened to full history")
	}
	for key := range report.Counts {
		if strings.HasPrefix(key, "history ") || strings.HasPrefix(key, "period ") {
			t.Fatal("out-of-scope proof", key)
		}
	}
	if len(report.Counts) != 8 {
		t.Fatal("expected both windows against all three peers and read-back", report.Counts)
	}
}

func TestReadyWindowScopePrerequisites(t *testing.T) {
	_, base, options := newReadyFixture(t, 0)
	base.fullHistory = false
	base.windows = syncRanges{{"gap", syncTestNow - 600, syncTestNow - 60}}
	options.scope, options.period = "windows", nil
	if reasons := readyPrerequisites(base, options); len(reasons) != 0 {
		t.Fatal(reasons)
	}
	for _, change := range []func(*syncOptions, *syncReadyOptions){
		func(o *syncOptions, _ *syncReadyOptions) { o.noVerify = true },
		func(o *syncOptions, _ *syncReadyOptions) { o.fullHistory = true },
		func(o *syncOptions, _ *syncReadyOptions) { o.settle = time.Minute },
		func(o *syncOptions, _ *syncReadyOptions) { o.windows = nil },
		func(o *syncOptions, _ *syncReadyOptions) {
			o.windows = syncRanges{{"open", syncTestNow - 60, syncTestNow + 60}}
		},
		func(_ *syncOptions, r *syncReadyOptions) { r.gapsComplete = false },
	} {
		o, r := base, options
		change(&o, &r)
		if reasons := readyPrerequisites(o, r); len(reasons) == 0 {
			t.Fatal("invalid window proof accepted")
		}
	}
}

func TestReadyDisjointDonorCohortsMeetEachSourceMinimum(t *testing.T) {
	f, o, r := newReadyFixture(t, 0)
	for sourceIndex, source := range o.from {
		for i := 0; i < 100; i++ {
			name := fmt.Sprintf("disjoint.%d.%03d", sourceIndex, i)
			schema := []syncArchive{{Step: 3600, Points: 24 * 10}}
			f.put(source, name, schema)
			f.put(o.to, name, schema)
		}
	}
	if err := runSyncReady(context.Background(), &o, r, f, func() error { return runSync(context.Background(), o, f) }); err != nil {
		t.Fatal(err)
	}
	report := readReadyReport(t, o.state)
	if report.Verdict != "READY" || len(report.Sample) != 300 {
		t.Fatal(report.Verdict, report.Reasons, len(report.Sample))
	}
	for _, source := range o.from {
		if report.Counts["history "+source].Names != 100 || report.Counts["period "+source].Names != 100 {
			t.Fatal("source cohort was diluted by disjoint donors", report.Counts)
		}
	}
}

func TestSyncReadyBlockResumeWithoutOriginalDescriptor(t *testing.T) {
	for _, noReady := range []bool{false, true} {
		t.Run(fmt.Sprint(noReady), func(t *testing.T) {
			root := t.TempDir()
			state, block := filepath.Join(root, "state"), filepath.Join(root, "block.json")
			data := fmt.Sprintf(`{"target":%q,"donors":[%q],"peers":[%q],"state":%q,"period":{"from":100,"until":200},"gaps":[{"name":"gap","from":110,"until":120}],"carbonapi_url":"https://read.example","gap_ledger_complete":true}`, syncT, syncD1, syncP, state)
			if err := os.WriteFile(block, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"--block", block, "--process-metrics-port", "7123"}
			if noReady {
				args = append(args, "--no-ready")
			}
			c, err := parseSyncCommand(args)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.saveConfig(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(block); err != nil {
				t.Fatal(err)
			}
			resumed, err := parseSyncCommand([]string{"--resume", state})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.ready, resumed.ready) || !reflect.DeepEqual(c.sync.windows, resumed.sync.windows) || !reflect.DeepEqual(c.sync.from, resumed.sync.from) || !reflect.DeepEqual(c.explicit, resumed.explicit) {
				t.Fatalf("READY scope changed on resume: %+v != %+v", c.ready, resumed.ready)
			}
		})
	}
}
