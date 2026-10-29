package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"

	"github.com/go-graphite/go-carbon/helper/heartbeat"
)

const (
	step = 60
	base = 1_700_000_000 - 1_700_000_000%step // aligned
)

// fakeServer is a carbonserver stand-in serving /metrics/list/ and the
// carbonapi_v3_pb multi-fetch with whisper's from/until rounding.
type fakeServer struct {
	mu       sync.Mutex
	step     int64
	data     map[string]map[int64]float64
	requests [][]string // names per render request
	times    []time.Time
	fail     func(names []string, n int) int // status to fail with, 0 for success
	srv      *httptest.Server
}

func newFake(t *testing.T) *fakeServer {
	f := &fakeServer{step: step, data: map[string]map[int64]float64{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) set(name string, ts int64, v float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.data[name] == nil {
		f.data[name] = map[int64]float64{}
	}
	f.data[name][ts] = v
}

// fill sets name to value(ts) on every slot from from to the end of the test
// window (base+10*step) except skip.
func (f *fakeServer) fill(name string, from int64, value func(int64) float64, skip func(int64) bool) {
	for ts := from; ts <= base+10*step; ts += step {
		if skip == nil || !skip(ts) {
			f.set(name, ts, value(ts))
		}
	}
}

func (f *fakeServer) renders() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.requests...)
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/metrics/list/":
		f.mu.Lock()
		names := make([]string, 0, len(f.data))
		for n := range f.data {
			names = append(names, n)
		}
		f.mu.Unlock()
		b, _ := (&protov3.ListMetricsResponse{Metrics: names}).MarshalVT()
		_, _ = w.Write(b)
	case "/render/":
		body, _ := io.ReadAll(r.Body)
		var req protov3.MultiFetchRequest
		if r.URL.Query().Get("format") != "carbonapi_v3_pb" || req.UnmarshalVT(body) != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		names := make([]string, len(req.Metrics))
		for i, m := range req.Metrics {
			names[i] = m.Name
		}
		f.mu.Lock()
		f.requests = append(f.requests, names)
		f.times = append(f.times, time.Now())
		n := len(f.requests)
		fail := f.fail
		var resp protov3.MultiFetchResponse
		for _, m := range req.Metrics {
			pts, ok := f.data[m.Name]
			if !ok {
				continue
			}
			start := m.StartTime - m.StartTime%f.step + f.step
			stop := m.StopTime - m.StopTime%f.step + f.step
			var vals []float64
			for ts := start; ts < stop; ts += f.step {
				v, ok := pts[ts]
				if !ok {
					v = math.NaN()
				}
				vals = append(vals, v)
			}
			resp.Metrics = append(resp.Metrics, &protov3.FetchResponse{Name: m.Name, StartTime: start, StopTime: stop, StepTime: f.step, Values: vals})
		}
		f.mu.Unlock()
		if fail != nil {
			if status := fail(names, n); status != 0 {
				http.Error(w, "injected", status)
				return
			}
		}
		if len(resp.Metrics) == 0 {
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		b, _ := resp.MarshalVT()
		_, _ = w.Write(b)
	default:
		http.NotFound(w, r)
	}
}

// fakeIngest records plaintext lines and optionally applies them to a fake.
type fakeIngest struct {
	mu      sync.Mutex
	lines   []string
	accepts int
	ln      net.Listener
	wake    chan struct{}
}

func newIngest(t *testing.T, apply func(name string, ts int64, v float64)) *fakeIngest {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	in := &fakeIngest{ln: ln, wake: make(chan struct{}, 1)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			in.mu.Lock()
			in.accepts++
			in.mu.Unlock()
			go func() {
				defer c.Close()
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					line := sc.Text()
					if apply != nil {
						p := strings.Fields(line)
						v, _ := strconv.ParseFloat(p[1], 64)
						ts, _ := strconv.ParseInt(p[2], 10, 64)
						apply(p[0], ts, v)
					}
					in.mu.Lock()
					in.lines = append(in.lines, line)
					in.mu.Unlock()
					select {
					case in.wake <- struct{}{}:
					default:
					}
				}
			}()
		}
	}()
	return in
}

// wait returns the received lines once there are n of them.
func (in *fakeIngest) wait(t *testing.T, n int) []string {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		got := in.got()
		if len(got) >= n {
			return got
		}
		select {
		case <-in.wake:
		case <-timer.C:
			return in.got()
		}
	}
}

func (in *fakeIngest) got() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	out := append([]string(nil), in.lines...)
	sort.Strings(out)
	return out
}

func testOptions(local *fakeServer, ingest string, replicas ...*fakeServer) *options {
	o := &options{
		local: local.srv.URL, ingest: ingest, settle: 0,
		lookback: 30 * time.Minute, batch: 500, workers: 2, replicaRPS: 1000,
		timeout: 5 * time.Second, retries: 3, backoff: 5 * time.Millisecond, maxBack: 20 * time.Millisecond,
		writeRate: 0, flushPt: 3, writeRetries: 3,
		from: strconv.Itoa(base), until: strconv.Itoa(base + 10*step),
	}
	for _, r := range replicas {
		o.replicas = append(o.replicas, r.srv.URL)
	}
	return o
}

func runOK(t *testing.T, o *options) report {
	t.Helper()
	r, err := run(context.Background(), o, time.Unix(base+3600, 0))
	if err != nil {
		t.Fatalf("run: %v (report %+v)", err, r)
	}
	return r
}

func line(name string, v float64, ts int64) string {
	return name + " " + strconv.FormatFloat(v, 'g', -1, 64) + " " + strconv.FormatInt(ts, 10)
}

func TestFillOnlyMissingWithFallback(t *testing.T) {
	t.Parallel()
	local, r1, r2 := newFake(t), newFake(t), newFake(t)
	from := int64(base)
	val := func(ts int64) float64 { return float64(ts-base) / 7 }
	gap := func(ts int64) bool { return ts >= base+3*step && ts <= base+6*step }

	// gappy: slots 3..6 missing locally; one local value differs from the
	// replica and must stay authoritative.
	local.fill("a.gappy", from-20*step, val, gap)
	local.set("a.gappy", base+2*step, -1)
	r1.fill("a.gappy", from-20*step, func(int64) float64 { return 42.5 }, nil)
	r2.fill("a.gappy", from-20*step, func(int64) float64 { return 99 }, nil)
	// complete: no replica request at all.
	local.fill("b.complete", from-20*step, val, nil)
	r1.fill("b.complete", from, val, nil)
	// inactive: no local point in the lookback or window.
	local.set("c.inactive", from-3600*24, 1)
	r1.fill("c.inactive", from, val, nil)
	// fallback: unknown to replica 1, present on replica 2.
	local.fill("d.fallback", from-20*step, val, gap)
	r2.fill("d.fallback", from, func(int64) float64 { return 1e-7 }, nil)
	// nowhere: no replica has it.
	local.fill("e.nowhere", from-20*step, val, gap)

	in := newIngest(t, nil)
	r := runOK(t, testOptions(local, in.ln.Addr().String(), r1, r2))

	var want []string
	for ts := base + 3*step; ts <= base+6*step; ts += step {
		want = append(want, line("a.gappy", 42.5, int64(ts)), line("d.fallback", 1e-7, int64(ts)))
	}
	sort.Strings(want)
	if got := in.wait(t, len(want)); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sent:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	m := r.Metrics
	if m["with_gaps"] != 3 || m["complete"] != 1 || m["inactive"] != 1 || m["fillable_points"] != 8 ||
		m["unfilled_metrics"] != 1 || m["missing_slots"] != 12 || r.Write["points_sent"] != 8 {
		t.Fatalf("report %+v %+v", m, r.Write)
	}
	r1req := r1.renders()
	if len(r1req) != 1 || strings.Join(r1req[0], ",") != "a.gappy,d.fallback,e.nowhere" {
		t.Fatalf("replica 1 requests %v", r1req)
	}
	if r2req := r2.renders(); len(r2req) != 1 || strings.Join(r2req[0], ",") != "d.fallback,e.nowhere" {
		t.Fatalf("replica 2 must get only metrics replica 1 had nothing for: %v", r2req)
	}
}

func TestBatchingAndRateLimit(t *testing.T) {
	t.Parallel()
	local, rep := newFake(t), newFake(t)
	var names []string
	for i := range 25 {
		n := "m." + strconv.Itoa(100+i)
		names = append(names, n)
		local.fill(n, base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts == base+5*step })
		rep.fill(n, base, func(int64) float64 { return 2 }, nil)
	}
	in := newIngest(t, nil)
	o := testOptions(local, in.ln.Addr().String(), rep)
	o.batch, o.workers, o.replicaRPS = 10, 3, 10
	began := time.Now()
	r := runOK(t, o)
	reqs := rep.renders()
	if len(reqs) != 3 || len(local.renders()) != 3 {
		t.Fatalf("want 3 batches, got replica %d local %d", len(reqs), len(local.renders()))
	}
	var seen []string
	for _, q := range reqs {
		if len(q) > 10 {
			t.Fatalf("batch of %d names", len(q))
		}
		seen = append(seen, q...)
	}
	sort.Strings(seen)
	if strings.Join(seen, ",") != strings.Join(names, ",") {
		t.Fatalf("names requested %v", seen)
	}
	// Burst 1 at 10/s: three requests need at least 200ms.
	if d := time.Since(began); d < 190*time.Millisecond {
		t.Fatalf("replica rate limit not applied: %s", d)
	}
	rep.mu.Lock()
	times := append([]time.Time(nil), rep.times...)
	rep.mu.Unlock()
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	if gap := times[2].Sub(times[0]); gap < 190*time.Millisecond {
		t.Fatalf("requests 1..3 spaced %s", gap)
	}
	if r.Write["points_sent"] != 25 || r.Write["flushes"] < 9 {
		t.Fatalf("write report %+v", r.Write)
	}
}

func TestRetriesThrottlingAndFailover(t *testing.T) {
	t.Parallel()
	local, r1, r2 := newFake(t), newFake(t), newFake(t)
	local.fill("x", base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts == base+step })
	r1.fill("x", base, func(int64) float64 { return 3 }, nil)
	r2.fill("x", base, func(int64) float64 { return 4 }, nil)
	// Two 429s, then success: retried on replica 1.
	r1.fail = func(_ []string, n int) int {
		if n <= 2 {
			return http.StatusTooManyRequests
		}
		return 0
	}
	in := newIngest(t, nil)
	r := runOK(t, testOptions(local, in.ln.Addr().String(), r1, r2))
	if got := in.wait(t, 1); len(got) != 1 || got[0] != line("x", 3, base+step) || r.Replicas[r1.srv.URL]["retries"] != 2 {
		t.Fatalf("sent %v report %+v", got, r.Replicas)
	}
	if len(r2.renders()) != 0 {
		t.Fatal("replica 2 used although replica 1 recovered")
	}

	// Replica 1 down: everything comes from replica 2.
	r1.fail = func([]string, int) int { return http.StatusInternalServerError }
	in2 := newIngest(t, nil)
	r = runOK(t, testOptions(local, in2.ln.Addr().String(), r1, r2))
	if got := in2.wait(t, 1); len(got) != 1 || got[0] != line("x", 4, base+step) || r.Replicas[r1.srv.URL]["failures"] != 1 {
		t.Fatalf("failover sent %v report %+v", got, r.Replicas)
	}

	// Every replica down: the batch fails and nothing is written.
	r2.fail = r1.fail
	in3 := newIngest(t, nil)
	if _, err := run(context.Background(), testOptions(local, in3.ln.Addr().String(), r1, r2), time.Unix(base+3600, 0)); err == nil {
		t.Fatal("expected failure with every replica down")
	}
	if len(in3.got()) != 0 {
		t.Fatal("points written without a replica answer")
	}
}

func TestStepMismatchIsNotFilled(t *testing.T) {
	local, rep := newFake(t), newFake(t)
	rep.step = 30
	local.fill("s", base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts == base+step })
	for ts := int64(base); ts <= base+10*step; ts += 30 {
		rep.set("s", ts, 5)
	}
	in := newIngest(t, nil)
	r := runOK(t, testOptions(local, in.ln.Addr().String(), rep))
	if r.Metrics["step_mismatch"] != 1 || r.Metrics["unfilled_metrics"] != 1 || len(in.got()) != 0 {
		t.Fatalf("report %+v sent %v", r.Metrics, in.got())
	}
}

func TestCheckpointResume(t *testing.T) {
	t.Parallel()
	local, rep := newFake(t), newFake(t)
	for i := range 30 {
		n := "m." + strconv.Itoa(10+i) // m.10 .. m.39, sorted
		local.fill(n, base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts == base+2*step })
		rep.fill(n, base, func(int64) float64 { return 7 }, nil)
	}
	// The middle batch (m.20..m.29) fails on its only replica.
	rep.fail = func(names []string, _ int) int {
		for _, n := range names {
			if n == "m.25" {
				return http.StatusBadGateway
			}
		}
		return 0
	}
	in := newIngest(t, local.set) // applied points become local data
	o := testOptions(local, in.ln.Addr().String(), rep)
	o.batch, o.workers, o.retries = 10, 1, 0
	o.checkpoint = filepath.Join(t.TempDir(), "ckpt.json")
	if _, err := run(context.Background(), o, time.Unix(base+3600, 0)); err == nil {
		t.Fatal("first run must report the failed batch")
	}
	var c checkpointState
	b, _ := os.ReadFile(o.checkpoint)
	if json.Unmarshal(b, &c) != nil || c.DoneThrough != "m.19" || c.Complete {
		t.Fatalf("checkpoint after failure %s", b)
	}
	if n := len(in.wait(t, 20)); n != 20 {
		t.Fatalf("first run sent %d points, want 20", n)
	}

	rep.fail = nil
	before := len(local.renders())
	r := runOK(t, o)
	if r.Metrics["skipped_checkpoint"] != 10 || r.Metrics["scanned"] != 20 {
		t.Fatalf("resume report %+v", r.Metrics)
	}
	for _, q := range local.renders()[before:] {
		for _, n := range q {
			if n <= "m.19" {
				t.Fatalf("resumed run re-read finished metric %s", n)
			}
		}
	}
	// m.30..m.39 were filled by the first run and are complete now.
	if n := len(in.wait(t, 30)); n != 30 || r.Metrics["complete"] != 10 {
		t.Fatalf("after resume sent %d points (want 30), report %+v", n, r.Metrics)
	}
	b, _ = os.ReadFile(o.checkpoint)
	if json.Unmarshal(b, &c) != nil || !c.Complete {
		t.Fatalf("checkpoint not complete: %s", b)
	}
	before = len(local.renders())
	runOK(t, o)
	if len(local.renders()) != before {
		t.Fatal("completed checkpoint still read metrics")
	}

	// A checkpoint for another window is refused.
	o.until = strconv.Itoa(base + 11*step)
	if _, err := run(context.Background(), o, time.Unix(base+3600, 0)); err == nil || !strings.Contains(err.Error(), "window") {
		t.Fatalf("want window mismatch, got %v", err)
	}
}

func TestDryRun(t *testing.T) {
	local, rep := newFake(t), newFake(t)
	local.fill("d", base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts >= base+step && ts <= base+3*step })
	rep.fill("d", base, func(int64) float64 { return 2 }, nil)
	in := newIngest(t, nil)
	o := testOptions(local, in.ln.Addr().String(), rep)
	o.dryRun = true
	o.checkpoint = filepath.Join(t.TempDir(), "ckpt.json")
	r := runOK(t, o)
	if r.Metrics["fillable_points"] != 3 || r.Write != nil || !r.DryRun {
		t.Fatalf("dry-run report %+v", r)
	}
	time.Sleep(50 * time.Millisecond)
	in.mu.Lock()
	accepts := in.accepts
	in.mu.Unlock()
	if accepts != 0 {
		t.Fatal("dry run connected to ingest")
	}
	if _, err := os.Stat(o.checkpoint); !os.IsNotExist(err) {
		t.Fatal("dry run wrote a checkpoint")
	}
}

func writeHeartbeat(t *testing.T, path string, s heartbeat.State) {
	t.Helper()
	s.Version = heartbeat.Version
	b, _ := json.Marshal(s)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func TestCacheGatePausesWrites(t *testing.T) {
	hb := filepath.Join(t.TempDir(), "hb.json")
	writeHeartbeat(t, hb, heartbeat.State{Updated: time.Now(), IntervalSeconds: 10, CacheSize: 90, CacheMaxSize: 100})
	in := newIngest(t, nil)
	gate := &cacheGate{path: hb, pauseAt: 0.5, resumeAt: 0.4, poll: 5 * time.Millisecond, now: time.Now}
	w := newWriter(in.ln.Addr().String(), 0, 1, gate, 3, time.Millisecond, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.run(ctx) }()
	done := make(chan error, 1)
	go func() { done <- w.write(ctx, []point{{"p", 1, base}}) }()
	select {
	case <-done:
		t.Fatal("write went through with the cache 90% full")
	case <-time.After(100 * time.Millisecond):
	}
	// Below pause but above resume: still paused (hysteresis).
	writeHeartbeat(t, hb, heartbeat.State{Updated: time.Now(), IntervalSeconds: 10, CacheSize: 45, CacheMaxSize: 100})
	select {
	case <-done:
		t.Fatal("resumed above the resume threshold")
	case <-time.After(50 * time.Millisecond):
	}
	writeHeartbeat(t, hb, heartbeat.State{Updated: time.Now(), IntervalSeconds: 10, CacheSize: 10, CacheMaxSize: 100})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not resume")
	}
	if w.stats.pauses.Load() != 1 || w.stats.points.Load() != 1 {
		t.Fatalf("pauses %d points %d", w.stats.pauses.Load(), w.stats.points.Load())
	}
	// Stale or clean heartbeats pause too.
	for _, s := range []heartbeat.State{
		{Updated: time.Now().Add(-time.Hour), IntervalSeconds: 10, CacheSize: 1, CacheMaxSize: 100},
		{Updated: time.Now(), Clean: true, CacheSize: 1, CacheMaxSize: 100},
	} {
		writeHeartbeat(t, hb, s)
		if _, why := gate.state(); why == "" {
			t.Fatalf("state %+v must pause", s)
		}
	}
}

func TestWriterRateLimit(t *testing.T) {
	in := newIngest(t, nil)
	w := newWriter(in.ln.Addr().String(), 100, 10, nil, 3, time.Millisecond, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.run(ctx) }()
	pts := make([]point, 30)
	for i := range pts {
		pts[i] = point{"r", float64(i), int64(base + i*step)}
	}
	began := time.Now()
	if err := w.write(ctx, pts); err != nil {
		t.Fatal(err)
	}
	// 100 points/s with a 10-point burst: 30 points need at least 200ms.
	if d := time.Since(began); d < 190*time.Millisecond {
		t.Fatalf("30 points at 100/s took %s", d)
	}
	if len(in.wait(t, 30)) != 30 {
		t.Fatalf("received %d lines", len(in.got()))
	}
}

func TestUncleanWindow(t *testing.T) {
	hb := filepath.Join(t.TempDir(), "hb.json")
	now := time.Unix(base+7200, 0)
	o := &options{uncleanStart: true, heartbeatFile: hb, uncleanLoss: 16 * time.Minute, uncleanMargin: time.Minute,
		maxUncleanAge: 24 * time.Hour, settle: time.Minute}
	writeHeartbeat(t, hb, heartbeat.State{Updated: now})
	if _, _, ok, err := o.window(now); ok || err != nil {
		t.Fatalf("no unclean stop: ok=%v err=%v", ok, err)
	}
	lastAlive := now.Add(-time.Hour)
	writeHeartbeat(t, hb, heartbeat.State{Updated: now, Unclean: &heartbeat.Window{
		LastAlive: lastAlive, LossWindowSeconds: 960, Restarted: lastAlive.Add(3 * time.Minute), Resumed: lastAlive.Add(5 * time.Minute)}})
	from, until, ok, err := o.window(now)
	if err != nil || !ok || from != lastAlive.Add(-17*time.Minute).Unix() || until != lastAlive.Add(6*time.Minute).Unix() {
		t.Fatalf("window %d..%d ok=%v err=%v", from, until, ok, err)
	}
	o.maxUncleanAge = 30 * time.Minute
	if _, _, _, err := o.window(now); err == nil {
		t.Fatal("old unclean stop must be refused")
	}
}

func TestParseFlagsAndGlob(t *testing.T) {
	for _, args := range [][]string{
		{"-since", "10m"}, // no replica
		{"-replica", "http://r", "-since", "1m", "-from", "1"}, // two window sources
		{"-replica", "http://r"},                               // no window
		{"-replica", "http://r", "-since-unclean-start"},       // no heartbeat file
	} {
		if _, err := parseFlags(args); err == nil {
			t.Fatalf("%v must be rejected", args)
		}
	}
	o, err := parseFlags([]string{"-replica", "http://a/, http://b", "-replica", "http://c", "-since", "20m"})
	if err != nil || o.replicas.String() != "http://a,http://b,http://c" {
		t.Fatalf("replicas %v err %v", o.replicas, err)
	}
	re, err := globRegexp("sys.{web,db}*.cpu.[ab]?")
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{
		"sys.web1.cpu.a1": true, "sys.db.cpu.bx": true, "sys.web.x.cpu.a1": false, "sys.app1.cpu.a1": false, "sys.web1.cpu.c1": false,
	} {
		if re.MatchString(name) != want {
			t.Fatalf("glob match %s: want %v", name, want)
		}
	}
	if _, err := globRegexp("a.{b"); err == nil {
		t.Fatal("unbalanced glob accepted")
	}
}

func TestSelectNames(t *testing.T) {
	local := newFake(t)
	for _, n := range []string{"a.x", "a.y", "b.x"} {
		local.set(n, base, 1)
	}
	o := &options{prefix: "a."}
	c := &client{base: local.srv.URL, http: http.DefaultClient}
	names, err := o.selectNames(context.Background(), c)
	sort.Strings(names)
	if err != nil || strings.Join(names, ",") != "a.x,a.y" {
		t.Fatalf("names %v %v", names, err)
	}
	f := filepath.Join(t.TempDir(), "names")
	_ = os.WriteFile(f, []byte("q.1\n\nq.2\nz.9\n"), 0o600)
	o = &options{namesFile: f, glob: "q.*"}
	if names, err = o.selectNames(context.Background(), c); err != nil || strings.Join(names, ",") != "q.1,q.2" {
		t.Fatalf("names file %v %v", names, err)
	}
}

func TestWriterFailureStopsRun(t *testing.T) {
	local, rep := newFake(t), newFake(t)
	for i := range 40 {
		n := "w." + strconv.Itoa(10+i)
		local.fill(n, base-5*step, func(int64) float64 { return 1 }, func(ts int64) bool { return ts == base+step })
		rep.fill(n, base, func(int64) float64 { return 2 }, nil)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens: every connect fails
	o := testOptions(local, addr, rep)
	o.batch, o.workers, o.flushPt = 1, 1, 1
	began := time.Now()
	_, err = run(context.Background(), o, time.Unix(base+3600, 0))
	if err == nil || !strings.Contains(err.Error(), "write to") {
		t.Fatalf("want a write error, got %v", err)
	}
	if n := len(rep.renders()); n > 3 || time.Since(began) > 10*time.Second {
		t.Fatalf("run kept fetching after the writer died: %d replica requests in %s", n, time.Since(began))
	}
}
