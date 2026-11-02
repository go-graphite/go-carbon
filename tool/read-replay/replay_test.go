package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("GO_CARBON_READ_REPLAY_HELPER") == "1" {
		main()
	}
	os.Exit(m.Run())
}

func testConfig() config {
	return config{Target: "http://127.0.0.1:8080", Multiplier: .5, MaxRPS: 100, Concurrency: 8, Duration: 10 * time.Second, Timeout: time.Second, MaxErrorRatio: .05, AbortP99: 2 * time.Second}
}

func fixtureRequests(t *testing.T, offsets ...time.Duration) []request {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var requests []request
	for _, d := range offsets {
		r, err := parseRequest("/render/?target=a%2Ab&format=protobuf&from=1767222000&until=1767225600")
		if err != nil {
			t.Fatal(err)
		}
		r.At, r.Runtime = base.Add(d), 12*time.Millisecond
		requests = append(requests, r)
	}
	return requests
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func response(code int) *http.Response {
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("body"))}
}

func TestPacingAndLooping(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "arrival", true: "fixed"}[fixed], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := testConfig()
				requests := fixtureRequests(t, 0, 200*time.Millisecond, 600*time.Millisecond)
				c.Start, c.End = requests[0].At, requests[0].At.Add(time.Second)
				c.Duration = 4200 * time.Millisecond
				if fixed {
					c.RPS = 5
					c.Duration = time.Second
				}
				s, p, err := makeSchedule(requests, c)
				if err != nil {
					t.Fatal(err)
				}
				var times []time.Duration
				start := time.Now()
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					times = append(times, time.Since(start))
					return response(200), nil
				})}
				report := runReplay(context.Background(), c, requests, s, summary{}, client, io.Discard)
				want := []time.Duration{0, 400 * time.Millisecond, 1200 * time.Millisecond, 2 * time.Second, 2400 * time.Millisecond, 3200 * time.Millisecond, 4 * time.Second}
				if fixed {
					want = []time.Duration{0, 200 * time.Millisecond, 400 * time.Millisecond, 600 * time.Millisecond, 800 * time.Millisecond}
				}
				if len(times) != len(want) {
					t.Fatalf("times %v want %v", times, want)
				}
				for i := range want {
					if times[i] != want[i] {
						t.Fatalf("times %v want %v", times, want)
					}
				}
				if report.Total.Count != uint64(len(want)) || report.Total.DroppedLate != 0 || report.Reason != "duration" {
					t.Fatalf("%+v", report)
				}
				if !fixed && p.RequestedRPS != 1.5 {
					t.Fatal(p)
				}
			})
		})
	}
}

// Blocking stdout models a scheduler stall without real sleeps or fake clocks.
type stalledWriter struct{ stalled bool }

func (w *stalledWriter) Write(p []byte) (int, error) {
	if !w.stalled {
		w.stalled = true
		time.Sleep(550 * time.Millisecond)
	}
	return len(p), nil
}

func TestNoCatchUpAfterStallAndHardRateCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := testConfig()
		c.RPS, c.MaxRPS, c.Duration = 100, 10, 61*time.Second
		requests := fixtureRequests(t, 0)
		s, _, err := makeSchedule(requests, c)
		if err != nil {
			t.Fatal(err)
		}
		var times []time.Time
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			times = append(times, time.Now())
			return response(200), nil
		})}
		report := runReplay(context.Background(), c, requests, s, summary{}, client, &stalledWriter{})
		for i := 1; i < len(times); i++ {
			if times[i].Sub(times[i-1]) < 100*time.Millisecond {
				t.Fatalf("burst: %s", times[i].Sub(times[i-1]))
			}
		}
		if report.Total.DroppedLate == 0 || report.Total.DroppedCap == 0 || len(times) != 605 {
			t.Fatalf("count=%d late=%d cap=%d", len(times), report.Total.DroppedLate, report.Total.DroppedCap)
		}
		var count, drops uint64
		for _, minute := range report.Minutes {
			count += minute.Count
			drops += minute.DroppedLate + minute.DroppedCap
		}
		if len(report.Minutes) != 2 || count != report.Total.Count || drops != report.Total.DroppedLate+report.Total.DroppedCap {
			t.Fatalf("inconsistent minute totals: %+v", report.Minutes)
		}
	})
}

func TestConcurrencyAndFiveSecondDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := testConfig()
		c.RPS, c.Concurrency, c.Duration = 100, 2, time.Second
		requests := fixtureRequests(t, 0)
		s, _, _ := makeSchedule(requests, c)
		var active, peak atomic.Int32
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			n := active.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			<-r.Context().Done()
			active.Add(-1)
			return nil, r.Context().Err()
		})}
		start := time.Now()
		report := runReplay(context.Background(), c, requests, s, summary{}, client, io.Discard)
		if peak.Load() != 2 || report.Total.Started != 2 || report.Total.DroppedBusy != 98 || report.Undrained != 2 || time.Since(start) != 6*time.Second {
			t.Fatalf("peak=%d report=%+v elapsed=%s", peak.Load(), report, time.Since(start))
		}
		synctest.Wait()
		if active.Load() != 0 {
			t.Fatal("requests not cancelled")
		}
	})
}

func TestAbortConditions(t *testing.T) {
	for _, mode := range []string{"http", "transport", "timeout", "latency"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := testConfig()
				c.RPS, c.Duration = 1, time.Minute
				requests := fixtureRequests(t, 0)
				s, _, _ := makeSchedule(requests, c)
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					switch mode {
					case "http":
						return response(503), nil
					case "transport":
						return nil, errors.New("connection failed")
					case "timeout":
						return nil, context.DeadlineExceeded
					default:
						time.Sleep(2100 * time.Millisecond)
						return response(200), nil
					}
				})}
				report := runReplay(context.Background(), c, requests, s, summary{}, client, io.Discard)
				want := "max_error_ratio"
				if mode == "latency" {
					want = "abort_p99"
				}
				if report.Reason != want || report.Total.Count == 0 {
					t.Fatalf("%+v", report)
				}
				if mode == "timeout" && (report.Total.Timeouts != 1 || report.Total.Errors != 0) {
					t.Fatalf("%+v", report.Total)
				}
			})
		})
	}
}

func TestRollingWindowExpiresAndThresholdIsStrict(t *testing.T) {
	c := testConfig()
	c.MaxErrorRatio, c.AbortP99 = .5, time.Second
	now := time.Now()
	var w rolling
	w.add(now, result{Code: 503, Latency: time.Second})
	w.add(now.Add(time.Second), result{Code: 200, Latency: time.Millisecond})
	if reason := w.reason(now.Add(time.Second), c); reason != "" {
		t.Fatal(reason)
	}
	if reason := w.reason(now.Add(60*time.Second), c); reason != "" || w.hist.count != 1 || w.failed != 0 {
		t.Fatalf("%s %+v", reason, w)
	}
	if reason := w.reason(now.Add(61*time.Second), c); reason != "" || w.hist.count != 0 {
		t.Fatalf("%s count=%d", reason, w.hist.count)
	}
	var a accumulator
	for i := 1; i <= 1000; i++ {
		a.add(result{Code: 200, Latency: time.Duration(i) * time.Millisecond, Runtime: time.Duration(i) * time.Microsecond, Bytes: 5})
	}
	m := a.snapshot()
	if m.Count != 1000 || m.Bytes != 5000 || m.Latency.P99 < 990 || m.Latency.P99 > 998 || m.Latency.Max != 1000 || m.LoggedRuntime.Max != 1 {
		t.Fatalf("%+v", m)
	}
}

func TestHTTPIntegrationAndConcurrency(t *testing.T) {
	var active, peak atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if r.UserAgent() != "go-carbon-read-replay" || !strings.Contains(r.RequestURI, "target=a%2Ab&format=protobuf") {
			t.Errorf("request: %s %s", r.UserAgent(), r.RequestURI)
		}
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, strings.Repeat("x", 100000))
	}))
	t.Cleanup(func() { once.Do(func() { close(release) }); server.Close() })
	c := testConfig()
	c.Target, c.RPS, c.MaxRPS, c.Concurrency = server.URL, 1000, 1000, 2
	c.Duration = 30 * time.Second
	client, tr := newClient(c)
	defer tr.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := fixtureRequests(t, 0)
	s, _, _ := makeSchedule(requests, c)
	done := make(chan summary, 1)
	go func() { done <- runReplay(ctx, c, requests, s, summary{}, client, io.Discard) }()
	guard := time.NewTimer(10 * time.Second)
	defer guard.Stop()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-guard.C:
			t.Fatal("requests did not arrive")
		}
	}
	cancel()
	once.Do(func() { close(release) })
	select {
	case report := <-done:
		if peak.Load() != 2 || report.Total.Started != 2 || report.Total.Count != 2 || report.Total.Bytes != 200000 || report.Total.HTTPCodes[200] != 2 || report.Total.Errors != 0 || report.Undrained != 0 {
			t.Fatalf("peak=%d %+v", peak.Load(), report)
		}
	case <-guard.C:
		t.Fatal("did not drain")
	}
}

func TestHTTPTimeoutAndRedirect(t *testing.T) {
	var redirected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("target") {
		case "slow":
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "redirect":
			http.Redirect(w, r, "/render/?target=followed", http.StatusFound)
		default:
			redirected.Add(1)
		}
	}))
	defer server.Close()
	c := testConfig()
	c.Timeout = 50 * time.Millisecond
	client, tr := newClient(c)
	defer tr.CloseIdleConnections()
	r, _ := parseRequest("/render/?target=slow")
	x := send(context.Background(), client, server.URL, r)
	if !x.Timeout || x.Error || x.Code != 200 {
		t.Fatalf("%+v", x)
	}
	r, _ = parseRequest("/render/?target=redirect")
	x = send(context.Background(), client, server.URL, r)
	if x.Code != 302 || !x.failed() || redirected.Load() != 0 {
		t.Fatalf("%+v redirect=%d", x, redirected.Load())
	}
}

func TestCLIDryRunAbortAndOutputProtection(t *testing.T) {
	requests := fixtureRequests(t, 0)
	path := writeLog(t, "hour.json", logEntry(requests[0].At, requests[0].URL, true))
	dest := filepath.Join(t.TempDir(), "summary.json")
	var out, errOut bytes.Buffer
	args := []string{"--dry-run", "--summary", dest, path}
	if code := runCLI(context.Background(), args, &out, &errOut); code != 0 {
		t.Fatalf("code=%d %s", code, &errOut)
	}
	for _, want := range []string{"requests=1", "from_cache=1", "original_rps=1.000000", "planned_rps=0.500000", `target="a*b" count=1`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %s in %s", want, &out)
		}
	}
	if code := runCLI(context.Background(), args, &out, &errOut); code != 1 {
		t.Fatal("overwrote summary")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer server.Close()
	dest = filepath.Join(t.TempDir(), "abort.json")
	if code := runCLI(context.Background(), []string{"--target", server.URL, "--summary", dest, path}, &out, &errOut); code != 3 {
		t.Fatalf("code=%d %s", code, &errOut)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	var report summary
	if err := json.Unmarshal(data, &report); err != nil || report.Reason != "max_error_ratio" || report.Total.HTTPCodes[500] != 1 {
		t.Fatalf("%s %v", data, err)
	}
}

func TestSignalWritesSummary(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			seen := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case seen <- struct{}{}:
				default:
				}
				_, _ = io.WriteString(w, "ok")
			}))
			defer server.Close()
			r := fixtureRequests(t, 0)[0]
			path := writeLog(t, "hour.json", logEntry(r.At, r.URL, false))
			dest := filepath.Join(t.TempDir(), "signal.json")
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "--target", server.URL, "--rps", "10", "--duration", "1h", "--summary", dest, path)
			cmd.Env = append(os.Environ(), "GO_CARBON_READ_REPLAY_HELPER=1")
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			select {
			case <-seen:
			case <-time.After(10 * time.Second):
				t.Fatal("child did not send")
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("%v %s", err, &output)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("child did not exit")
			}
			data, err := os.ReadFile(dest)
			if err != nil {
				t.Fatal(err)
			}
			var report summary
			if err := json.Unmarshal(data, &report); err != nil || report.Reason != "signal" || report.Total.Count == 0 || report.Undrained != 0 || !strings.Contains(output.String(), `"type":"summary"`) {
				t.Fatalf("%s %v output=%s", data, err, &output)
			}
		})
	}
}

func TestInterruptedLoadWritesSummary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := writeLog(t, "hour.json", "{}\n")
	dest := filepath.Join(t.TempDir(), "cancel.json")
	var out, errOut bytes.Buffer
	if code := runCLI(ctx, []string{"--summary", dest, path}, &out, &errOut); code != 0 {
		t.Fatalf("code=%d %s", code, &errOut)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	var report summary
	if err := json.Unmarshal(data, &report); err != nil || report.Reason != "signal" || report.Total.Started != 0 {
		t.Fatalf("%s %v", data, err)
	}
}

func TestInvalidOptions(t *testing.T) {
	for _, mutate := range []func(*config){
		func(c *config) { c.MaxRPS = math.NaN() }, func(c *config) { c.RPS = math.Inf(1) }, func(c *config) { c.MaxErrorRatio = math.NaN() },
		func(c *config) { c.MaxRPS = 1e-100 },
		func(c *config) { c.Concurrency = 0 }, func(c *config) { c.Duration = 0 }, func(c *config) { c.AbortP99 = 0 },
		func(c *config) { c.Target = "http://user:pass@localhost" }, func(c *config) { c.Target = "http://localhost/path" },
		func(c *config) { c.Start = time.Now(); c.End = c.Start.Add(-time.Second) },
	} {
		c := testConfig()
		mutate(&c)
		if c.validate() == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
}
