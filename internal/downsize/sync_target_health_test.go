package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type targetHealthTransport func(*http.Request) (*http.Response, error)

func (f targetHealthTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTargetHealthCacheHysteresisAndOverflow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newTargetHealthGate("target", .75, .60)
		var body string
		client := &http.Client{Transport: targetHealthTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})}
		poll := func(points, dropped int) {
			body = fmt.Sprintf("# TYPE cache_points gauge\ncache_points %d\ncache_limit_points 100\ncache_overflow_dropped_points_total %d\n", points, dropped)
			sample, err := readTargetHealth(context.Background(), client, "http://target/metrics")
			if err != nil {
				t.Fatal(err)
			}
			g.update(sample, nil)
		}
		poll(74, 10)
		if err := g.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		poll(75, 10)
		var admitted atomic.Int64
		for range 16 {
			go func() {
				if g.wait(context.Background()) == nil {
					admitted.Add(1)
				}
			}()
		}
		synctest.Wait()
		if admitted.Load() != 0 {
			t.Fatal("admitted overloaded target")
		}
		time.Sleep(20 * time.Second)
		poll(60, 10)
		synctest.Wait()
		if admitted.Load() != 0 {
			t.Fatal("resumed at low threshold instead of below it")
		}
		poll(59, 10)
		synctest.Wait()
		if admitted.Load() != 16 {
			t.Fatal("lost waiting batches", admitted.Load())
		}
		if paused, seconds := g.snapshot(); paused || seconds != 20 {
			t.Fatal(paused, seconds)
		}
		poll(10, 11) // Fresh overflow takes precedence even at low occupancy.
		if paused, _ := g.snapshot(); !paused {
			t.Fatal("overflow increase did not pause")
		}
		time.Sleep(10 * time.Second)
		poll(10, 11)
		if paused, seconds := g.snapshot(); paused || seconds != 30 {
			t.Fatal(paused, seconds)
		}
		poll(10, 0) // A process restart is not new overflow.
		if paused, _ := g.snapshot(); paused {
			t.Fatal("counter reset paused target")
		}
		poll(10, 1)
		g.update(targetHealthSample{}, errors.New("endpoint unreachable"))
		if err := g.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if paused, _ := g.snapshot(); paused {
			t.Fatal("stale health blocked work")
		}
	})
}

func TestTargetHealthMonitorFailureReleasesWaiters(t *testing.T) {
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			http.Error(w, "unavailable", 503)
			return
		}
		fmt.Fprint(w, "cache_points 80\ncache_limit_points 100\ncache_overflow_dropped_points_total 0\n")
	}))
	defer server.Close()
	g := newTargetHealthGate(server.URL, .75, .60)
	stop := g.monitor(context.Background(), server.URL, 10*time.Millisecond)
	defer stop()
	if paused, _ := g.snapshot(); !paused {
		t.Fatal("initial sample did not gate work")
	}
	done := make(chan error, 1)
	go func() { done <- g.wait(context.Background()) }()
	unavailable.Store(true)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitor failure left batch blocked")
	}
	// The same failure on the first poll must allow work immediately too.
	other := newTargetHealthGate(server.URL, .75, .60)
	stopOther := other.monitor(context.Background(), server.URL, time.Hour)
	defer stopOther()
	if err := other.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTargetHealthMonitorUnreachableDoesNotBlock(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	endpoint := server.URL
	server.Close()
	g := newTargetHealthGate(endpoint, .75, .60)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stop := g.monitor(ctx, endpoint, time.Hour)
	defer stop()
	if err := g.wait(ctx); err != nil {
		t.Fatal("unreachable health endpoint blocked admission", err)
	}
}

func TestTargetHealthReadRejectsInvalidMetrics(t *testing.T) {
	for _, body := range []string{
		"cache_points 75\ncache_limit_points 100\n",
		"cache_points 75\ncache_limit_points 0\ncache_overflow_dropped_points_total 0\n",
		"cache_points NaN\ncache_limit_points 100\ncache_overflow_dropped_points_total 0\n",
		"cache_points 75\ncache_points 75\ncache_limit_points 100\ncache_overflow_dropped_points_total 0\n",
		"cache_points 75\ncache_limit_points 100\ncache_overflow_dropped_points_total -1\n",
	} {
		client := &http.Client{Transport: targetHealthTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})}
		if _, err := readTargetHealth(context.Background(), client, "http://target/metrics"); err == nil {
			t.Fatal("accepted", body)
		}
	}
	client := &http.Client{Timeout: 10 * time.Millisecond, Transport: targetHealthTransport(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	if _, err := readTargetHealth(context.Background(), client, "http://target/metrics"); err == nil {
		t.Fatal("hung health endpoint did not time out")
	}
}

func TestTargetHealthBatchAdmissionAndReadBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newTargetHealthGate("target", .75, .60)
		g.update(targetHealthSample{points: 75, limit: 100}, nil)
		b := newSyncLiveBackend(syncLiveOptions{})
		b.health, b.readTimeout = g, time.Second
		var calls atomic.Int64
		b.m.client.Transport = targetHealthTransport(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("[]"))}, nil
		})
		done := make(chan error, 1)
		go func() {
			_, err := b.Digest(context.Background(), "http://target:4242", []string{"kept.name"}, 0, 100)
			done <- err
		}()
		synctest.Wait()
		time.Sleep(30 * time.Minute)
		if calls.Load() != 0 {
			t.Fatal("submitted verification while paused")
		}
		g.update(targetHealthSample{points: 59, limit: 100}, nil)
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal("health consumed read timeout", err)
		}
		if calls.Load() != 1 {
			t.Fatal("batch was skipped or duplicated", calls.Load())
		}
	})
}

func TestTargetHealthCopyAndNativeBatchCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newTargetHealthGate("target", .75, .60)
		g.update(targetHealthSample{points: 90, limit: 100}, nil)
		b := newSyncLiveBackend(syncLiveOptions{})
		b.health = g
		c := &planFreeCopier{o: copyOptions{health: g}}
		for _, run := range []func(context.Context) error{
			func(ctx context.Context) error { return c.send(ctx, "source", []string{"kept.name"}, false) },
			func(ctx context.Context) error { return b.executeWindowRange(ctx, nil, task{}, []string{"kept.name"}) },
			func(ctx context.Context) error {
				_, err := b.Copy(ctx, "source", "target", []string{"kept.name"}, "unused")
				return err
			},
		} {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- run(ctx) }()
			synctest.Wait()
			select {
			case err := <-done:
				t.Fatal("batch did not wait", err)
			default:
			}
			cancel()
			synctest.Wait()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		snap := (&planFreeCopier{o: copyOptions{health: g}, progress: newCopyProgress()}).progressSnapshot()
		if !snap.WaitingForTargetHealth {
			t.Fatal("copy progress omitted health wait")
		}
	})
}

func TestTargetHealthProgressFile(t *testing.T) {
	g := newTargetHealthGate("target", .75, .60)
	g.update(targetHealthSample{points: 80, limit: 100}, nil)
	path := filepath.Join(t.TempDir(), "progress.json")
	stop := startProgressFile(context.Background(), path, func() progressSnapshot {
		p := progressSnapshot{Phase: "verify:1"}
		p.WaitingForTargetHealth, p.TargetHealthPauseSeconds = g.snapshot()
		return p
	}, func(err error) { t.Error(err) })
	defer stop(nil)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot progressSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.WaitingForTargetHealth || snapshot.TargetHealthPauseSeconds <= 0 {
		t.Fatal(string(data))
	}
}

func TestTargetHealthCopyResumesWithoutSkipping(t *testing.T) {
	f, from, to := newFakeCopy(t, []string{"kept.a", "kept.b"})
	o := copyTestOptions(t, from, to)
	o.streams, o.maxNames = 1, 1
	o.health = newTargetHealthGate(to, .75, .60)
	first := make(chan struct{}, 2)
	o.onCommitted = func(int) { first <- struct{}{} }
	f.inject = func(attempt int, _ []string) int {
		if attempt == 1 {
			o.health.update(targetHealthSample{points: 90, limit: 100}, nil)
		}
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- planFreeCopy(ctx, o) }()
	select {
	case <-first:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The first batch finishes while paused; a second batch cannot enter.
	select {
	case err := <-done:
		t.Fatal("copy did not wait for health", err)
	case <-time.After(50 * time.Millisecond):
	}
	f.mu.Lock()
	attempts := f.attempts
	f.mu.Unlock()
	if attempts != 1 {
		t.Fatal("submitted a new copy while paused", attempts)
	}
	o.health.update(targetHealthSample{points: 59, limit: 100}, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.imported["kept.a"] != 1 || f.imported["kept.b"] != 1 {
		t.Fatal(f.imported)
	}
}

func TestTargetHealthFlagsAndResume(t *testing.T) {
	c, err := parseSyncCommand(syncCLIArgs(t))
	if err != nil {
		t.Fatal(err)
	}
	if !c.health.enabled || c.health.high != .75 || c.health.low != .60 {
		t.Fatal(c.health)
	}
	if got := targetHealthURL("http://[::1]:4242", 7007); !strings.HasPrefix(got, "http://[::1]:7007/metrics?") || !strings.Contains(got, "cache_overflow_dropped_points_total") {
		t.Fatal(got)
	}
	for _, args := range [][]string{
		{"--target-health-high", "NaN"}, {"--target-health-low", "0"}, {"--target-health-low", "0.8"}, {"--target-health-high", "1.1"},
	} {
		if _, err := parseSyncCommand(syncCLIArgs(t, args...)); err == nil {
			t.Fatal("accepted", args)
		}
	}
	if err := os.MkdirAll(c.sync.state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	restored, err := parseSyncCommand([]string{"--resume", c.sync.state, "--target-health=false", "--target-health-high=.8", "--target-health-low=.5"})
	if err != nil {
		t.Fatal(err)
	}
	if restored.health.enabled || restored.health.high != .8 || restored.health.low != .5 {
		t.Fatal(restored.health)
	}
	// Old configs without health flags adopt safe defaults on binary upgrade.
	data, err := os.ReadFile(filepath.Join(c.sync.state, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "target-health") {
		t.Fatal("health policy not saved")
	}
	var legacy savedSyncConfig
	if err := json.Unmarshal(data, &legacy); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"target-health", "target-health-high", "target-health-low"} {
		delete(legacy.Flags, key)
		delete(legacy.Explicit, key)
	}
	data, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.sync.state, "config.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	upgraded, err := parseSyncCommand([]string{"--resume", c.sync.state})
	if err != nil {
		t.Fatal(err)
	}
	if !upgraded.health.enabled || upgraded.health.high != .75 || upgraded.health.low != .60 {
		t.Fatal(upgraded.health)
	}
}
