package carbon

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestPendingReadsBeforePersistenceAndInput(t *testing.T) {
	defer zapwriter.Test()()
	root := t.TempDir()
	config := TestConfig(root)
	if err := os.WriteFile(filepath.Join(root, "schemas.conf"), []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	first := New(config)
	if err := first.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	cfg := first.Config
	cfg.Whisper.Compressed = true
	cfg.Whisper.OutOfOrder = true
	cfg.Tags.Enabled = false
	cfg.Dump.Enabled = true
	cfg.Dump.Path = t.TempDir()
	cfg.Dump.RestorePerSecond = 0
	cfg.Udp.Enabled = false
	cfg.Tcp.Enabled = false
	cfg.Pickle.Enabled = false
	cfg.Grpc.Enabled = false
	cfg.Carbonlink.Enabled = false
	cfg.Common.MetricInterval = &Duration{time.Hour}
	reserve := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		return l.Addr().String()
	}
	cfg.Carbonserver.Enabled = true
	cfg.Carbonserver.Listen = reserve()
	cfg.Carbonserver.TrieIndex = true
	cfg.Carbonserver.ConcurrentIndex = true
	cfg.Carbonserver.RealtimeIndex = 100
	cfg.Carbonserver.ScanFrequency = &Duration{20 * time.Millisecond}
	cfg.Carbonserver.FileListCache = filepath.Join(t.TempDir(), "flc")
	cfg.Carbonserver.FileListCacheVersion = 2
	cfg.Carbonserver.QueryCacheEnabled = false
	cfg.Carbonserver.FindCacheEnabled = false
	cfg.Carbonserver.GlobCacheEnabled = false
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer first.Stop()
	stamp := time.Now().Unix() - 60
	first.Cache.Add(points.OnePoint("existing.metric", 1, stamp))
	deadline := time.Now().Add(5 * time.Second)
	for !first.Cache.IsEmpty() || !first.Carbonserver.HasMappedIndex() {
		if time.Now().After(deadline) {
			t.Fatal("first live snapshot did not complete", zapwriter.TestString())
		}
		time.Sleep(10 * time.Millisecond)
	}
	first.Lock()
	first.Persister.Stop()
	first.Persister = nil
	first.Unlock()
	first.Cache.Add(points.OnePoint("existing.metric", 2, stamp+1))
	first.Cache.Add(points.OnePoint("cache.only", 3, stamp+1))
	// These are diverted after the cache dump, including a later value for the
	// same timestamp. Cache-before-WAL order must survive recovery and reads.
	first.FlushTraces = func() {
		first.Cache.Add(points.OnePoint("existing.metric", 4, stamp+1))
		first.Cache.Add(points.OnePoint("wal.only", 5, stamp+1))
	}
	if err := first.DumpStop(); err != nil {
		t.Fatal(err)
	}
	first.FlushTraces = nil
	manifest, err := os.ReadFile(filepath.Join(cfg.Dump.Path, ".pending-points.json"))
	if err != nil {
		t.Fatal("missing checkpoint", err, zapwriter.TestString())
	}
	var saved recovery.Manifest
	if err := json.Unmarshal(manifest, &saved); err != nil {
		t.Fatal(err)
	}
	tail := filepath.Join(cfg.Dump.Path, recovery.TailName(saved.WAL.Name))
	if err := os.WriteFile(tail, points.OnePoint("existing.metric", 9, stamp+2).AppendBinary(nil), 0600); err != nil {
		t.Fatal(err)
	}
	unlockTail, err := handoff.LockExclusive(tail)
	if err != nil {
		t.Fatal(err)
	}
	releaseTail := sync.OnceFunc(func() { _ = unlockTail() })

	next := New(config)
	next.Config = cfg
	cfg.Tcp.Enabled = true
	cfg.Tcp.Listen = reserve()
	cfg.Dump.RestorePerSecond = 1
	cfg.Whisper.MaxUpdatesPerSecond = 1
	done := make(chan error, 1)
	go func() { done <- next.Start() }()
	defer next.Stop()
	defer releaseTail()
	deadline = time.Now().Add(5 * time.Second)
	for !strings.Contains(zapwriter.TestString(), "serving reads from pending checkpoint") {
		select {
		case err := <-done:
			t.Fatal("startup completed before pending reads", err, zapwriter.TestString())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("pending reads did not open", zapwriter.TestString())
		}
		time.Sleep(time.Millisecond)
	}
	read := func() (map[string]float64, []byte, error) {
		url := fmt.Sprintf("http://%s/render/?target=existing.metric&target=cache.only&target=wal.only&target=live.only&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, stamp-1, stamp+3)
		resp, err := http.Get(url)
		if err != nil {
			return nil, nil, err
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, raw, err
		}
		if resp.StatusCode != 200 {
			return nil, raw, fmt.Errorf("read %d: %s", resp.StatusCode, raw)
		}
		var body struct {
			Metrics []struct {
				Name      string
				StartTime int64
				StepTime  int64
				Values    []float64
				IsAbsent  []bool
			}
		}
		if err = json.Unmarshal(raw, &body); err != nil {
			return nil, raw, err
		}
		values := map[string]float64{}
		for _, m := range body.Metrics {
			for i, v := range m.Values {
				if !m.IsAbsent[i] {
					values[fmt.Sprintf("%s/%d", m.Name, m.StartTime+int64(i)*m.StepTime)] = v
				}
			}
		}
		return values, raw, nil
	}
	check := func(extra map[string]float64) {
		t.Helper()
		values, raw, err := read()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]float64{fmt.Sprintf("existing.metric/%d", stamp): 1, fmt.Sprintf("existing.metric/%d", stamp+1): 4, fmt.Sprintf("cache.only/%d", stamp+1): 3, fmt.Sprintf("wal.only/%d", stamp+1): 5}
		for k, v := range extra {
			want[k] = v
		}
		for key, want := range want {
			if got, ok := values[key]; !ok || got != want {
				t.Fatalf("%s = %v/%t, want %v; %s", key, got, ok, want, raw)
			}
		}
	}
	check(nil)
	// The old worker still owns the tail lock. Unrelated checkpoint metrics
	// must remain in the immutable source, without starting persistence while
	// foreground intake waits. Reads continue to see their complete history.
	gateDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(gateDeadline) {
		if _, err := os.Stat(filepath.Join(cfg.Whisper.DataDir, "cache", "only.wsp")); !os.IsNotExist(err) {
			t.Fatal("background persistence started before the input tail was ready", err)
		}
		select {
		case err := <-done:
			t.Fatal("startup passed the locked input tail", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	releaseTail()
	// Input opens with reads, while saved metrics are still outstanding.
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not return after pending reads opened")
	}
	if next.Cache.PendingOutstanding() == 0 {
		t.Fatal("recovery finished before input could overlap it")
	}
	check(map[string]float64{fmt.Sprintf("existing.metric/%d", stamp+2): 9})
	c, err := net.DialTimeout("tcp", cfg.Tcp.Listen, time.Second)
	if err != nil {
		t.Fatal("input did not open with pending reads", err)
	}
	// A live write claims the metric's saved history first; reads and the
	// eventual persisted file must hold both, saved values unchanged.
	if _, err = fmt.Fprintf(c, "existing.metric 6 %d\nwal.only 7 %d\nlive.only 8 %d\n", stamp+2, stamp+2, stamp+2); err != nil {
		t.Fatal(err)
	}
	c.Close()
	liveKey := fmt.Sprintf("live.only/%d", stamp+2)
	live := map[string]float64{fmt.Sprintf("existing.metric/%d", stamp+2): 6, fmt.Sprintf("wal.only/%d", stamp+2): 7, liveKey: 8}
	deadline = time.Now().Add(5 * time.Second)
	for {
		values, raw, err := read()
		if err == nil && values[liveKey] == 8 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("live input not readable: %v; %s", err, raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
	check(live)
	deadline = time.Now().Add(20 * time.Second)
	for !strings.Contains(zapwriter.TestString(), "pending checkpoint persisted") {
		if time.Now().After(deadline) {
			t.Fatal("pending recovery stalled", zapwriter.TestString())
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries, err := os.ReadDir(cfg.Dump.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "cache.") || strings.HasPrefix(e.Name(), "input.") {
			t.Fatal("recovery source not retired", e.Name())
		}
	}
	deadline = time.Now().Add(20 * time.Second)
	for !next.Cache.IsEmpty() {
		if time.Now().After(deadline) {
			t.Fatal("cache did not drain")
		}
		time.Sleep(10 * time.Millisecond)
	}
	check(live)
}

func TestScanGateOpensAtLimit(t *testing.T) {
	var limited atomic.Bool
	gate, _, stop := newScanGate(20*time.Millisecond, func() { limited.Store(true) })
	defer stop()
	select {
	case <-gate:
	case <-time.After(5 * time.Second):
		t.Fatal("gate did not open at its limit")
	}
	if !limited.Load() {
		t.Fatal("limit callback did not run before the gate opened")
	}

	limited.Store(false)
	gate, open, stop := newScanGate(50*time.Millisecond, func() { limited.Store(true) })
	stop()
	open()
	open()
	<-gate
	time.Sleep(100 * time.Millisecond)
	if limited.Load() {
		t.Fatal("stopped limit still fired")
	}
}
