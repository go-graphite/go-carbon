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
	"testing"
	"time"

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
	if _, err := os.Stat(filepath.Join(cfg.Dump.Path, ".pending-points.json")); err != nil {
		t.Fatal("missing checkpoint", err, zapwriter.TestString())
	}

	next := New(config)
	next.Config = cfg
	cfg.Tcp.Enabled = true
	cfg.Tcp.Listen = reserve()
	cfg.Dump.RestorePerSecond = 1
	cfg.Whisper.MaxUpdatesPerSecond = 1
	done := make(chan error, 1)
	go func() { done <- next.Start() }()
	defer next.Stop()
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
	if c, err := net.DialTimeout("tcp", cfg.Tcp.Listen, 100*time.Millisecond); err == nil {
		c.Close()
		t.Fatal("input opened before old history persisted")
	}
	check := func() {
		t.Helper()
		url := fmt.Sprintf("http://%s/render/?target=existing.metric&target=cache.only&target=wal.only&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, stamp-1, stamp+3)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("read %d: %s", resp.StatusCode, raw)
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
			t.Fatal(err)
		}
		values := map[string]float64{}
		for _, m := range body.Metrics {
			for i, v := range m.Values {
				if !m.IsAbsent[i] {
					values[fmt.Sprintf("%s/%d", m.Name, m.StartTime+int64(i)*m.StepTime)] = v
				}
			}
		}
		for key, want := range map[string]float64{fmt.Sprintf("existing.metric/%d", stamp): 1, fmt.Sprintf("existing.metric/%d", stamp+1): 4, fmt.Sprintf("cache.only/%d", stamp+1): 3, fmt.Sprintf("wal.only/%d", stamp+1): 5} {
			if got, ok := values[key]; !ok || got != want {
				t.Fatalf("%s = %v/%t, want %v; %s", key, got, ok, want, raw)
			}
		}
	}
	check()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("pending recovery stalled")
	}
	check()
	if c, err := net.DialTimeout("tcp", cfg.Tcp.Listen, time.Second); err != nil {
		t.Fatal("input did not open after recovery", err)
	} else {
		c.Close()
	}
	if !next.Cache.IsEmpty() {
		t.Fatal("input opened before recovery drained")
	}
}
