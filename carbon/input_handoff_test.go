//go:build unix

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
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/lomik/zapwriter"
)

// handoffTestConfig is a compressed, checkpointing store with plaintext input.
func handoffTestConfig(t *testing.T, root string) *Config {
	t.Helper()
	config := TestConfig(root)
	if err := os.WriteFile(filepath.Join(root, "schemas.conf"), []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	app := New(config)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	reserve := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		return l.Addr().String()
	}
	cfg := app.Config
	cfg.Whisper.Compressed = true
	// Repeated corrections to an already persisted compressed point need
	// the existing out-of-order policy, independently of restart handoff.
	cfg.Whisper.OutOfOrder = true
	cfg.Tags.Enabled = false
	cfg.Dump.Enabled = true
	// Unix socket paths must fit sockaddr_un, including on macOS where
	// the default testing temporary directory is too long.
	dumpDir, err := os.MkdirTemp("/tmp", "carbon-handoff-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dumpDir) })
	cfg.Dump.Path = dumpDir
	cfg.Dump.RestorePerSecond = 0
	cfg.Udp.Enabled = false
	cfg.Tcp.Enabled = true
	cfg.Tcp.Listen = reserve()
	cfg.Pickle.Enabled = false
	cfg.Grpc.Enabled = false
	cfg.Carbonlink.Enabled = false
	cfg.Common.MetricInterval = &Duration{time.Hour}
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
	return cfg
}

// successorScript is a stand-in for the next binary's capability flags.
func successorScript(t *testing.T, input bool) string {
	t.Helper()
	body := "#!/bin/sh\ncase \"$1\" in\n-handoff-protocol) echo " + handoff.Protocol + ";;\n"
	if input {
		body += "-input-handoff-protocol) echo " + handoff.InputProtocol + ";;\n"
	}
	body += "*) exit 2;;\nesac\n"
	path := filepath.Join(t.TempDir(), "next")
	if err := os.WriteFile(path, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRestartHandsInputOver restarts a store while clients keep writing on the
// same connections. No write may fail, every point must survive, and points
// that arrived after the checkpoint must not overtake older ones.
func TestRestartHandsInputOver(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input bool
	}{{"input handoff", true}, {"reads only", false}} {
		t.Run(tc.name, func(t *testing.T) {
			defer zapwriter.Test()()
			root := t.TempDir()
			cfg := handoffTestConfig(t, root)
			first := New(filepath.Join(root, "go-carbon.conf"))
			first.Config = cfg
			if err := first.Start(); err != nil {
				t.Fatal(err)
			}
			defer first.Stop()
			deadline := time.Now().Add(5 * time.Second)
			for !first.Carbonserver.HasMappedIndex() {
				if time.Now().After(deadline) {
					t.Fatal("first snapshot did not complete", zapwriter.TestString())
				}
				time.Sleep(10 * time.Millisecond)
			}

			next := New(filepath.Join(root, "go-carbon.conf"))
			next.Config = cfg
			nextStarted := make(chan error, 1)
			first.SetReadHandoff(successorScript(t, tc.input), func() {
				// The service manager starts the next instance now.
				go func() {
					next.RegisterReadHandoff()
					nextStarted <- next.Start()
				}()
			})
			defer next.Stop()

			// Each client owns one metric with a point per second, and they all
			// overwrite one shared timestamp of a "latest" metric with their
			// sequence number: the last write must win.
			const clients, perClient = 6, 1500
			base := time.Now().Unix() - perClient - 10
			var wg sync.WaitGroup
			var failed sync.Map
			var lastMu sync.Mutex
			last := map[int]int{}
			for c := range clients {
				conn, err := net.Dial("tcp", cfg.Tcp.Listen)
				if err != nil {
					t.Fatal(err)
				}
				wg.Go(func() {
					defer conn.Close()
					for seq := range perClient {
						line := fmt.Sprintf("client%d.metric %d %d\nlatest.c%d %d %d\n", c, seq, base+int64(seq), c, seq, base)
						if _, err := conn.Write([]byte(line)); err != nil {
							failed.Store(c, err)
							return
						}
						lastMu.Lock()
						last[c] = seq
						lastMu.Unlock()
						time.Sleep(2 * time.Millisecond)
					}
				})
			}
			time.Sleep(300 * time.Millisecond)
			stopped := make(chan error, 1)
			go func() { stopped <- first.DumpStop() }()
			select {
			case err := <-nextStarted:
				if err != nil {
					t.Fatal("next start", err, zapwriter.TestString())
				}
			case <-time.After(20 * time.Second):
				t.Fatal("next did not start", zapwriter.TestString())
			}
			if err := <-stopped; err != nil {
				t.Fatal("dump stop", err)
			}
			wg.Wait()

			logs := zapwriter.TestString()
			if tc.input {
				failed.Range(func(c, err any) bool { t.Fatalf("client %v write failed: %v", c, err); return false })
				for _, want := range []string{"input handed over", "took over input from stopping instance", "input tail loaded"} {
					if !strings.Contains(logs, want) {
						t.Fatalf("missing %q: %s", want, logs)
					}
				}
			} else if strings.Contains(logs, "input handed over") {
				t.Fatal("input handed to a successor without support")
			}

			if !tc.input {
				// Senders see input closed and lose what was in flight.
				for c := range clients {
					if _, ok := failed.Load(c); !ok {
						t.Fatalf("client %d never saw input close", c)
					}
				}
				return
			}
			read := func(target string, from, until int64) map[int64]float64 {
				url := fmt.Sprintf("http://%s/render/?target=%s&from=%d&until=%d&format=json", cfg.Carbonserver.Listen, target, from, until)
				resp, err := http.Get(url)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				raw, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("read status %d: %s", resp.StatusCode, raw)
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
				if err := json.Unmarshal(raw, &body); err != nil {
					t.Fatal(err, string(raw))
				}
				out := map[int64]float64{}
				for _, m := range body.Metrics {
					for i, v := range m.Values {
						if !m.IsAbsent[i] {
							out[m.StartTime+int64(i)*m.StepTime] = v
						}
					}
				}
				return out
			}
			deadline = time.Now().Add(20 * time.Second)
			for c := range clients {
				sent := last[c]
				for {
					got := read(fmt.Sprintf("client%d.metric", c), base-1, base+perClient)
					missing := 0
					for seq := 0; seq <= sent; seq++ {
						if v, ok := got[base+int64(seq)]; !ok || v != float64(seq) {
							missing++
						}
					}
					latest := read(fmt.Sprintf("latest.c%d", c), base-1, base+1)[base]
					if missing == 0 && latest == float64(sent) {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("client %d: %d of %d points missing, latest %v want %d", c, missing, sent+1, latest, sent)
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		})
	}
}
