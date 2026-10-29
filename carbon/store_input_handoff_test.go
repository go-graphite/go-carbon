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

// storeHandoffSuccessor is a stand-in for the next binary's capability flags,
// optionally including shared-store input handoff.
func storeHandoffSuccessor(t *testing.T, storeInput bool) string {
	t.Helper()
	capabilities := "read,input"
	if storeInput {
		capabilities += ",store-input"
	}
	return filepath.Join(os.Getenv(testHandoffSuccessorDir), capabilities)
}

// readSeries renders target and returns its non-absent values by timestamp.
func readSeries(t *testing.T, listen, target string, from, until int64) map[int64]float64 {
	t.Helper()
	url := fmt.Sprintf("http://%s/render/?target=%s&from=%d&until=%d&format=json", listen, target, from, until)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return decodeSeries(t, resp)
}

func decodeSeries(t *testing.T, resp *http.Response) map[int64]float64 {
	t.Helper()
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

// TestSharedStoreRestartHandsInputOver restarts a shared store (no mirrored
// files) while clients keep writing on the same connections. Reads cannot move
// with the database, but input must: no write may fail and every point must
// survive. Without support, or when the successor fails to take input, input
// closes as before, and every point read before that still survives through
// the tail log, without gaps.
func testSharedStoreRestartHandsInputOver(t *testing.T) {
	for _, tc := range []struct {
		name       string
		enabled    bool
		storeInput bool
		// successor: "start" runs the next instance; "abandon" registers,
		// asks for the listener and goes away; "silent" registers and never
		// asks. The last two start the real successor afterwards.
		successor  string
		handedOver bool
	}{
		{"store input handoff", true, true, "start", true},
		{"disabled", false, true, "start", false},
		{"successor without support", true, false, "start", false},
		{"successor gone before taking input", true, true, "abandon", false},
		{"successor never asks", true, true, "silent", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if isolatedTestProcess(t) {
				return
			}
			defer zapwriter.Test()()
			if tc.successor == "silent" {
				saved := inputHandoffTimeout
				inputHandoffTimeout = time.Second
				t.Cleanup(func() { inputHandoffTimeout = saved })
			}
			root := t.TempDir()
			cfg := handoffTestConfig(t, root)
			// A converted store as in production: incremental migration over
			// an exact shared store, conversion paused.
			cfg.Whisper.StorageBackend = "pebble-chunk"
			cfg.Whisper.StoreEncoding = "exact"
			cfg.Whisper.StoreIncrementalMigration = true
			cfg.Whisper.FLock = true
			cfg.Whisper.StoreDir = t.TempDir()
			cfg.Whisper.StoreCacheSize = 8 << 20
			cfg.Whisper.StoreMemTableSize = 8 << 20
			cfg.Whisper.StoreMigrationWorkers = 1
			cfg.Whisper.StoreMigrationMinFreeBytes = 0
			cfg.Whisper.StoreMigrationBytesInFlight = 8 << 20
			cfg.Whisper.StoreMigrationControlFile = filepath.Join(root, "migration-control.json")
			cfg.Buckyd.Enabled = false
			if err := os.MkdirAll(cfg.Whisper.DataDir, 0750); err != nil {
				t.Fatal(err)
			}
			incrementalLease(t, cfg, 1, 0)
			cfg.Dump.StoreInputHandoff = tc.enabled

			first := New(filepath.Join(root, "go-carbon.conf"))
			first.Config = cfg
			cfg = startHandoffTestApp(t, first)
			defer first.Stop()
			if first.MetricStore == nil || first.Config.Whisper.StoreMirrorFiles {
				t.Fatal("not a shared store")
			}
			deadline := time.Now().Add(5 * time.Second)
			// Completed conversion uses the catalog index; input-only handoff
			// does not require a memory-mapped filesystem snapshot.
			for first.Carbonserver.CurrentFileIndex() == nil {
				if time.Now().After(deadline) {
					t.Fatal("initial index did not complete", zapwriter.TestString())
				}
				time.Sleep(10 * time.Millisecond)
			}

			next := New(filepath.Join(root, "go-carbon.conf"))
			nextConfig := *cfg
			nextConfig.Cache.MaxSize = handoffRecoveryCacheSize
			next.Config = &nextConfig
			defer next.Stop()
			nextStarted := make(chan error, 1)
			stopDone := make(chan struct{})
			startNext := func() {
				go func() {
					next.RegisterReadHandoff()
					nextStarted <- next.Start()
				}()
			}
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					if first.MetricStore != nil && tc.handedOver {
						nextStarted <- fmt.Errorf("released while still holding the database")
						return
					}
					switch tc.successor {
					case "start":
						startNext()
					case "abandon", "silent":
						go func() {
							claim := handoff.Register(readHandoffPath(cfg))
							if claim == nil {
								nextStarted <- fmt.Errorf("no input offer to abandon")
								return
							}
							defer claim.Close()
							if tc.successor == "abandon" {
								if _, _, err := claim.Take(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}); err != nil || !claim.InputOffered() {
									nextStarted <- fmt.Errorf("take: %w, offered %v", err, claim.InputOffered())
									return
								}
								_ = claim.Close()
							} else {
								<-stopDone
								_ = claim.Close()
							}
							// The real replacement starts after this abandoned offer
							// closes, rather than registering against its closing socket.
							<-stopDone
							startNext()
						}()
					}
				})
			}
			successor := storeHandoffSuccessor(t, tc.storeInput)
			first.SetReadHandoff(successor, release)
			// The registered successor is this test process: check the
			// stand-in binary for it instead.
			savedPeer := handoff.PeerExecutable
			handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return successor, nil }
			t.Cleanup(func() { handoff.PeerExecutable = savedPeer })

			const clients, perClient = 6, 1500
			base := time.Now().Unix() - perClient - 10
			var wg sync.WaitGroup
			var failed sync.Map
			var lastMu sync.Mutex
			last := map[int]int{}
			finishWriting := make(chan struct{})
			finishClients := sync.OnceFunc(func() { close(finishWriting) })
			t.Cleanup(finishClients)
			for c := range clients {
				conn, err := net.Dial("tcp", cfg.Tcp.Listen)
				if err != nil {
					t.Fatal(err)
				}
				wg.Go(func() {
					defer conn.Close()
					for seq := range perClient {
						if seq == perClient-32 {
							<-finishWriting
						}
						line := fmt.Sprintf("client%d.metric %d %d\n", c, seq, base+int64(seq))
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
			deadline = time.Now().Add(5 * time.Second)
			for {
				indexed := true
				for c := range clients {
					indexed = indexed && first.Carbonserver.MetricExists(fmt.Sprintf("client%d.metric", c))
				}
				if indexed {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("initial input metrics were not indexed", zapwriter.TestString())
				}
				time.Sleep(10 * time.Millisecond)
			}
			stopped := make(chan error, 1)
			go func() {
				defer close(stopDone)
				err := first.DumpStop()
				finishClients()
				if err == nil {
					// As main does after a stop that did not release: close
					// what the successor would lock, then release.
					first.ReleaseForHandoff()
					release()
				}
				stopped <- err
			}()
			select {
			case err := <-nextStarted:
				if err != nil {
					t.Fatal("next start", err, zapwriter.TestString())
				}
			case <-time.After(30 * time.Second):
				t.Fatal("next did not start", zapwriter.TestString())
			}
			if err := <-stopped; err != nil {
				t.Fatal("dump stop", err)
			}
			if tc.handedOver && first.MetricStore != nil {
				t.Fatal("predecessor kept the database")
			}
			wg.Wait()

			logs := zapwriter.TestString()
			if tc.handedOver {
				failed.Range(func(c, err any) bool { t.Fatalf("client %v write failed: %v", c, err); return false })
				for _, want := range []string{"input handed over", "input handed off to successor", "took over input from stopping instance", "input tail loaded", "persister stopped with input active"} {
					if !strings.Contains(logs, want) {
						t.Fatalf("missing %q: %s", want, logs)
					}
				}
			} else if strings.Contains(logs, "input handed off to successor") {
				t.Fatal("input handed over without support", logs)
			}

			deadline = time.Now().Add(20 * time.Second)
			for c := range clients {
				sent := last[c]
				for {
					got := readSeries(t, cfg.Carbonserver.Listen, fmt.Sprintf("client%d.metric", c), base-1, base+perClient)
					// Every point up to the newest one stored is there, so
					// nothing read before input closed or moved was lost.
					newest, missing := -1, 0
					for seq := 0; seq <= sent; seq++ {
						if _, ok := got[base+int64(seq)]; ok {
							newest = seq
						}
					}
					for seq := 0; seq <= newest; seq++ {
						if v, ok := got[base+int64(seq)]; !ok || v != float64(seq) {
							missing++
						}
					}
					complete := missing == 0 && newest >= 0
					if tc.handedOver {
						complete = complete && newest == sent
					}
					if complete {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("client %d: %d of %d points missing below the newest stored (%d of %d sent)", c, missing, newest+1, newest, sent)
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
			if !tc.handedOver {
				for c := range clients {
					if _, ok := failed.Load(c); !ok {
						t.Fatalf("client %d never saw input close", c)
					}
				}
			}
		})
	}
}

// TestDumpStopStopsPersisterAlongsideWarmup checks that the persister's stop,
// which waits for every in-flight update, no longer waits for the checkpoint
// warmup to finish first, and logs both phases.
func testDumpStopStopsPersisterAlongsideWarmup(t *testing.T) {
	for _, early := range []bool{true, false} {
		t.Run(fmt.Sprintf("early=%v", early), func(t *testing.T) {
			if isolatedTestProcess(t) {
				return
			}
			defer zapwriter.Test()()
			saved := earlyPersisterStop
			earlyPersisterStop = early
			t.Cleanup(func() { earlyPersisterStop = saved; checkpointWarmHook = nil })
			root := t.TempDir()
			cfg := handoffTestConfig(t, root)
			app := New(filepath.Join(root, "go-carbon.conf"))
			app.Config = cfg
			if err := app.Start(); err != nil {
				t.Fatal(err)
			}
			defer app.Stop()
			deadline := time.Now().Add(5 * time.Second)
			for !app.Carbonserver.HasMappedIndex() {
				if time.Now().After(deadline) {
					t.Fatal("snapshot did not complete")
				}
				time.Sleep(10 * time.Millisecond)
			}
			var stoppedDuringWarmup bool
			checkpointWarmHook = func() {
				// Hold the warmup until the persister stop is logged, or 2 s.
				end := time.Now().Add(2 * time.Second)
				for time.Now().Before(end) {
					if strings.Contains(zapwriter.TestString(), "persister stopped with input active") {
						stoppedDuringWarmup = true
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			if err := app.DumpStop(); err != nil {
				t.Fatal(err)
			}
			if stoppedDuringWarmup != early {
				t.Fatalf("persister stopped during warmup = %v, want %v", stoppedDuringWarmup, early)
			}
			logs := zapwriter.TestString()
			for _, want := range []string{"persister stopped with input active", "pending checkpoint names warmed with input active", "grace stop with dump inited"} {
				if !strings.Contains(logs, want) {
					t.Fatalf("missing %q: %s", want, logs)
				}
			}
			app.ReleaseForHandoff()
		})
	}
}
