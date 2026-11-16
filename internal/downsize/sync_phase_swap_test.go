package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// Run with separately built pre-change and candidate CLI executables. The old
// process owns the saved state; no file is edited between stop and replacement.
func TestSyncPhaseResumeExecutableSwap(t *testing.T) {
	old, next := os.Getenv("STORECTL_SWAP_OLD"), os.Getenv("STORECTL_SWAP_NEW")
	if old == "" || next == "" {
		t.Skip("set STORECTL_SWAP_OLD and STORECTL_SWAP_NEW to run executable upgrade acceptance")
	}
	for _, phase := range []string{"window retry", "verify round 1"} {
		t.Run(phase, func(t *testing.T) {
			state := filepath.Join(t.TempDir(), "state")
			source, target := nativeControllerStore(t), nativeControllerStore(t)
			names := []string{"swap.a", "swap.b", "swap.c", "swap.d"}
			for i, n := range names {
				addPoints(t, source, n, float64(i+1))
				if phase == "verify round 1" {
					addPoints(t, target, n, float64(i+1))
				}
			}
			entered, release := make(chan struct{}, 4), make(chan struct{})
			wait := func(r *http.Request) bool {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-r.Context().Done():
					return false
				case <-release:
					return true
				}
			}
			srcHandler := nativeControllerService(t, source)
			donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if phase == "window retry" && r.URL.Path == "/v1/native/export" {
					raw, _ := io.ReadAll(r.Body)
					r.Body = io.NopCloser(bytes.NewReader(raw))
					var req struct{ Names []string }
					_ = json.Unmarshal(raw, &req)
					if len(req.Names) > 0 && req.Names[0] >= "swap.c" && !wait(r) {
						return
					}
				}
				srcHandler.ServeHTTP(w, r)
			}))
			defer donor.Close()
			dstHandler := nativeControllerService(t, target)
			survivor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if phase == "verify round 1" && r.URL.Path == "/v1/transfer/digest" {
					raw, _ := io.ReadAll(r.Body)
					r.Body = io.NopCloser(bytes.NewReader(raw))
					var req struct{ Names []string }
					_ = json.Unmarshal(raw, &req)
					if len(req.Names) > 0 && req.Names[0] >= "swap.c" && !wait(r) {
						return
					}
				}
				dstHandler.ServeHTTP(w, r)
			}))
			defer survivor.Close()
			carbon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req protov3.MultiGlobRequest
				raw, _ := io.ReadAll(r.Body)
				_ = req.UnmarshalVT(raw)
				var out protov3.MultiMetricsInfoResponse
				for _, n := range req.Metrics {
					out.Metrics = append(out.Metrics, &protov3.MetricsInfoResponse{Name: n, Retentions: []*protov3.Retention{{SecondsPerPoint: 10, NumberOfPoints: 128}}})
				}
				data, _ := out.MarshalVT()
				_, _ = w.Write(data)
			}))
			defer carbon.Close()
			released := false
			defer func() {
				if !released {
					close(release)
				}
			}()
			u, _ := url.Parse(carbon.URL)
			args := []string{"--source", donor.URL, "--to", survivor.URL, "--state", state, "--window", fmt.Sprintf("gap:%d:%d", testMetricNow-20, testMetricNow), "--no-full-history", "--settle=0s", "--include-short-retention", "--no-ready", "--streams=1", "--verify-batch=2", "--jwt-secret", syncTestJWTSecret(t), "--carbonserver-port", u.Port(), "--transfer-idle-timeout=1m"}
			if phase == "window retry" {
				// A real-shaped old journal: missing selection finished with uncertainty.
				// The old executable resumes it, resolves group one and is killed in two.
				c, err := parseSyncCommand(args)
				if err != nil {
					t.Fatal(err)
				}
				if err = c.saveConfig(); err != nil {
					t.Fatal(err)
				}
				identity, _ := syncIdentity(c.sync)
				j, err := openSyncJournal(filepath.Join(state, "sync.jsonl"), identity)
				if err != nil {
					t.Fatal(err)
				}
				if err = j.append(syncRow{Kind: "discovered", Names: 4, Missing: 4}); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 4; i += 2 {
					r := syncRow{Kind: "window-selected", Source: donor.URL, Round: i/2 + 1, First: names[i], Last: names[i+1], Names: 2, SelectionComplete: true}
					for _, n := range names[i : i+2] {
						r.Skipped = append(r.Skipped, syncSkip{n, syncWindowSelectionReason})
					}
					if err = j.append(r); err != nil {
						t.Fatal(err)
					}
				}
				j.close()
				var union, missing strings.Builder
				for _, n := range names {
					fmt.Fprintf(&union, "%s\t1\t0\n", n)
					fmt.Fprintf(&missing, "%s\t1\n", n)
				}
				if err = os.WriteFile(filepath.Join(state, "union.tsv"), []byte(union.String()), 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(state, "missing.tsv"), []byte(missing.String()), 0600); err != nil {
					t.Fatal(err)
				}
				args = []string{"--resume", state}
			}
			start := func(binary string, args []string) (*exec.Cmd, <-chan error, string) {
				t.Helper()
				path := filepath.Join(t.TempDir(), "client.log")
				f, err := os.Create(path)
				if err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(binary, append([]string{"sync"}, args...)...)
				cmd.Env = append(os.Environ(), "STORECTL_GRAPHITE_ADDR=127.0.0.1:0", "NO_PROXY=*", "no_proxy=*")
				cmd.Stdout, cmd.Stderr = f, f
				if err = cmd.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { e := cmd.Wait(); f.Close(); done <- e; close(done) }()
				t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
				return cmd, done, path
			}
			await := func(done <-chan error, path string) {
				t.Helper()
				select {
				case <-entered:
				case err := <-done:
					data, _ := os.ReadFile(path)
					t.Fatalf("client exited: %v\n%s", err, data)
				case <-time.After(15 * time.Second):
					data, _ := os.ReadFile(path)
					t.Fatalf("phase not reached\n%s", data)
				}
			}
			cmd, done, path := start(old, args)
			await(done, path)
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(8 * time.Second):
				t.Fatal("old executable did not stop")
			}
			prior := redoProgress(t, state)
			if prior.Phase != phase || prior.Done < 2 {
				t.Fatalf("old process did not save a real mid-phase cursor: %+v", prior)
			}
			_, done, path = start(next, []string{"--resume", state})
			await(done, path)
			saved := redoProgress(t, state)
			if saved.Phase != phase || saved.Done < prior.Done {
				t.Fatalf("binary swap regressed %+v -> %+v", prior, saved)
			}
			close(release)
			released = true
			select {
			case err := <-done:
				if err != nil {
					data, _ := os.ReadFile(path)
					t.Fatalf("new executable failed: %v\n%s", err, data)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("new executable did not finish")
			}
			final := redoProgress(t, state)
			for op, count := range final.RedoNames {
				if count != 0 {
					t.Fatalf("%s redo_names=%d", op, count)
				}
			}
			t.Logf("old->new executable swap: phase=%s done=%d->%d redo_names=%v", phase, prior.Done, saved.Done, final.RedoNames)
		})
	}
}

// The pre-change executable publishes the terminal verdict itself. The only
// seeded exception is a durable exclusion predating that invocation; no state
// is edited between executables. Network counters independently reject replay.
func TestSyncReadyNoopExecutableSwap(t *testing.T) {
	old, next := os.Getenv("STORECTL_SWAP_OLD"), os.Getenv("STORECTL_SWAP_NEW")
	if old == "" || next == "" {
		t.Skip("set STORECTL_SWAP_OLD and STORECTL_SWAP_NEW")
	}
	for _, except := range []bool{false, true} {
		t.Run(map[bool]string{false: "READY", true: "READY_EXCEPT"}[except], func(t *testing.T) {
			state := filepath.Join(t.TempDir(), "state")
			source, target := nativeControllerStore(t), nativeControllerStore(t)
			for i := range 100 {
				name := fmt.Sprintf("terminal.%03d", i)
				addPoints(t, source, name, 2)
				addPoints(t, target, name, 2)
			}
			var dataCalls, proofCalls atomic.Int64
			watch := func(h http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/transfer/digest" {
						raw, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							http.Error(w, "body", 500)
							return
						}
						r.Body = io.NopCloser(bytes.NewReader(raw))
						var req struct{ Mode string }
						_ = json.Unmarshal(raw, &req)
						if strings.HasPrefix(req.Mode, "proof-") {
							proofCalls.Add(1)
						} else {
							dataCalls.Add(1)
						}
					} else if r.URL.Path == "/v1/transfer/fetch" || strings.HasPrefix(r.URL.Path, "/v1/native/") {
						dataCalls.Add(1)
					}
					h.ServeHTTP(w, r)
				})
			}
			donor := httptest.NewServer(watch(nativeControllerService(t, source)))
			defer donor.Close()
			survivor := httptest.NewServer(watch(nativeControllerService(t, target)))
			defer survivor.Close()
			carbon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch strings.TrimRight(r.URL.Path, "/") {
				case "/metrics":
					fmt.Fprintf(w, "process_start_time_seconds %d\n", testMetricNow-100)
				case "/render":
					dataCalls.Add(1)
					if r.Method == http.MethodPost {
						var req protov3.MultiFetchRequest
						raw, _ := io.ReadAll(r.Body)
						_ = req.UnmarshalVT(raw)
						var out protov3.MultiFetchResponse
						for _, m := range req.Metrics {
							out.Metrics = append(out.Metrics, &protov3.FetchResponse{Name: m.Name, StartTime: testMetricNow - 20, StopTime: testMetricNow, StepTime: 10, Values: []float64{1, 2}})
						}
						raw, _ = out.MarshalVT()
						_, _ = w.Write(raw)
						return
					}
					_ = json.NewEncoder(w).Encode([]any{map[string]any{"target": r.URL.Query().Get("target"), "datapoints": [][2]float64{{1, float64(testMetricNow - 20)}, {2, float64(testMetricNow - 10)}}}})
				default:
					var req protov3.MultiGlobRequest
					raw, _ := io.ReadAll(r.Body)
					_ = req.UnmarshalVT(raw)
					var out protov3.MultiMetricsInfoResponse
					for _, n := range req.Metrics {
						out.Metrics = append(out.Metrics, &protov3.MetricsInfoResponse{Name: n, ConsolidationFunc: "sum", Retentions: []*protov3.Retention{{SecondsPerPoint: 10, NumberOfPoints: 128}}})
					}
					raw, _ = out.MarshalVT()
					_, _ = w.Write(raw)
				}
			}))
			defer carbon.Close()
			u, _ := url.Parse(carbon.URL)
			args := []string{"--source", donor.URL, "--to", survivor.URL, "--state", state, "--window", fmt.Sprintf("gap:%d:%d", testMetricNow-20, testMetricNow), "--no-full-history", "--settle=0s", "--include-short-retention", "--ready-scope=windows", "--gap-ledger-complete", "--carbonapi-url", carbon.URL, "--process-metrics-port", u.Port(), "--streams=1", "--verify-batch=50", "--jwt-secret", syncTestJWTSecret(t), "--carbonserver-port", u.Port()}
			if except {
				c, err := parseSyncCommand(args)
				if err != nil {
					t.Fatal(err)
				}
				if err = c.saveConfig(); err != nil {
					t.Fatal(err)
				}
				id, _ := syncIdentity(c.sync)
				j, err := openSyncJournal(filepath.Join(state, "sync.jsonl"), id)
				if err != nil {
					t.Fatal(err)
				}
				j.close()
				issues, err := openSyncIssues(state)
				if err != nil {
					t.Fatal(err)
				}
				if err = issues.record([]syncResidual{{syncHole: syncHole{Name: "excluded"}, Reason: "preexisting named exception"}}, "verify", false); err != nil {
					t.Fatal(err)
				}
				if err = issues.checkpoint(); err != nil {
					t.Fatal(err)
				}
				args = []string{"--resume", state}
			}
			run := func(binary string, args []string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, binary, append([]string{"sync"}, args...)...)
				cmd.Env = append(os.Environ(), "STORECTL_GRAPHITE_ADDR=127.0.0.1:0", "NO_PROXY=*", "no_proxy=*")
				return cmd.CombinedOutput()
			}
			logs, err := run(old, args)
			if except {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 3 {
					t.Fatalf("old READY_EXCEPT: %v\n%s", err, logs)
				}
			} else if err != nil {
				t.Fatalf("old READY: %v\n%s", err, logs)
			}
			report, _ := os.ReadFile(filepath.Join(state, "ready.json"))
			prior := redoProgress(t, state)
			calls := dataCalls.Load()
			proofs := proofCalls.Load()
			logs, err = run(next, []string{"--resume", state})
			if err != nil {
				t.Fatalf("replacement: %v\n%s", err, logs)
			}
			saved, _ := os.ReadFile(filepath.Join(state, "ready.json"))
			if !bytes.Equal(report, saved) {
				t.Fatalf("verdict recomputed: %s", logs)
			}
			if dataCalls.Load() != calls {
				t.Fatalf("backend data replayed: %d -> %d\n%s", calls, dataCalls.Load(), logs)
			}
			if proofCalls.Load() <= proofs {
				t.Fatal("replacement did not validate metadata scope")
			}
			final := redoProgress(t, state)
			if final.Phase != "ready" || final.Done != prior.Done || final.PhaseRegressions != 0 {
				t.Fatalf("progress changed: %+v -> %+v", prior, final)
			}
			for _, n := range final.RedoNames {
				if n != 0 {
					t.Fatal(final.RedoNames)
				}
			}
			t.Logf("old->new %s: done=%d backend data calls=0 redo_names=0 phase_regressions=0", readReadyReport(t, state).Verdict, final.Done)
		})
	}
}

// The previous executable itself demonstrates the refusal, then the candidate
// adopts exactly the same journal. No state is edited between the executables.
func TestSyncReceiptResumeExecutableSwap(t *testing.T) {
	old, next := os.Getenv("STORECTL_SWAP_OLD"), os.Getenv("STORECTL_SWAP_NEW")
	if old == "" || next == "" {
		t.Skip("set STORECTL_SWAP_OLD and STORECTL_SWAP_NEW")
	}
	state := filepath.Join(t.TempDir(), "state")
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	for _, n := range []string{"receipt.a", "receipt.b", "receipt.c", "receipt.d", "receipt.e", "receipt.f"} {
		addPoints(t, source, n, 2)
		addPoints(t, target, n, 2)
	}
	var callsMu sync.Mutex
	calls := map[string]int{}
	watch := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/transfer/digest" {
				raw, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(raw))
				var req struct {
					Names []string
					Mode  string
				}
				_ = json.Unmarshal(raw, &req)
				if !strings.HasPrefix(req.Mode, "proof-") {
					p := redoProgress(t, state)
					if p.Phase != "fill" || p.Round != 16 || p.Done < 3854336 {
						t.Errorf("first data request regressed saved position: %+v", p)
					}
					callsMu.Lock()
					for _, n := range req.Names {
						calls[n]++
					}
					callsMu.Unlock()
				}
			}
			h.ServeHTTP(w, r)
		})
	}
	donor := httptest.NewServer(watch(nativeControllerService(t, source)))
	defer donor.Close()
	survivor := httptest.NewServer(watch(nativeControllerService(t, target)))
	defer survivor.Close()
	carbon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req protov3.MultiGlobRequest
		raw, _ := io.ReadAll(r.Body)
		_ = req.UnmarshalVT(raw)
		var out protov3.MultiMetricsInfoResponse
		for _, n := range req.Metrics {
			out.Metrics = append(out.Metrics, &protov3.MetricsInfoResponse{Name: n, Retentions: []*protov3.Retention{{SecondsPerPoint: 10, NumberOfPoints: 128}}})
		}
		raw, _ = out.MarshalVT()
		_, _ = w.Write(raw)
	}))
	defer carbon.Close()
	u, _ := url.Parse(carbon.URL)
	args := []string{"--source", donor.URL, "--to", survivor.URL, "--state", state, "--window", fmt.Sprintf("gap:%d:%d", testMetricNow-20, testMetricNow), "--no-full-history", "--settle=0s", "--include-short-retention", "--no-ready", "--streams=1", "--verify-batch=2", "--jwt-secret", syncTestJWTSecret(t), "--carbonserver-port", u.Port()}
	c, err := parseSyncCommand(args)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	identity, _ := syncIdentity(c.sync)
	j, err := openSyncJournal(filepath.Join(state, "sync.jsonl"), identity)
	if err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: c.sync, j: j, copied: map[string]bool{}, forbid: map[string]bool{}, failedCp: map[string]bool{}}
	seedReceiptResume(t, s, os.Getenv("STORECTL_RECEIPT_SCALE") == "1")
	w := c.sync.windows[0]
	if err = j.append(syncRow{Kind: "verified", Round: 1, First: "receipt.a", Last: "receipt.f", Names: 6, Work: []syncRedoWork{{Keys: []uint64{redoKey("verify", c.sync.to, w.From, w.Until)}, IDs: []syncRedoRange{{0, 5}}}}}); err != nil {
		t.Fatal(err)
	}
	if err = s.savePhase("fill", 16, 3854336, 9537100); err != nil {
		t.Fatal(err)
	}
	j.close()
	run := func(binary string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "sync", "--resume", state)
		cmd.Env = append(os.Environ(), "STORECTL_GRAPHITE_ADDR=127.0.0.1:0", "NO_PROXY=*", "no_proxy=*")
		return cmd.CombinedOutput()
	}
	before, err := run(old)
	if err == nil || !bytes.Contains(before, []byte("lacks unambiguous receipt")) {
		t.Fatalf("old executable did not reproduce refusal: %v\n%s", err, before)
	}
	callsMu.Lock()
	oldCalls := len(calls)
	callsMu.Unlock()
	if oldCalls != 0 {
		t.Fatal("old refusal performed data work")
	}
	after, err := run(next)
	if err != nil {
		t.Fatalf("candidate failed: %v\n%s", err, after)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if len(calls) != 2 || calls["receipt.d"] == 0 || calls["receipt.e"] == 0 {
		t.Fatalf("candidate reverified proven names: %v", calls)
	}
	p := redoProgress(t, state)
	if p.PhaseRegressions != 0 || p.AmbiguousReceiptNames != 2 || p.RedoNames["verify"] != 2 {
		t.Fatalf("bad recovery counters: %+v\n%s", p, after)
	}
	t.Logf("old refusal -> new fill16 done3854336 preserved; data names=%v ambiguous_receipt_names=2 redo_names.verify=2 phase_regressions=0", calls)
}
