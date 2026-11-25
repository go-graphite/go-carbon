package downsize

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// Run the real CLI signal handler in a separate process, with stores owned by
// the parent so committed imports and receipts survive the client stopping.
func TestStorectlSignalHelperProcess(t *testing.T) {
	raw := os.Getenv("STORECTL_SIGNAL_TEST_ARGS")
	if raw == "" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		panic(err)
	}
	if err := MainWithMetrics(args, "signal-test"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// Keep process launches outside parallel in-process journal tests: a fork can
// briefly inherit an open flock until exec closes close-on-exec descriptors.
func TestStorectlSignalStopAndResume(t *testing.T) {
	for _, stage := range []string{"copy", "lost-ack", "verify", "backoff"} {
		for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
			t.Run(fmt.Sprintf("%s/%s", stage, sig), func(t *testing.T) {
				// Sibling subprocesses also own live journal locks: launch one
				// signal/resume scenario at a time.
				source, target := nativeControllerStore(t), nativeControllerStore(t)
				// Terminal closeout uses the authenticated maintenance API.
				secret := filepath.Join(t.TempDir(), "secret")
				if err := os.WriteFile(secret, []byte(strings.Repeat("s", 32)), 0600); err != nil {
					t.Fatal(err)
				}
				names := []string{"stop.a", "stop.b", "stop.c", "stop.d"}
				for i, name := range names {
					addPoints(t, source, name, math.Copysign(float64(i), -1))
				}
				var stopping, verifying atomic.Bool
				stopping.Store(true)
				entered := make(chan struct{}, 1)
				notify := func() {
					select {
					case entered <- struct{}{}:
					default:
					}
				}
				handler := nativeControllerService(t, target, secret)
				var pulls atomic.Int64
				survivor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if stopping.Load() {
						if stage == "verify" && verifying.Load() && r.URL.Path == "/v1/transfer/digest" {
							_, _ = io.Copy(io.Discard, r.Body)
							notify()
							<-r.Context().Done()
							return
						}
						if r.URL.Path == "/v1/native/pull" && pulls.Add(1) > 1 {
							switch stage {
							case "copy":
								_, _ = io.Copy(io.Discard, r.Body)
								notify()
								<-r.Context().Done()
								return
							case "lost-ack":
								// Commit atomically, then withhold the durable acknowledgement.
								response := httptest.NewRecorder()
								handler.ServeHTTP(response, r)
								if response.Code != http.StatusOK {
									t.Errorf("pull: %d %s", response.Code, response.Body.String())
								}
								notify()
								<-r.Context().Done()
								return
							case "backoff":
								http.Error(w, "store not ready", http.StatusServiceUnavailable)
								notify()
								return
							}
						}
					}
					handler.ServeHTTP(w, r)
				}))
				defer survivor.Close()
				donor := httptest.NewServer(nativeControllerService(t, source, secret))
				defer donor.Close()
				carbon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/info/" {
						http.NotFound(w, r)
						return
					}
					verifying.Store(true)
					body, _ := io.ReadAll(r.Body)
					var req protov3.MultiGlobRequest
					if err := req.UnmarshalVT(body); err != nil {
						t.Error(err)
						return
					}
					var out protov3.MultiMetricsInfoResponse
					for _, name := range req.Metrics {
						out.Metrics = append(out.Metrics, &protov3.MetricsInfoResponse{Name: name, Retentions: []*protov3.Retention{{SecondsPerPoint: 10, NumberOfPoints: 128}}})
					}
					data, _ := out.MarshalVT()
					_, _ = w.Write(data)
				}))
				defer carbon.Close()
				u, _ := url.Parse(carbon.URL)
				state := filepath.Join(t.TempDir(), "state")
				args := []string{"sync", "--source", donor.URL, "--to", survivor.URL, "--state", state,
					"--window", fmt.Sprintf("gap:%d:%d", testMetricNow-20, testMetricNow), "--no-full-history", "--settle=0s",
					"--include-short-retention", "--streams=1", "--verify-batch=2", "--jwt-secret", secret,
					"--carbonserver-port", u.Port(), "--transfer-idle-timeout=1m"}
				start := func(args []string) (*exec.Cmd, <-chan error, string) {
					t.Helper()
					data, _ := json.Marshal(args)
					logPath := filepath.Join(t.TempDir(), "client.log")
					log, err := os.Create(logPath)
					if err != nil {
						t.Fatal(err)
					}
					cmd := exec.Command(os.Args[0], "-test.run=^TestStorectlSignalHelperProcess$")
					cmd.Env = append(os.Environ(), "STORECTL_SIGNAL_TEST_ARGS="+string(data), "STORECTL_GRAPHITE_ADDR=127.0.0.1:0")
					cmd.Stdout, cmd.Stderr = log, log
					if err := cmd.Start(); err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					go func() { err := cmd.Wait(); _ = log.Close(); done <- err; close(done) }()
					t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
					return cmd, done, logPath
				}
				cmd, done, logPath := start(args)
				select {
				case <-entered:
				case err := <-done:
					data, _ := os.ReadFile(logPath)
					t.Fatalf("client exited before %s: %v\n%s", stage, err, data)
				case <-time.After(20 * time.Second):
					data, _ := os.ReadFile(logPath)
					t.Fatalf("did not reach %s\n%s", stage, data)
				}
				if stage == "backoff" {
					// Wait for the retry loop's second rejection so it is sleeping, rather
					// than merely cancelling the HTTP request that precedes backoff.
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("no retry")
					}
					time.Sleep(50 * time.Millisecond)
				}
				began := time.Now()
				if err := cmd.Process.Signal(sig); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("interrupted copy reported success")
					}
				case <-time.After(9 * time.Second):
					t.Fatal("client did not stop within 9s")
				}
				t.Logf("stop took %s", time.Since(began))
				data, err := os.ReadFile(filepath.Join(state, "progress.json"))
				if err != nil {
					t.Fatal(err)
				}
				var progress progressSnapshot
				if err := json.Unmarshal(data, &progress); err != nil {
					t.Fatal(err)
				}
				if progress.Status != "interrupted" {
					t.Fatalf("progress: %s", data)
				}
				journal, err := os.ReadFile(filepath.Join(state, "sync.jsonl"))
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(journal), `"window-missing"`) {
					t.Fatal("completed first batch was not checkpointed")
				}
				stopping.Store(false)
				_, resumed, logPath := start([]string{"sync", "--resume", state})
				select {
				case err := <-resumed:
					if err != nil {
						data, _ := os.ReadFile(logPath)
						t.Fatalf("resume: %v\n%s", err, data)
					}
				case <-time.After(20 * time.Second):
					t.Fatal("resume timed out")
				}
				for _, name := range names {
					want, err := source.Snapshot(context.Background(), name)
					if err != nil {
						t.Fatal(err)
					}
					got, err := target.Snapshot(context.Background(), name)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(want.Metadata.MetricConfig, got.Metadata.MetricConfig) || !reflect.DeepEqual(want.Archives, got.Archives) {
						t.Fatalf("resume changed data for %s: want %+v got %+v", name, want, got)
					}
					for ai, archive := range want.Archives {
						for pi, point := range archive.Points {
							if math.Float64bits(point.Value) != math.Float64bits(got.Archives[ai].Points[pi].Value) {
								t.Fatalf("resume changed value bits for %s", name)
							}
						}
					}
				}
			})
		}
	}
}

// An unresponsive source must not consume the old 15s cleanup timeout after
// all transfer goroutines have already joined.
func TestNativeSourceCleanupStopBudget(t *testing.T) {
	requested := make(chan struct{}, 1)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Error("expected DELETE", r.Method)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		requested <- struct{}{}
		<-r.Context().Done()
	}))
	defer source.Close()
	client := &http.Client{}
	defer client.CloseIdleConnections()
	c := &nativeController{mover: &mover{client: client}, sessions: map[string]*nativeRemoteSession{source.URL: {id: strings.Repeat("a", 32)}}}
	started := time.Now()
	c.release()
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatal("cleanup exceeded stop budget", elapsed)
	}
	select {
	case <-requested:
	default:
		t.Fatal("source release not attempted")
	}
}
