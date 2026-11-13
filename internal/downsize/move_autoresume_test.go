package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func autoresumeSample(generation string, writePoints uint64) hostSample {
	s := backlogTestSample(0)
	s.Time = float64(time.Now().UnixNano()) / 1e9
	s.Generation, s.WritePoints = generation, writePoints
	return s
}

// A go-carbon restart (new generation, counters back near zero, fewer write
// stalls) re-baselines the host instead of a hard stop; the next sample is
// compared against the new process only.
func TestTunerRestartRebaselinesInsteadOfAborting(t *testing.T) {
	tune := newTestTuner([]string{"a", "b"}, capacity{Streams: 16, Batch: 8192, BatchBytes: 64 << 20, DeleteRate: 10000}, false, 0, 0)
	if err := tune.observe("a", autoresumeSample("pid1:100", 1000000), storageSample{WriteStalls: 7}); err != nil {
		t.Fatal(err)
	}
	before := tune.hosts["a"].Streams
	restarted := autoresumeSample("pid2:200", 10)
	if err := tune.observe("a", restarted, storageSample{WriteStalls: 0}); err != nil {
		t.Fatalf("restart aborted the copy: %v", err)
	}
	h := tune.hosts["a"]
	if h.hard != "" || h.previous == nil || h.previous.Generation != "pid2:200" || h.storePrevious.WriteStalls != 0 {
		t.Fatalf("restart not re-baselined: hard=%q previous=%+v", h.hard, h.previous)
	}
	if h.Streams >= before || h.Streams == 0 {
		t.Fatalf("restart should halve, not stop, streams: %d -> %d", before, h.Streams)
	}
	next := autoresumeSample("pid2:200", 20)
	next.Time = restarted.Time + 1
	if err := tune.observe("a", next, storageSample{WriteStalls: 0}); err != nil {
		t.Fatal(err)
	}
	if h.writeRate != 10 {
		t.Fatalf("write rate after re-baseline = %v, want 10/s from the new process", h.writeRate)
	}
	// Write errors growing inside one process stay a hard stop.
	bad := autoresumeSample("pid2:200", 30)
	bad.Time, bad.WriteErrors = next.Time+1, 1
	if err := tune.observe("a", bad, storageSample{}); err == nil {
		t.Fatal("new write errors accepted")
	}
}

// A relaunch restores the journal's pre-restart Previous sample; the first
// poll against the restarted host must not fail on it.
func TestTunerRestoredPreviousFromOldGenerationDoesNotAbort(t *testing.T) {
	tune := newTestTuner([]string{"a"}, capacity{Streams: 16, Batch: 8192, BatchBytes: 64 << 20, DeleteRate: 10000}, false, 0, 0)
	old := autoresumeSample("pid1:100", 5000000)
	old.Time -= 3600
	tune.hosts["a"].previous, tune.hosts["a"].storePrevious = &old, &storageSample{WriteStalls: 40}
	if err := tune.observe("a", autoresumeSample("pid9:900", 3), storageSample{WriteStalls: 1}); err != nil {
		t.Fatalf("restored pre-restart Previous aborted the relaunch: %v", err)
	}
	if tune.hosts["a"].previous.Generation != "pid9:900" {
		t.Fatal("previous not replaced by the new process sample")
	}
}

// A helper 503 (read histogram reset / missing counters) or a refused status
// parks the host; the next good poll brings it back. Neither fails the run.
func TestPollWaitsForHostThroughHelperAndStatusErrors(t *testing.T) {
	var helperFail, statusFail atomic.Int32
	helperFail.Store(1)
	helper := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if helperFail.Add(-1) >= 0 {
			http.Error(w, "read histogram reset", 503)
			return
		}
		_ = json.NewEncoder(w).Encode(autoresumeSample("pid1:1", 100))
	}))
	defer helper.Close()
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if statusFail.Add(-1) >= 0 {
			http.Error(w, "starting", 503)
			return
		}
		_ = json.NewEncoder(w).Encode(storageSample{Protocol: 1})
	}))
	defer store.Close()
	j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	m := &mover{options: options{tick: time.Millisecond}, planData: &manifest{}, journal: j, client: store.Client(), helpers: map[string]string{store.URL: helper.URL},
		tuner: newTestTuner([]string{store.URL}, capacity{Streams: 8, Batch: 64, BatchBytes: 1 << 20, DeleteRate: 10}, false, 0, 0)}
	h := m.tuner.hosts[store.URL]
	ctx := context.Background()
	if err := m.poll(ctx, []string{store.URL}); err != nil {
		t.Fatalf("helper 503 failed the run: %v", err)
	}
	if h.Streams != 0 || h.downSince.IsZero() {
		t.Fatalf("down host not parked: streams=%d", h.Streams)
	}
	statusFail.Store(1)
	if err := m.poll(ctx, []string{store.URL}); err != nil {
		t.Fatalf("status 503 failed the run: %v", err)
	}
	if err := m.poll(ctx, []string{store.URL}); err != nil {
		t.Fatal(err)
	}
	if h.Streams != 4 || !h.downSince.IsZero() {
		t.Fatalf("host not restored: streams=%d downSince=%v", h.Streams, h.downSince)
	}
	// Transport refusal is transient too.
	store.Close()
	if err := m.poll(ctx, []string{store.URL}); err != nil {
		t.Fatalf("refused status failed the run: %v", err)
	}
	if !transientHostError(fmt.Errorf("%w: x: helper 503", errGuards)) || transientHostError(errors.New("unsupported bulk transfer protocol")) {
		t.Fatal("transientHostError classification")
	}
}

type resilientRig struct {
	m      *mover
	dst    *httptest.Server
	copied sync.Map
	pulls  atomic.Int32
}

// newResilientRig builds a one-stream plan of n names into a fake target whose
// pull handler is fail(attempt) (true = fail as returned by the handler).
func newResilientRig(t *testing.T, n int, fail func(w http.ResponseWriter, attempt int32) bool) *resilientRig {
	t.Helper()
	r := &resilientRig{}
	r.dst = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v1/transfer/pull":
			attempt := r.pulls.Add(1)
			if fail(w, attempt) {
				return
			}
			var body struct{ Names []string }
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			proofs := make([]chunkstore.TransferProof, 0, len(body.Names))
			for _, name := range body.Names {
				proofs = append(proofs, chunkstore.TransferProof{Source: chunkstore.TransferVersion{Name: name}, Target: chunkstore.TransferVersion{Name: name}, Digest: "fixture"})
				r.copied.Store(name, true)
			}
			_ = json.NewEncoder(w).Encode(proofs)
		case "/v1/transfer/verify":
			_, _ = w.Write([]byte(`{"verified":1}`))
		default:
			http.Error(w, "bad", 400)
		}
	}))
	t.Cleanup(r.dst.Close)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := "http://source-a:4242"
	p := &manifest{root: root, PlanID: strings.Repeat("b", 64)}
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	_ = encoder.Encode(planRow{Type: "header", Format: "go-carbon-ring", Version: 1, PlanID: p.PlanID, Source: source, Destination: "dst"})
	for i := range n {
		_ = encoder.Encode(planRow{Type: "move", Metric: fmt.Sprintf("restart.name.%03d", i), EstimatedBytes: 1024})
	}
	_ = encoder.Encode(planRow{Type: "end", Metrics: int64(n), EstimatedBytes: int64(n) * 1024})
	sum := sha256.Sum256(data.Bytes())
	name := fmt.Sprintf("%x.jsonl", sum)
	if err := os.WriteFile(filepath.Join(root, name), data.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	p.Streams = append(p.Streams, route{Path: name, Source: source, Destination: "dst", sourceURL: source, targetURL: r.dst.URL, Metrics: int64(n), EstimatedBytes: int64(n) * 1024, SHA256: fmt.Sprintf("%x", sum)})
	j, err := openJournal(filepath.Join(root, "state"), p.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.close)
	r.m = &mover{options: options{streams: 2, tick: time.Millisecond, stallTimeout: time.Hour}, planData: p, journal: j, client: r.dst.Client(),
		tuner: newTestTuner([]string{source, r.dst.URL}, capacity{Streams: 2, Batch: 4, BatchBytes: 1 << 20, DeleteRate: 1000}, false, 0, 0)}
	return r
}

func (r *resilientRig) run(t *testing.T, ctx context.Context) error {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reported error
	var once sync.Once
	err := r.m.runCopyWorkers(ctx, func(err error) { once.Do(func() { reported = err }); cancel() })
	if reported != nil {
		return reported
	}
	return err
}

// The target drops connections (process gone) and then answers 503 while it
// starts; every in-flight batch is re-pulled and every name is copied.
func TestExecuteResilientRidesThroughTargetRestart(t *testing.T) {
	r := newResilientRig(t, 12, func(w http.ResponseWriter, attempt int32) bool {
		switch {
		case attempt <= 3:
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return true
		case attempt <= 5:
			http.Error(w, "starting", 503)
			return true
		}
		return false
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.run(t, ctx); err != nil {
		t.Fatalf("copy aborted on a target restart: %v", err)
	}
	for i := range 12 {
		if _, ok := r.copied.Load(fmt.Sprintf("restart.name.%03d", i)); !ok {
			t.Fatalf("name %d not copied", i)
		}
	}
}

// The stall clock runs only while both hosts are up: a down target is waited
// for; once it is up and still failing past --stall-timeout, the copy fails
// loudly with errCopyStalled.
func TestExecuteResilientStallsOnlyWhileHostsUp(t *testing.T) {
	r := newResilientRig(t, 2, func(w http.ResponseWriter, attempt int32) bool {
		http.Error(w, "broken", 503)
		return true
	})
	r.m.stallTimeout = 20 * time.Millisecond
	target := r.m.tuner.hosts[r.dst.URL]
	r.m.tuner.hostUnavailable(r.dst.URL, errors.New("refused"))
	// Keep admission open so failures reach the stall clock while "down".
	r.m.tuner.mu.Lock()
	target.Streams = 2
	r.m.tuner.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.run(t, ctx) }()
	select {
	case err := <-done:
		t.Fatalf("down host counted as a stall: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	r.m.tuner.mu.Lock()
	target.downSince = time.Time{}
	r.m.tuner.mu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, errCopyStalled) {
			t.Fatalf("want errCopyStalled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no stall error with both hosts up")
	}
}
