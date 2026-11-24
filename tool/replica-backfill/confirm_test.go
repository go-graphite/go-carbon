package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// quotaIngest applies a namespace throughput quota to a real TCP receiver.
// Each point over the quota disappears without a transport error, as in the
// cache admission path. The oracle uses the same input without a quota.
type quotaIngest struct {
	mu       sync.Mutex
	start    time.Time
	interval time.Duration
	limit    int
	used     int
	dropped  int
	all      bool
	local    *fakeServer
}

func (q *quotaIngest) add(name string, ts int64, v float64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if time.Since(q.start) >= q.interval {
		q.start, q.used = time.Now(), 0
	}
	if strings.HasPrefix(name, "replay.") && (q.all || q.limit > 0 && q.used >= q.limit) {
		q.dropped++
		return
	}
	q.used++
	q.local.set(name, ts, v)
}

func confirmationFixture(t *testing.T) (*fakeServer, *fakeServer, *options) {
	t.Helper()
	local, replica := newFake(t), newFake(t)
	for i := range 9 {
		ts := int64(base + i*step)
		local.set("replay.metric", ts, math.NaN())
		replica.set("replay.metric", ts, float64(i)+0.123456789)
	}
	o := testOptions(local, "", replica)
	o.lookback, o.workers, o.flushPt = 0, 1, 9
	o.confirmDelay, o.maxBack, o.confirmRetries = 20*time.Millisecond, 80*time.Millisecond, 5
	o.confirmTimeout = 3 * time.Second
	o.checkpoint = filepath.Join(t.TempDir(), "checkpoint.json")
	return local, replica, o
}

func checkpointFor(t *testing.T, path string) checkpointState {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state checkpointState
	if err := json.Unmarshal(b, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestConfirmationRecoversQuotaBurstAgainstOracle(t *testing.T) {
	t.Parallel()
	var oracle []byte
	for _, limited := range []bool{false, true} {
		local, replica, o := confirmationFixture(t)
		q := &quotaIngest{interval: 30 * time.Millisecond, local: local}
		if limited {
			q.limit = 3
		}
		in := newIngest(t, q.add)
		o.ingest = in.ln.Addr().String()
		// Thousands of unrelated live drops must not hold replay checkpoints.
		// No aggregate throttle counter is consulted by the confirmation guard.
		q.mu.Lock()
		q.dropped = 10000
		q.mu.Unlock()
		r := runOK(t, o)
		if r.Write["points_sent"] != 9 || r.Write["points_unconfirmed"] != 0 || r.Metrics["failed_batches"] != 0 || !checkpointFor(t, o.checkpoint).Complete {
			t.Fatalf("report %+v", r)
		}
		if limited && (r.Write["resent_batches"] == 0 || r.Write["points_resent"] == 0 || r.Write["unconfirmed_point_observations"] == 0) {
			t.Fatalf("quota drops were not detected: %+v", r.Write)
		}
		if !limited && r.Write["resent_batches"] != 0 {
			t.Fatalf("unrelated live drops caused retry: %+v", r.Write)
		}
		// JSON preserves the float's exact round trip; compare every timestamp
		// and value byte for byte, including the existing missing boundary slot.
		local.mu.Lock()
		got, gotErr := json.Marshal(local.data)
		local.mu.Unlock()
		replica.mu.Lock()
		want, wantErr := json.Marshal(replica.data)
		replica.mu.Unlock()
		if gotErr != nil || wantErr != nil {
			t.Fatalf("marshal oracle: %v %v", gotErr, wantErr)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("receiver differs from donor:\n%s\n%s", got, want)
		}
		if !limited {
			oracle = got
		} else if !bytes.Equal(got, oracle) {
			t.Fatalf("quota result differs from no-quota oracle:\n%s\n%s", got, oracle)
		}
	}
}

func TestConfirmationPersistentDropsFailAndResume(t *testing.T) {
	t.Parallel()
	local, _, o := confirmationFixture(t)
	q := &quotaIngest{interval: time.Hour, local: local, all: true}
	in := newIngest(t, q.add)
	o.ingest, o.confirmRetries = in.ln.Addr().String(), 1
	r, err := run(context.Background(), o, time.Now())
	if err == nil || r.Error == "" || r.Metrics["failed_batches"] == 0 || r.Write["points_sent"] != 0 || r.Write["points_unconfirmed"] != 9 || r.Write["confirmation_failures"] != 1 || r.Write["resent_batches"] != 1 {
		t.Fatalf("silently accepted drops: %+v, %v", r, err)
	}
	if state := checkpointFor(t, o.checkpoint); state.Complete || state.DoneThrough != "" {
		t.Fatalf("checkpoint skipped dropped batch: %+v", state)
	}
	q.mu.Lock()
	q.all = false
	q.mu.Unlock()
	r = runOK(t, o)
	if r.Write["points_sent"] != 9 || r.Metrics["skipped_checkpoint"] != 0 || !checkpointFor(t, o.checkpoint).Complete {
		t.Fatalf("resume did not repair drops: %+v", r)
	}
}

func TestConfirmationInterruptedMidThrottle(t *testing.T) {
	t.Parallel()
	local, _, o := confirmationFixture(t)
	q := &quotaIngest{interval: time.Hour, local: local, all: true}
	in := newIngest(t, q.add)
	o.ingest = in.ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel when a confirmation observes the missing points, rather than
	// relying on a timing race with the first socket write.
	local.fail = func(_ []string, n int) int {
		if n > 1 {
			cancel()
		}
		return 0
	}
	r, err := run(ctx, o, time.Now())
	if err == nil || r.Write["points_sent"] != 0 {
		t.Fatalf("interrupted replay succeeded: %+v, %v", r, err)
	}
	if state := checkpointFor(t, o.checkpoint); state.Complete || state.DoneThrough != "" {
		t.Fatalf("interrupted throttle advanced checkpoint: %+v", state)
	}
	local.mu.Lock()
	local.fail = nil
	local.mu.Unlock()
	q.mu.Lock()
	q.all = false
	q.mu.Unlock()
	if r := runOK(t, o); r.Write["points_sent"] != 9 {
		t.Fatalf("resume lost points: %+v", r)
	}
}

func TestConfirmationRejectsLegacyCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"from":1,"until":2,"complete":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCheckpoint(path, 1, 2); err == nil || !strings.Contains(err.Error(), "rescan") {
		t.Fatalf("legacy checkpoint could skip silent losses: %v", err)
	}
}

func TestConfirmationHTTPFailureIsNotSuccess(t *testing.T) {
	t.Parallel()
	local, _, o := confirmationFixture(t)
	in := newIngest(t, local.set)
	o.ingest, o.retries = in.ln.Addr().String(), 0
	local.fail = func(_ []string, n int) int {
		if n > 1 {
			return http.StatusServiceUnavailable
		}
		return 0
	}
	r, err := run(context.Background(), o, time.Now())
	if err == nil || r.Write["points_sent"] != 0 || r.Write["confirmation_failures"] != 1 || checkpointFor(t, o.checkpoint).Complete {
		t.Fatalf("failed readback reported success: %+v %v", r, err)
	}
}

func TestReplayExitCode(t *testing.T) {
	if os.Getenv("REPLICA_BACKFILL_TEST_MAIN") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"replica-backfill"}, os.Args[i+1:]...)
				os.Exit(realMain())
			}
		}
		os.Exit(99)
	}
	local, _, o := confirmationFixture(t)
	q := &quotaIngest{interval: time.Hour, local: local, all: true}
	in := newIngest(t, q.add)
	for _, dropping := range []bool{true, false} {
		q.mu.Lock()
		q.all = dropping
		q.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		retries := "3"
		if dropping {
			retries = "0"
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplayExitCode$", "--",
			"-local", o.local, "-replica", o.replicas[0], "-ingest", in.ln.Addr().String(),
			"-from", o.from, "-until", o.until, "-lookback=0", "-progress=0", "-flush-points=9",
			"-confirm-delay=20ms", "-confirm-retries", retries, "-checkpoint", o.checkpoint)
		cmd.Env = append(os.Environ(), "REPLICA_BACKFILL_TEST_MAIN=1")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := cmd.Output()
		cancel()
		var r report
		if decodeErr := json.Unmarshal(stdout, &r); decodeErr != nil {
			t.Fatalf("decode report: %v; stdout=%s stderr=%s", decodeErr, stdout, stderr.String())
		}
		if dropping {
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 1 || r.Error == "" || r.Write["points_sent"] != 0 || !strings.Contains(stderr.String(), "ERROR:") {
				t.Fatalf("quota loss did not fail loudly: report=%+v err=%v stderr=%s", r, err, stderr.String())
			}
		} else if err != nil || r.Error != "" || r.Write["points_sent"] != 9 {
			t.Fatalf("healthy resume failed: report=%+v err=%v stderr=%s", r, err, stderr.String())
		}
	}
}

func TestConfirmationDeadlineBoundsPersistentThrottle(t *testing.T) {
	t.Parallel()
	_, _, o := confirmationFixture(t)
	in := newIngest(t, nil)
	o.ingest = in.ln.Addr().String()
	o.confirmDelay, o.confirmTimeout = time.Second, 30*time.Millisecond
	r, err := run(context.Background(), o, time.Now())
	if err == nil || r.Write["points_sent"] != 0 || checkpointFor(t, o.checkpoint).Complete {
		t.Fatalf("deadline reported success: %+v %v", r, err)
	}
}

func TestConfirmationFlags(t *testing.T) {
	base := []string{"-replica=http://example.invalid", "-since=1h"}
	o, err := parseFlags(base)
	if err != nil || o.confirmRetries != 20 || o.confirmDelay != time.Second || o.confirmTimeout != 10*time.Minute {
		t.Fatalf("unsafe confirmation defaults: %+v %v", o, err)
	}
	for _, flag := range []string{"-confirm-retries=-1", "-confirm-delay=0", "-confirm-delay=31s", "-confirm-timeout=0"} {
		if _, err := parseFlags(append(base, flag)); err == nil {
			t.Fatalf("invalid flag accepted: %s", flag)
		}
	}
}

func TestConfirmationRequiresExactValues(t *testing.T) {
	t.Parallel()
	local, _, o := confirmationFixture(t)
	in := newIngest(t, func(name string, ts int64, v float64) {
		// A present timestamp with a different value is not confirmation.
		local.set(name, ts, v+1)
	})
	o.ingest, o.confirmRetries = in.ln.Addr().String(), 0
	r, err := run(context.Background(), o, time.Now())
	if err == nil || r.Write["points_sent"] != 0 || r.Write["points_unconfirmed"] != 9 || checkpointFor(t, o.checkpoint).Complete {
		t.Fatalf("different values counted as confirmed: %+v %v", r, err)
	}
}

func TestConfirmationBackoffReducesSharedRate(t *testing.T) {
	w := newWriter("unused", 400, 1, nil, 0, 0, 0)
	if got := w.reduceRate(); got != 200 {
		t.Fatalf("rate after first quota event = %v", got)
	}
	if got := w.reduceRate(); got != 100 {
		t.Fatalf("rate after second quota event = %v", got)
	}
	for range 10 {
		w.reduceRate()
	}
	if got := w.limiter.Limit(); got != 1 {
		t.Fatalf("rate floor = %v", got)
	}
	if got := newWriter("unused", 0, 1, nil, 0, 0, 0).reduceRate(); got != 50000 {
		t.Fatalf("unlimited rate was not bounded on retry: %v", got)
	}
}
