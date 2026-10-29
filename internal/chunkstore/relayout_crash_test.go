package chunkstore

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// The relayout crash and overlap tests come from an independent review of the
// time-partitioned layout (doc/time-partitioned-layout-review.md), adapted so
// that an overlapping run is refused.

// A relayout killed after ingesting a group's copies, after switching the
// first metric's descriptor, or after advancing its cursor resumes on the
// next open and leaves every metric switched to its copy with the old group
// gone. The child process makes the chosen boundary durable and exits without
// closing the store. The small codec isolates the engine's relayout protocol;
// this is not a power-loss simulation or a physical CWhisper oracle.
func TestRelayoutResumesAfterCrash(t *testing.T) {
	t.Parallel()
	const crashExit = 74
	ctx := context.Background()
	opts := Options{ExactBlocks: true, Continuation: relayoutCodec{}, TimeBuckets: true, CacheSize: 8 << 20, MemTableSize: 4 << 20}
	if phase := os.Getenv("GO_CARBON_TEST_RELAYOUT_CRASH_PHASE"); phase != "" {
		crashRelayoutAt(t, phase, os.Getenv("GO_CARBON_TEST_RELAYOUT_CRASH_DIR"), opts, crashExit)
		return
	}
	for _, phase := range []string{"after-ingest", "after-first-switch", "after-cursor"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			s, _ := openTestStore(t, dir, opts)
			createTestMetric(t, s, "m.first")
			createTestMetric(t, s, "m.second")
			closeTestStore(t, s)
			childCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestRelayoutResumesAfterCrash$")
			cmd.Env = append(os.Environ(), "GO_CARBON_TEST_RELAYOUT_CRASH_PHASE="+phase, "GO_CARBON_TEST_RELAYOUT_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
				t.Fatalf("unexpected child termination: %v\n%s", err, output)
			}
			s, _ = openTestStore(t, dir, opts)
			if err := s.RelayoutContinuation(ctx, 0); err != nil {
				t.Fatal(err)
			}
			closeTestStore(t, s)
			s, _ = openTestStore(t, dir, opts)
			defer closeTestStore(t, s)
			for _, name := range []string{"m.first", "m.second"} {
				m, err := s.Metadata(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				io := s.newContinuationIO(ctx, s.db, nil, m)
				for _, kv := range []struct {
					key   []byte
					value string
				}{{[]byte("b"), "v7"}, {ContinuationBucketKey(0, []byte("c")), "group"}} {
					got, err := io.Get(kv.key)
					if err != nil || string(got) != kv.value {
						t.Fatalf("%s key %x after crash, resume and reopen: %q %v", name, kv.key, got, err)
					}
				}
				if _, err := io.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
					t.Fatalf("%s old group survived a completed relayout: %v", name, err)
				}
			}
			if _, done, err := s.relayoutCursor(); err != nil || !done {
				t.Fatalf("cursor after a completed relayout: done=%v err=%v", done, err)
			}
		})
	}
}

// crashRelayoutAt runs in the child: it starts a relayout, waits for phase,
// syncs the WAL and exits without closing the store.
func crashRelayoutAt(t *testing.T, phase, dir string, opts Options, code int) {
	ctx := context.Background()
	opts.SyncInterval = time.Hour
	s, _ := openTestStore(t, dir, opts)
	first, err := s.Metadata(ctx, "m.first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Metadata(ctx, "m.second")
	if err != nil {
		t.Fatal(err)
	}
	if s.metricStripe(first.Name) == s.metricStripe(second.Name) {
		t.Fatal("fixture metric stripes collide")
	}
	// Holding a metric's lock stops the relayout before that metric's
	// descriptor switch; the lock is never released before the exit.
	switch phase {
	case "after-ingest":
		s.lockMetricMutation(first.Name)
	case "after-first-switch":
		s.lockMetricMutation(second.Name)
	}
	finished := make(chan error, 1)
	go func() { finished <- s.RelayoutContinuation(ctx, 0) }()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		ready := false
		switch phase {
		case "after-ingest":
			_, err := s.newContinuationIO(ctx, s.db, nil, first).Get(ContinuationBucketKey(0, []byte("c")))
			ready = err == nil
		case "after-first-switch":
			v, err := s.newContinuationIO(ctx, s.db, nil, first).Get([]byte("b"))
			ready = err == nil && string(v) == "v7"
		case "after-cursor":
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
				ready = true
			default:
			}
		default:
			t.Fatalf("unknown phase %q", phase)
		}
		if ready {
			if err := s.db.LogData(nil, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			os.Exit(code)
		}
	}
	t.Fatal("crash boundary not reached")
}

// overlapRelayoutCodec holds the first relayout inside its plan until
// released and counts the plans made.
type overlapRelayoutCodec struct {
	relayoutCodec
	plans   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (c *overlapRelayoutCodec) RelayoutPlan(ctx context.Context, io ContinuationIO) (RelayoutPlan, error) {
	plan, err := c.relayoutCodec.RelayoutPlan(ctx, io)
	if err != nil || c.plans.Add(1) > 1 {
		return plan, err
	}
	close(c.entered)
	select {
	case <-ctx.Done():
		return RelayoutPlan{}, ctx.Err()
	case <-c.release:
		return plan, nil
	}
}

// Two overlapping runs that both planned the same block would let the second
// re-ingest the copy and then, finding the descriptor switched, delete the
// copy the first had made live. A call made while a run is inside its plan
// is refused before it plans anything, and the run's switch keeps its copy.
func TestRelayoutRefusesOverlappingRun(t *testing.T) {
	t.Parallel()
	codec := &overlapRelayoutCodec{entered: make(chan struct{}), release: make(chan struct{})}
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: codec, TimeBuckets: true})
	defer closeTestStore(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := createTestMetric(t, s, "same.metric")
	first := make(chan error, 1)
	go func() { first <- s.RelayoutContinuation(ctx, 0) }()
	select {
	case <-codec.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := s.RelayoutContinuation(ctx, 0); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("overlapping relayout admitted: %v", err)
	}
	close(codec.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if n := codec.plans.Load(); n != 1 {
		t.Fatalf("%d plans, want only the first run's", n)
	}
	io := s.newContinuationIO(ctx, s.db, nil, m)
	if desc, err := io.Get([]byte("b")); err != nil || string(desc) != "v7" {
		t.Fatalf("descriptor after relayout: %q %v", desc, err)
	}
	if value, err := io.Get(ContinuationBucketKey(0, []byte("c"))); err != nil || string(value) != "group" {
		t.Fatalf("history the descriptor points at: %q %v", value, err)
	}
	// The finished run admits the next one.
	if err := s.RelayoutContinuation(ctx, 0); err != nil {
		t.Fatalf("relayout after the first finished: %v", err)
	}
}
