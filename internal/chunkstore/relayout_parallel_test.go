package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// A small physical fixture with one flush per metric makes the cursor's
// boundaries observable without allocating 64 MiB for every test group.
type controlledRelayoutCodec struct {
	relayoutCodec
	before func(context.Context, string) error
}

func (c *controlledRelayoutCodec) Create(ctx context.Context, io ContinuationIO, m Metadata, now func() time.Time) error {
	if err := c.relayoutCodec.Create(ctx, io, m, now); err != nil {
		return err
	}
	return io.Set([]byte("test-name"), []byte(m.Name))
}

func (c *controlledRelayoutCodec) RelayoutPlan(ctx context.Context, io ContinuationIO) (RelayoutPlan, error) {
	name, err := io.Get([]byte("test-name"))
	if err != nil {
		return RelayoutPlan{}, err
	}
	if c.before != nil {
		if err := c.before(ctx, string(name)); err != nil {
			return RelayoutPlan{}, err
		}
	}
	plan, err := c.relayoutCodec.RelayoutPlan(ctx, io)
	if len(plan.Blocks) > 0 {
		plan.Bytes = relayoutTargetBytes
	}
	return plan, err
}

func TestRelayoutParallelResume(t *testing.T) {
	t.Parallel()
	// Interrupt the beginning and middle of each of three ranges. All other
	// ranges finish, including ranges above a hole in the durable prefix.
	var cases []struct {
		phase   string
		blocked int
	}
	for _, phase := range []string{"plan", "after-ingest"} {
		for _, blocked := range []int{1, 3, 5, 7, 9, 11} {
			cases = append(cases, struct {
				phase   string
				blocked int
			}{phase, blocked})
		}
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%d", tc.phase, tc.blocked), func(t *testing.T) {
			blocked := tc.blocked
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			codec := &controlledRelayoutCodec{}
			opts := Options{ExactBlocks: true, Continuation: codec, TimeBuckets: true, RelayoutWorkers: 3,
				CacheSize: 8 << 20, MemTableSize: 4 << 20, SyncInterval: time.Hour}
			dir := t.TempDir()
			s, _ := openTestStore(t, dir, opts)
			original := make([]Metadata, 12)
			for i := range original {
				original[i] = createTestMetric(t, s, fmt.Sprintf("m.%02d", i+1))
			}
			entered := make(chan struct{})
			codec.before = func(ctx context.Context, name string) error {
				if tc.phase == "plan" && name == original[blocked-1].Name {
					close(entered)
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			var release func()
			if tc.phase == "after-ingest" {
				release = s.lockMetricMutation(original[blocked-1].Name)
				close(entered)
			}
			defer func() {
				if release != nil {
					release()
				}
			}()
			finished := make(chan error, 1)
			go func() { finished <- s.RelayoutContinuation(ctx, 0) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			wantMoved := 12 - (4 - (blocked-1)%4)
			var previous uint64
			for {
				cursor, done, err := s.relayoutCursor()
				if err != nil || done || cursor < previous || cursor >= uint64(blocked) {
					t.Fatalf("unsafe cursor: previous=%d cursor=%d done=%v err=%v", previous, cursor, done, err)
				}
				previous = cursor
				ready := true
				if tc.phase == "after-ingest" {
					_, err := s.newContinuationIO(context.Background(), s.db, nil, original[blocked-1]).Get(ContinuationBucketKey(0, []byte("c")))
					ready = err == nil
				}
				if ready && s.RelayoutStats().Blocks == uint64(wantMoved) && cursor == uint64(blocked-1) {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			cancel()
			if release != nil {
				release()
				release = nil
			}
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatalf("interrupted run: %v", err)
			}
			before := s.RelayoutStats()
			closeTestStore(t, s)
			codec.before = nil
			s, _ = openTestStore(t, dir, opts)
			defer closeTestStore(t, s)
			cursor, done, err := s.relayoutCursor()
			if err != nil || done || cursor < previous {
				t.Fatalf("cursor regressed on reopen: %d -> %d, done=%v err=%v", previous, cursor, done, err)
			}
			if err := s.RelayoutContinuation(context.Background(), 0); err != nil {
				t.Fatal(err)
			}
			after := s.RelayoutStats()
			if !after.Done || after.Cursor != 12 || before.Blocks+after.Blocks != 12 || before.Metrics+after.Metrics != 12 || before.Bytes+after.Bytes != 12*relayoutTargetBytes || after.Skipped != 0 {
				t.Fatalf("moves were lost or repeated: before=%+v after=%+v", before, after)
			}
			for _, old := range original {
				m, err := s.Metadata(context.Background(), old.Name)
				if err != nil || m.Revision != old.Revision+1 {
					t.Fatalf("%s switched more or less than once: old=%+v new=%+v err=%v", old.Name, old, m, err)
				}
				io := s.newContinuationIO(context.Background(), s.db, nil, m)
				if v, err := io.Get([]byte("b")); err != nil || string(v) != "v7" {
					t.Fatalf("%s descriptor: %q %v", m.Name, v, err)
				}
				if _, err := io.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
					t.Fatalf("%s retained old group: %v", m.Name, err)
				}
				groups := 0
				if err := io.Visit(ContinuationBucketKey(0, nil), func(k, v []byte) error {
					groups++
					if string(k) != string(ContinuationBucketKey(0, []byte("c"))) || string(v) != "group" {
						t.Errorf("%s orphan or damaged group: %x %q", m.Name, k, v)
					}
					return nil
				}); err != nil || groups != 1 {
					t.Fatalf("%s groups=%d err=%v", m.Name, groups, err)
				}
			}
		})
	}
}

func TestRelayoutParallelErrorCancelsAndJoins(t *testing.T) {
	t.Parallel()
	for _, panicWorker := range []bool{false, true} {
		t.Run(fmt.Sprint(panicWorker), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			entered := make(chan struct{}, 3)
			var stopped atomic.Int32
			failure := errors.New("injected plan failure")
			codec := &controlledRelayoutCodec{before: func(ctx context.Context, name string) error {
				entered <- struct{}{}
				if name == "a" {
					for range 3 {
						select {
						case <-entered:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					if panicWorker {
						panic(failure)
					}
					return failure
				}
				<-ctx.Done()
				stopped.Add(1)
				return ctx.Err()
			}}
			s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: codec, TimeBuckets: true, RelayoutWorkers: 3})
			defer closeTestStore(t, s)
			for _, name := range []string{"a", "b", "c"} {
				createTestMetric(t, s, name)
			}
			err := s.RelayoutContinuation(ctx, 0)
			if (!panicWorker && !errors.Is(err, failure)) || (panicWorker && (err == nil || !strings.Contains(err.Error(), "panic: injected plan failure"))) {
				t.Fatalf("first error lost: %v", err)
			}
			if stopped.Load() != 2 || s.RelayoutStats().Done || s.relayout.running.Load() {
				t.Fatalf("workers not joined: stopped=%d stats=%+v", stopped.Load(), s.RelayoutStats())
			}
		})
	}
}

// Concurrent flushes share one interval: K workers cannot each spend a full
// rate budget. Rate changes and cancellation still release every waiter.
func TestRelayoutSharedPacing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Store{}
		s.relayout.rate.Store(1 << 20)
		paced, at := uint64(0), time.Now()
		start := at
		result := make(chan error, 1)
		s.relayout.bytes.Add(1 << 20)
		go func() { result <- s.paceRelayout(context.Background(), &paced, &at) }()
		synctest.Wait()
		s.relayout.bytes.Add(3 << 20)
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("other workers' copies escaped pacing: %v", err)
		default:
		}
		if err := <-result; err != nil || time.Since(start) != 4*time.Second || paced != 4<<20 {
			t.Fatalf("shared pacing: bytes=%d err=%v", paced, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.relayout.bytes.Add(1 << 20)
		go func() { result <- s.paceRelayout(ctx, &paced, &at) }()
		synctest.Wait()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("pacing cancellation: %v", err)
		}
	})
}

func TestRelayoutParallelSharesRate(t *testing.T) {
	t.Parallel()
	// Real time is needed here: synctest cannot advance its clock while
	// other workers wait on the pacing mutex. The pacer's arithmetic and
	// reload behavior are tested separately with virtual time.
	codec := &controlledRelayoutCodec{}
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, Continuation: codec, TimeBuckets: true, RelayoutWorkers: 3})
	defer closeTestStore(t, s)
	for i := range 9 {
		createTestMetric(t, s, fmt.Sprintf("m.%d", i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := s.RelayoutContinuation(ctx, 10*relayoutTargetBytes); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 850*time.Millisecond {
		t.Fatalf("nine group budgets at ten per second took only %v with three workers", elapsed)
	}
}

func TestRelayoutWorkerOptions(t *testing.T) {
	for _, workers := range []int{-1, 33} {
		if s, err := Open(t.TempDir(), Options{RelayoutWorkers: workers}); err == nil {
			s.Close()
			t.Fatalf("accepted %d relayout workers", workers)
		}
	}
	for _, workers := range []int{0, 1, 32} {
		s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: relayoutCodec{}, TimeBuckets: true, RelayoutWorkers: workers})
		// Empty ranges and K greater than the candidate count must complete.
		if err := s.RelayoutContinuation(context.Background(), 0); err != nil || !s.RelayoutStats().Done {
			t.Fatalf("empty run with %d workers: %+v %v", workers, s.RelayoutStats(), err)
		}
		closeTestStore(t, s)
	}
}
