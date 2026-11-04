package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	bytes  func(string) int
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
		if c.bytes != nil {
			plan.Bytes = c.bytes(string(name))
		}
	}
	return plan, err
}

func TestRelayoutParallelResume(t *testing.T) {
	t.Parallel()
	// Interrupt chunk boundaries and interiors, including the final short
	// chunk. Other workers must drain the queue above the blocked chunk.
	var cases []struct {
		phase   string
		blocked int
	}
	for _, phase := range []string{"plan", "after-ingest"} {
		for _, blocked := range []int{1, 2, 25, 26, 49} {
			cases = append(cases, struct {
				phase   string
				blocked int
			}{phase, blocked})
		}
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%d", tc.phase, tc.blocked), func(t *testing.T) {
			blocked := tc.blocked
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			codec := &controlledRelayoutCodec{}
			opts := Options{ExactBlocks: true, Continuation: codec, TimeBuckets: true, RelayoutWorkers: 3,
				CacheSize: 8 << 20, MemTableSize: 4 << 20, SyncInterval: time.Hour}
			dir := t.TempDir()
			s, _ := openTestStore(t, dir, opts)
			const metrics = 49 // K=3 gives two candidates per chunk, then one.
			chunkSize := relayoutChunkSize(metrics, 3)
			original := make([]Metadata, metrics)
			// Avoid stripe collisions with the held mutation lock: an unrelated
			// blocked chunk would make the expected drain count ambiguous.
			blockedName := fmt.Sprintf("m.%03d", blocked)
			for i := range original {
				name := fmt.Sprintf("m.%03d", i+1)
				for suffix := 0; i != blocked-1 && s.metricStripe(name) == s.metricStripe(blockedName); suffix++ {
					name = fmt.Sprintf("m.%03d.%d", i+1, suffix)
				}
				original[i] = createTestMetric(t, s, name)
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
			chunkStart := (blocked - 1) / chunkSize * chunkSize
			wantCursor := uint64(chunkStart)
			wantMoved := metrics - (min(chunkStart+chunkSize, metrics) - blocked + 1)
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
				if ready && s.RelayoutStats().Blocks == uint64(wantMoved) && cursor == wantCursor {
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
			if !after.Done || after.Cursor != metrics || before.Blocks+after.Blocks != metrics || before.Metrics+after.Metrics != metrics || before.Bytes+after.Bytes != metrics*relayoutTargetBytes || after.Skipped != 0 {
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

func TestRelayoutParallelPanicCancelsAndJoins(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	entered := make(chan struct{}, 3)
	var stopped atomic.Int32
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
			panic("injected plan failure")
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
	if err == nil || !strings.Contains(err.Error(), "panic: injected plan failure") {
		t.Fatalf("panic lost: %v", err)
	}
	if stopped.Load() != 2 || s.RelayoutStats().Done || s.relayout.running.Load() {
		t.Fatalf("workers not joined: stopped=%d stats=%+v", stopped.Load(), s.RelayoutStats())
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
		// Empty catalogs and K greater than the candidate count must complete.
		if err := s.RelayoutContinuation(context.Background(), 0); err != nil || !s.RelayoutStats().Done {
			t.Fatalf("empty run with %d workers: %+v %v", workers, s.RelayoutStats(), err)
		}
		closeTestStore(t, s)
	}
}

// The oldest eighth is heavy, concentrated entirely in the first static slice.
// Hold K expensive plans at a barrier in each round, and do the same for
// the last K chunks. Each round can progress
// only if all workers are still claiming work; the static scheduler deadlocks
// on its first heavy round because just one worker owns those IDs. This tests
// liveness without wall-time utilisation thresholds or scheduler-speed flakes.
func TestRelayoutStealsSkewedWork(t *testing.T) {
	t.Parallel()
	const workers, metrics, heavy = 4, 128, 16
	finalStart := metrics - workers*relayoutChunkSize(metrics, workers)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var mu sync.Mutex
	var waiting int
	round := make(chan struct{})
	var arrived, busy, peak atomic.Int32
	codec := &controlledRelayoutCodec{before: func(ctx context.Context, name string) error {
		var id int
		if _, err := fmt.Sscanf(name, "skew.%d", &id); err != nil {
			return err
		}
		if id >= heavy && id < finalStart {
			return nil
		}
		active := busy.Add(1)
		defer busy.Add(-1)
		for old := peak.Load(); active > old && !peak.CompareAndSwap(old, active); old = peak.Load() {
		}
		arrived.Add(1)
		mu.Lock()
		ready := round
		waiting++
		if waiting == workers {
			close(round)
			round = make(chan struct{})
			waiting = 0
		}
		mu.Unlock()
		select {
		case <-ready:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	codec.bytes = func(name string) int {
		var id int
		fmt.Sscanf(name, "skew.%d", &id)
		if id < heavy {
			return relayoutTargetBytes
		}
		return 1 << 20
	}
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, Continuation: codec, TimeBuckets: true, RelayoutWorkers: workers, CacheSize: 8 << 20, MemTableSize: 4 << 20})
	defer closeTestStore(t, s)
	for i := range metrics {
		createTestMetric(t, s, fmt.Sprintf("skew.%04d", i))
	}
	if err := s.RelayoutContinuation(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if arrived.Load() != int32(heavy+metrics-finalStart) || peak.Load() != workers || !s.RelayoutStats().Done || s.RelayoutStats().Blocks != metrics || s.RelayoutStats().Bytes != heavy*relayoutTargetBytes+(metrics-heavy)*(1<<20) {
		t.Fatalf("skewed queue: barrier arrivals=%d peak=%d stats=%+v", arrived.Load(), peak.Load(), s.RelayoutStats())
	}
}

// Observe a successful run draining two holes in order. Candidate IDs are
// deliberately sparse: the persisted value is an ID, never a chunk index.
func TestRelayoutCompletedChunkPrefix(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	gates := map[string]chan struct{}{"prefix.000": make(chan struct{}), "prefix.024": make(chan struct{})}
	codec := &controlledRelayoutCodec{before: func(ctx context.Context, name string) error {
		if gate := gates[name]; gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, Continuation: codec, TimeBuckets: true, RelayoutWorkers: 3, CacheSize: 8 << 20, MemTableSize: 4 << 20})
	defer closeTestStore(t, s)
	const metrics = 49 // two-candidate chunks, final chunk length one
	ids := make([]uint64, metrics)
	for i := range metrics {
		// Deleted records leave gaps between continuation metric IDs.
		m := createTestMetric(t, s, fmt.Sprintf("prefix.%03d", i))
		ids[i] = m.ID
		if _, err := s.Create(ctx, MetricConfig{Name: fmt.Sprintf("gap.%03d", i), Retentions: []Retention{{Step: 1, Count: 10}}, AggregationMethod: Average}); err != nil {
			t.Fatal(err)
		}
		// Remove it from the continuation candidate set, leaving its allocated ID.
		if err := s.Delete(ctx, fmt.Sprintf("gap.%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	finished := make(chan error, 1)
	go func() { finished <- s.RelayoutContinuation(ctx, 0) }()
	defer func() {
		cancel()
		for _, gate := range gates {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
		<-finished
	}()
	var previous uint64
	wait := func(blocks uint64, want uint64, done bool) {
		t.Helper()
		for {
			cursor, isDone, err := s.relayoutCursor()
			if err != nil || cursor < previous || cursor > want {
				t.Fatalf("cursor %d -> %d, want <=%d: %v", previous, cursor, want, err)
			}
			previous = cursor
			if cursor == want && isDone == done && s.RelayoutStats().Blocks == blocks {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
	wait(metrics-4, 0, false)
	close(gates["prefix.000"])
	wait(metrics-2, ids[23], false)
	close(gates["prefix.024"])
	// The ceiling includes the final deleted gap ID, beyond the last candidate.
	wait(metrics, ids[metrics-1]+1, true)
}
