package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/lomik/zapwriter"
)

func TestRelayoutMetricFailureContinuesAndRetries(t *testing.T) {
	defer zapwriter.Test()()
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			ctx := context.Background()
			codec := &controlledRelayoutCodec{before: func(_ context.Context, name string) error {
				if name == "m.00" || name == "m.01" {
					return errors.New("missing physical group")
				}
				return nil
			}}
			s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutWorkers: workers})
			s.StopBackground()
			defer closeTestStore(t, s)
			for i := range 20 {
				createTestMetric(t, s, fmt.Sprintf("m.%02d", i))
			}
			err := s.RelayoutContinuation(ctx, 0)
			st := s.RelayoutStats()
			if err == nil || st.Blocks != 18 || st.Errors != 2 || st.Skipped != 2 || st.Done || st.Error == "" || st.Cursor != 0 {
				t.Fatalf("poison metric stopped pass or marked it clean: stats=%+v err=%v", st, err)
			}
			if id, done, err := s.relayoutCursor(); err != nil || done || id != 0 {
				t.Fatalf("failed metrics not retried after reopen: %d %v %v", id, done, err)
			}
			for _, want := range []string{"ERROR", "relayout metric failed", "m.00", "m.01", "metric_id", "block_cursor", "missing physical group"} {
				if !strings.Contains(zapwriter.TestString(), want) {
					t.Fatalf("missing %q in zap log: %s", want, zapwriter.TestString())
				}
			}
			codec.before = nil
			if err := s.RelayoutContinuation(ctx, 0); err != nil {
				t.Fatal(err)
			}
			if st := s.RelayoutStats(); !st.Done || st.Blocks != 20 || st.Errors != 2 || st.Error == "" {
				t.Fatalf("retry lost moves/failure latch: %+v", st)
			}
		})
	}
}

func TestRelayoutApplyMetricFailureDoesNotBlockGroup(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			ctx := context.Background()
			s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: relayoutCodec{}, RelayoutWorkers: workers})
			s.StopBackground()
			defer closeTestStore(t, s)
			// With four workers, 64 candidates put both first metrics in
			// the same two-metric chunk and ingested table.
			var bad Metadata
			for i := range 64 {
				m := createTestMetric(t, s, fmt.Sprintf("m.%03d", i))
				if i == 0 {
					bad = m
				}
			}
			bad.Revision = math.MaxUint64
			if err := s.db.Set(revisionKey(bad), revisionBytes(bad), nil); err != nil {
				t.Fatal(err)
			}
			if err := s.RelayoutContinuation(ctx, 0); err == nil {
				t.Fatal("apply failure was hidden")
			}
			if st := s.RelayoutStats(); st.Blocks != 63 || st.Errors != 1 || st.Skipped != 1 || st.Error == "" || st.Done {
				t.Fatalf("failed apply blocked group: %+v", st)
			}
			io := s.newContinuationIO(ctx, s.db, nil, bad)
			if value, err := io.Get([]byte("b")); err != nil || string(value) != "v6" {
				t.Fatalf("failed apply changed descriptor: %q %v", value, err)
			}
			if value, err := io.Get([]byte("c")); err != nil || string(value) != "group" {
				t.Fatalf("failed apply lost source: %q %v", value, err)
			}
			bad.Revision = 1
			if err := s.db.Set(revisionKey(bad), revisionBytes(bad), nil); err != nil {
				t.Fatal(err)
			}
			if err := s.RelayoutContinuation(ctx, 0); err != nil {
				t.Fatal(err)
			}
			if st := s.RelayoutStats(); st.Blocks != 64 || !st.Done {
				t.Fatalf("failed apply was not retried: %+v", st)
			}
		})
	}
}

func TestRelayoutBackgroundRetriesFailedPass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fail := true
		codec := &controlledRelayoutCodec{before: func(_ context.Context, name string) error {
			if fail && name == "a" {
				return ErrNotFound
			}
			return nil
		}}
		s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: codec})
		s.StopBackground()
		defer closeTestStore(t, s)
		createTestMetric(t, s, "a")
		createTestMetric(t, s, "z")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() { defer close(done); s.runRelayoutBackground(ctx, 1<<40) }()
		synctest.Wait()
		if st := s.RelayoutStats(); st.Blocks != 1 || st.Errors != 1 || st.Done {
			t.Fatalf("first pass stopped at failed metric: %+v", st)
		}
		fail = false // worker is durably blocked on its hourly timer
		if err := s.SetRelayoutRate(1 << 39); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Hour)
		<-done
		if st := s.RelayoutStats(); st.Blocks != 2 || st.Errors != 1 || !st.Done || st.Rate != 1<<39 {
			t.Fatalf("failed metric not retried or reload lost: %+v", st)
		}
	})
}

// Interrupt before the failed pass can reset its cursor. A reopen must still
// scan the failed metric, even after later workers have published their moves.
func TestRelayoutFailureCursorSurvivesInterruption(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			failed, last := make(chan struct{}), make(chan struct{})
			codec := &controlledRelayoutCodec{before: func(ctx context.Context, name string) error {
				switch name {
				case "m.000":
					return errors.New("failed plan")
				case "m.001":
					close(failed)
				case "m.063":
					close(last)
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}}
			opts := Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutWorkers: workers}
			s, _ := openTestStore(t, "/store", opts)
			s.StopBackground()
			defer func() { closeTestStore(t, s) }()
			var bad Metadata
			for i := range 64 {
				m := createTestMetric(t, s, fmt.Sprintf("m.%03d", i))
				if i == 0 {
					bad = m
				}
			}
			result := make(chan error, 1)
			go func() { result <- s.RelayoutContinuation(ctx, 0) }()
			for _, event := range []<-chan struct{}{failed, last} {
				select {
				case <-event:
				case <-ctx.Done():
					t.Fatal("relayout did not reach the final candidate")
				}
			}
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("interrupted run: %v", err)
			}
			if id, done, err := s.relayoutCursor(); err != nil || done || id >= bad.ID {
				t.Fatalf("cursor forgot failed work: id=%d failed=%d done=%v err=%v", id, bad.ID, done, err)
			}
			if st := s.RelayoutStats(); st.Errors != 1 || st.Blocks == 0 {
				t.Fatalf("failure did not retain later progress: %+v", st)
			}
			closeTestStore(t, s)
			codec.before = nil
			s, _ = openTestStore(t, "/store", opts)
			s.StopBackground()
			if err := s.RelayoutContinuation(context.Background(), 0); err != nil {
				t.Fatal(err)
			}
			if !s.RelayoutStats().Done {
				t.Fatal("reopened pass did not finish")
			}
			io := s.newContinuationIO(context.Background(), s.db, nil, bad)
			if value, err := io.Get([]byte("b")); err != nil || string(value) != "v7" {
				t.Fatalf("reopen skipped failed metric: %q %v", value, err)
			}
		})
	}
}
