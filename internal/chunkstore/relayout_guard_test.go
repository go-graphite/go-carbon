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
	"github.com/lomik/zapwriter"
)

func TestRelayoutGuardSharedPauseResume(t *testing.T) {
	for _, reason := range []string{"low_free_space", "compaction_debt", "disk_usage_error"} {
		t.Run(reason, func(t *testing.T) {
			defer zapwriter.Test()()
			synctest.Test(t, func(t *testing.T) {
				s := &Store{}
				if err := s.SetRelayoutLimits(100, 200); err != nil {
					t.Fatal(err)
				}
				var healthy atomic.Bool
				sample := func() (uint64, uint64, error) {
					if healthy.Load() {
						return 100, 200, nil
					}
					switch reason {
					case "low_free_space":
						return 99, 0, nil
					case "compaction_debt":
						return 1000, 201, nil
					default:
						return 1000, 0, errors.New("statfs unavailable")
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 4)
				for range 4 {
					go func() { done <- s.relayout.guard.wait(ctx, sample) }()
				}
				synctest.Wait()
				stats := s.RelayoutStats()
				if !stats.Paused || stats.PauseReason != reason || stats.Pauses[reason] != 1 {
					t.Fatalf("shared pause: %+v", stats)
				}
				time.Sleep(3 * time.Second)
				synctest.Wait()
				if s.RelayoutStats().Pauses[reason] != 1 {
					t.Fatal("polls counted as new pauses")
				}
				if n := strings.Count(zapwriter.TestString(), "relayout paused"); n != 1 {
					t.Fatalf("WARN count during shared pause: %d", n)
				}
				healthy.Store(true)
				time.Sleep(time.Second)
				for range 4 {
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				stats = s.RelayoutStats()
				if stats.Paused || stats.PauseReason != "" || stats.Pauses[reason] != 1 {
					t.Fatalf("resume: %+v", stats)
				}
				// A second entry counts once, and cancellation wakes without a poll delay.
				healthy.Store(false)
				go func() { done <- s.relayout.guard.wait(ctx, sample) }()
				synctest.Wait()
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if s.RelayoutStats().Pauses[reason] != 2 {
					t.Fatal("second pause not counted")
				}
			})
		})
	}
}

func TestRelayoutGuardReloadWakesWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Store{}
		_ = s.SetRelayoutLimits(100, 200)
		done := make(chan error, 4)
		for range 4 {
			go func() {
				done <- s.relayout.guard.wait(context.Background(), func() (uint64, uint64, error) { return 50, 300, nil })
			}()
		}
		synctest.Wait()
		if !s.RelayoutStats().Paused {
			t.Fatal("not paused")
		}
		if err := s.SetRelayoutLimits(50, 300); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		for range 4 {
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("reload did not wake all workers")
			}
		}
		if s.RelayoutStats().Paused {
			t.Fatal("reload did not resume")
		}
		for _, limits := range [][2]uint64{{0, 1}, {1, 0}} {
			if err := s.SetRelayoutLimits(limits[0], limits[1]); err == nil {
				t.Fatal("zero limit accepted")
			}
		}
	})
}

type relayoutUsageFS struct {
	vfs.FS
	free atomic.Uint64
	fail atomic.Bool
}

func (f *relayoutUsageFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	if f.fail.Load() {
		return vfs.DiskUsage{}, errors.New("injected statfs failure")
	}
	return vfs.DiskUsage{AvailBytes: f.free.Load()}, nil
}

func TestRelayoutLowSpaceStopsCopiesAndCursor(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(time.Duration(workers).String(), func(t *testing.T) {
			fs := &relayoutUsageFS{FS: vfs.NewMem()}
			fs.free.Store(99)
			codec := &controlledRelayoutCodec{}
			s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutWorkers: workers, RelayoutMinFreeBytes: 100})
			defer closeTestStore(t, s)
			for i := 0; i < 8; i++ {
				createTestMetric(t, s, string(rune('a'+i)))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.RelayoutContinuation(ctx, 0) }()
			for deadline := time.Now().Add(5 * time.Second); !s.RelayoutStats().Paused; {
				if time.Now().After(deadline) {
					t.Fatal("not paused")
				}
				time.Sleep(time.Millisecond)
			}
			if st := s.RelayoutStats(); st.Bytes != 0 || st.Cursor != 0 || st.Done || st.Pauses["low_free_space"] != 1 {
				t.Fatalf("work escaped gate: %+v", st)
			}
			fs.free.Store(100 + 32*relayoutTargetBytes)
			if err := s.SetRelayoutLimits(100, DefaultRelayoutMaxCompactionDebt); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("did not resume")
			}
			if st := s.RelayoutStats(); st.Paused || !st.Done || st.Blocks != 8 || st.Cursor != 8 {
				t.Fatalf("resume: %+v", st)
			}
		})
	}
}

func TestRelayoutRechecksSpaceBeforeIngest(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			fs := &relayoutUsageFS{FS: vfs.NewMem()}
			fs.free.Store(100)
			codec := &controlledRelayoutCodec{before: func(context.Context, string) error { fs.free.Store(99); return nil }}
			s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutWorkers: workers, RelayoutMinFreeBytes: 100})
			defer closeTestStore(t, s)
			for i := 0; i < 8; i++ {
				createTestMetric(t, s, fmt.Sprintf("preingest.%d", i))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.RelayoutContinuation(ctx, 0) }()
			for deadline := time.Now().Add(5 * time.Second); ; {
				if s.RelayoutStats().Paused && s.db.Metrics().Snapshots.Count == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("did not pause outside planning snapshots")
				}
				time.Sleep(time.Millisecond)
			}
			if st := s.RelayoutStats(); st.Bytes != 0 || st.Blocks != 0 || st.Cursor != 0 {
				t.Fatalf("copy escaped pre-ingest recheck: %+v", st)
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("paused run did not cancel")
			}
			if st := s.RelayoutStats(); st.Paused || st.Done || st.Cursor != 0 {
				t.Fatalf("cancelled run: %+v", st)
			}
		})
	}
}
