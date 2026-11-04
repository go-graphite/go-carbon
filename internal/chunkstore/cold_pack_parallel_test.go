package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Pause outside the publication stripe, with independently variable metric
// latency. Virtual time makes the 2 ms validation fence deterministic.
type parallelCursorCodec struct {
	cursorTestCodec
	mu     sync.Mutex
	before func(context.Context, string, uint64) error
}

func (c *parallelCursorCodec) Repack(ctx context.Context, io ContinuationIO, r ColdPackRequest) (RepackStats, uint64, bool, error) {
	name, err := io.Get([]byte("test-name"))
	if err != nil {
		return RepackStats{}, 0, false, err
	}
	if c.before != nil {
		if err := c.before(ctx, string(name), r.Cursor); err != nil {
			return RepackStats{}, 0, false, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stats, next, done, err := c.cursorTestCodec.Repack(ctx, io, r)
	if err != nil {
		err = fmt.Errorf("%s block %d: %w", name, r.Cursor, err)
	}
	return stats, next, done, err
}

func parallelOptions(fs vfs.FS, codec *parallelCursorCodec, workers int) Options {
	o := cursorTestOptions(fs, &codec.cursorTestCodec)
	o.Continuation = codec
	o.ColdPack.Workers, o.ColdPack.CPUFraction, o.ColdPack.BytesPerSecond = workers, 1, 1<<40
	return o
}

func TestColdPackParallelWatermarkAndJoin(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &parallelCursorCodec{}
		o := parallelOptions(vfs.NewMem(), codec, 1)
		s, _ := openTestStore(t, "/store", o)
		s.StopBackground()
		defer closeTestStore(t, s)
		for i := range 12 {
			createTestMetric(t, s, fmt.Sprintf("m.%02d", i))
		}
		for _, workers := range []int{8, 2, 0} {
			entered, release := make(chan string, o.ColdPack.Workers), make(chan struct{})
			codec.before = func(ctx context.Context, name string, _ uint64) error {
				entered <- name
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			before := s.ColdPackStats()
			step := make(chan error, 1)
			go func() { _, err := s.ColdPackStep(context.Background()); step <- err }()
			distinct := make(map[string]bool)
			for range o.ColdPack.Workers {
				name := <-entered
				if distinct[name] {
					t.Fatal("workers share a metric")
				}
				distinct[name] = true
			}
			if st := s.ColdPackStats(); st.ActiveWorkers != o.ColdPack.Workers || !st.Active || st.Cursor != before.Cursor || st.BlockCursor != before.BlockCursor {
				t.Fatalf("in-flight watermark moved: %+v", st)
			}
			next := o.ColdPack
			if workers == 0 {
				next.Mode = "off"
			} else {
				next.Workers = workers
			}
			reloaded, configEntered := make(chan error, 1), make(chan struct{})
			go func() { close(configEntered); reloaded <- s.ConfigureColdPack(next) }()
			<-configEntered
			select {
			case <-reloaded:
				t.Fatal("reload returned with old workers active")
			default:
			}
			close(release)
			if err := <-step; err != nil {
				t.Fatal(err)
			}
			if err := <-reloaded; err != nil {
				t.Fatal(err)
			}
			st := s.ColdPackStats()
			if st.ActiveWorkers != 0 || st.Active {
				t.Fatalf("resize did not join: %+v", st)
			}
			if workers != 0 && (st.Cursor != "m.00" || st.BlockCursor == 0) {
				t.Fatalf("resize lost watermark: %+v", st)
			}
			o.ColdPack = next
		}
		before := s.ColdPackStats()
		if done, err := s.ColdPackStep(context.Background()); !done || err != nil {
			t.Fatal("off policy admitted work")
		}
		if st := s.ColdPackStats(); st.Mode != "off" || st.Blocks != before.Blocks {
			t.Fatalf("published after off: %+v", st)
		}
	})
}

func TestColdPackParallelCrashResume(t *testing.T) {
	// Pebble pools SST write tasks containing channels. All strict-memory
	// recovery trials share one virtual-clock bubble in a fresh process so those
	// channels cannot escape into other tests (including repeated test runs).
	if os.Getenv("COLD_PACK_PARALLEL_CRASH_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestColdPackParallelCrashResume$", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "COLD_PACK_PARALLEL_CRASH_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("recovery subprocess: %v\n%s", err, out)
		}
		return
	}
	pebbleSynctest(t, func(t *testing.T) {
		for seed := int64(0); seed < 12; seed++ {
			func() {
				t.Logf("recovery seed %d", seed)
				strict := vfs.NewStrictMem()
				codec := &parallelCursorCodec{before: func(ctx context.Context, name string, block uint64) error {
					// The first metric is deliberately slower than its successors.
					delay := time.Duration(1+int(name[len(name)-1])%4) * time.Microsecond
					if name == "m.000" {
						delay *= 5
					}
					timer := time.NewTimer(delay)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-timer.C:
						return nil
					}
				}}
				o := parallelOptions(strict, codec, 8)
				o.SyncInterval = time.Hour
				s, _ := openTestStore(t, "/store", o)
				s.StopBackground()
				defer func() { s.Close() }()
				const count = ColdPackPageSize + 9
				names := make([]string, 0, count)
				stripes := make(map[uint64]bool)
				for i := 0; len(names) < count; i++ {
					name := fmt.Sprintf("m.%03d", i)
					stripe := s.metricStripe(name)
					if stripes[stripe] {
						continue
					}
					stripes[stripe] = true
					names = append(names, name)
					createTestMetric(t, s, name)
				}
				// Force periodic checkpointing during this trial, without making the test
				// depend on a real 30-second wait or unsynced fixture creation.
				// Sync the WAL without flushing SSTs: Pebble pools channel-bearing
				// SST write tasks globally, across virtual-clock test bubbles.
				if err := s.db.LogData(nil, pebble.Sync); err != nil {
					t.Fatal(err)
				}
				s.coldPack.checkpointAt = time.Now().Add(-coldPackCheckpointInterval)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go func() {
					timer := time.NewTimer(time.Duration(1+rand.New(rand.NewSource(seed)).Intn(70)) * time.Microsecond)
					defer timer.Stop()
					select {
					case <-ctx.Done():
					case <-timer.C:
						cancel()
					}
				}()
				err := s.RunColdPack(ctx)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected interrupted pass: %v stats=%+v", err, s.ColdPackStats())
				}
				// Sync the canceled wave's low watermark, then do additional unsynced work
				// which must be replayable after power loss.
				if err := s.checkpointColdPackOnStop(); err != nil {
					t.Fatal(err)
				}
				saved := s.coldPack.checkpoint
				for _, name := range names {
					if name > saved.Cursor || name == saved.Cursor && saved.BlockCursor != 0 {
						break
					}
					meta, err := s.Metadata(context.Background(), name)
					if err != nil {
						t.Fatal(err)
					}
					snap := s.db.NewSnapshot()
					io := s.newContinuationIO(context.Background(), snap, nil, meta)
					_, err = io.Get([]byte("packed/2"))
					io.Close()
					snap.Close()
					if err != nil {
						t.Fatalf("checkpoint %q ahead of unfinished %q: %v", saved.Cursor, name, err)
					}
				}
				cursorTestSteps(t, s, 2)
				closeForRecovery(t, s, strict, true)
				codec.before = nil
				s, _ = openTestStore(t, "/store", o)
				s.StopBackground()
				if err := s.RunColdPack(context.Background()); err != nil {
					t.Fatal(err)
				}
				if st := s.ColdPackStats(); st.Passes != 1 || st.ActiveWorkers != 0 || st.PendingMetrics != 0 {
					t.Fatalf("resume: %+v", st)
				}
				if st := s.ColdPackStats(); st.StoredPoints != count*600 || st.StoredMetrics != count {
					t.Fatalf("crash replay lost or double counted points: %+v", st)
				}
				for _, name := range names {
					meta, err := s.Metadata(context.Background(), name)
					if err != nil {
						t.Fatal(err)
					}
					snap := s.db.NewSnapshot()
					io := s.newContinuationIO(context.Background(), snap, nil, meta)
					_, err = io.Get([]byte("packed/2"))
					io.Close()
					snap.Close()
					if err != nil {
						t.Fatalf("resume skipped %s: %v", name, err)
					}
				}
			}()
		}
	})
}

type coldPackFreeFS struct{ vfs.FS }

func (f coldPackFreeFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{AvailBytes: 1 << 40}, nil
}

func TestColdPackParallelBudgetsAndPressure(t *testing.T) {
	for _, workers := range []int{1, 8} {
		for _, budget := range []string{"cpu", "bytes"} {
			t.Run(fmt.Sprintf("%s/%d", budget, workers), func(t *testing.T) {
				pebbleSynctest(t, func(t *testing.T) {
					codec := &parallelCursorCodec{cursorTestCodec: cursorTestCodec{allowMissing: true}, before: func(_ context.Context, _ string, _ uint64) error { time.Sleep(time.Millisecond); return nil }}
					o := parallelOptions(coldPackFreeFS{vfs.NewMem()}, codec, workers)
					if budget == "cpu" {
						o.ColdPack.CPUFraction = .1
					} else {
						o.ColdPack.BytesPerSecond = 8000
					}
					s, _ := openTestStore(t, "/store", o)
					s.StopBackground()
					defer closeTestStore(t, s)
					for i := range 64 {
						createTestMetric(t, s, fmt.Sprintf("m.%03d", i))
					}
					start := time.Now()
					if err := s.RunColdPack(context.Background()); err != nil {
						t.Fatal(err)
					}
					st := s.ColdPackStats()
					elapsed := time.Since(start)
					// The final wave's debt may remain unpaid, just as in the serial driver.
					allowance := coldPackPaceQuantum + time.Duration(workers)*time.Millisecond
					if budget == "cpu" {
						limit := float64(elapsed+allowance) * float64(workers) * o.ColdPack.CPUFraction
						if float64(st.WorkerNanos) > limit {
							t.Fatalf("CPU budget exceeded: work=%d elapsed=%s", st.WorkerNanos, elapsed)
						}
						if workers == 8 && st.WorkerNanos < uint64(elapsed)/2 {
							t.Fatal("parallel CPU budget serialized")
						}
					} else {
						// Include one maximal observed wave of fixture reads/batch framing.
						allowed := (elapsed+coldPackPaceQuantum).Seconds()*float64(o.ColdPack.BytesPerSecond) + float64(workers*256)
						if float64(st.ReadBytes+st.WriteBytes) > allowed {
							t.Fatalf("shared byte budget exceeded: %+v elapsed=%s", st, elapsed)
						}
					}
					next := o.ColdPack
					next.MinFreeBytes = ^uint64(0)
					if err := s.ConfigureColdPack(next); err != nil {
						t.Fatal(err)
					}
					before := len(codec.calls)
					if _, err := s.ColdPackStep(context.Background()); err != nil {
						t.Fatal(err)
					}
					if len(codec.calls) != before || s.ColdPackStats().PressurePauses != 1 {
						t.Fatal("pressure admitted work")
					}
				})
			})
		}
	}
}

func TestColdPackParallelUnpackFailure(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &parallelCursorCodec{cursorTestCodec: cursorTestCodec{skip: true}}
		o := parallelOptions(vfs.NewMem(), codec, 8)
		o.ColdPack.Mode = "unpack"
		s, _ := openTestStore(t, "/store", o)
		s.StopBackground()
		defer closeTestStore(t, s)
		for i := range 10 {
			createTestMetric(t, s, fmt.Sprintf("unpack.%03d", i))
		}
		if done, err := s.ColdPackStep(context.Background()); done || err != nil {
			t.Fatalf("first wave: %v %v", done, err)
		}
		o.ColdPack.Workers = 2
		if err := s.ConfigureColdPack(o.ColdPack); err != nil {
			t.Fatal(err)
		}
		if !s.coldPack.checkpoint.PassFailed || s.coldPack.checkpoint.MetricsVisited != 8 {
			t.Fatal("resize lost unpack failure/watermark")
		}
		codec.skip = false
		if err := s.RunColdPack(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.DowngradeColdPack(context.Background()); err == nil {
			t.Fatal("partial clean wave authorized downgrade")
		}
		if err := s.RunColdPack(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.DowngradeColdPack(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
