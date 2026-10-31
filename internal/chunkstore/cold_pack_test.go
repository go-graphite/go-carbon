package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type testColdCodec struct {
	cancel context.CancelFunc
	bucketCodec
	fail      bool
	oversized bool
}

func (c testColdCodec) Repack(ctx context.Context, io ContinuationIO, r ColdPackRequest) (RepackStats, uint64, bool, error) {
	if c.fail {
		return RepackStats{}, 0, false, errors.New("test failure")
	}
	if c.oversized {
		err := io.Set([]byte("packed"), make([]byte, coldPackBatchLimit))
		return RepackStats{Blocks: 1}, 1, false, err
	}
	if r.Cursor != 0 {
		return RepackStats{}, 0, true, nil
	}
	if err := io.Set([]byte("packed"), []byte("complete")); err != nil {
		return RepackStats{}, 0, false, err
	}
	if c.cancel != nil {
		c.cancel()
	}
	return RepackStats{Blocks: 1, Points: 200}, 1, false, nil
}
func TestColdPackPagesAndBusyStripe(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	s.stopColdPack()
	for i := 0; i < 201; i++ {
		createTestMetric(t, s, fmt.Sprintf("page.%03d", i))
	}
	after := ""
	count := 0
	for {
		names, err := s.coldPackPage(ctx, after)
		if err != nil {
			t.Fatal(err)
		}
		if len(names) > ColdPackPageSize {
			t.Fatal("unbounded page")
		}
		if len(names) == 0 {
			break
		}
		for _, name := range names {
			if name <= after {
				t.Fatal("page regressed")
			}
			after = name
			count++
		}
	}
	if count != 201 {
		t.Fatalf("listed %d metrics", count)
	}
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "exact"}); err != nil {
		t.Fatal(err)
	}
	unlock := s.lockMetricMutation("page.000")
	_, err := s.ColdPackStep(ctx)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	if st := s.ColdPackStats(); st.Conflicts != 1 || st.Blocks != 0 {
		t.Fatalf("busy stripe waited or published: %+v", st)
	}
	for n := 0; n < 1000; n++ {
		done, err := s.ColdPackStep(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	st := s.ColdPackStats()
	if st.Passes != 1 || st.Blocks != 200 || st.Pages != 5 {
		t.Fatalf("paged sweep: %+v", st)
	}
}
func TestColdPackBudgetsAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codec testColdCodec
		skip  bool
	}{{"oversized", testColdCodec{oversized: true}, true}, {"failure", testColdCodec{fail: true}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: tc.codec, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			defer closeTestStore(t, s)
			s.stopColdPack()
			createTestMetric(t, s, "budget")
			if err := s.ConfigureColdPack(ColdPackOptions{Mode: "exact"}); err != nil {
				t.Fatal(err)
			}
			_, err := s.ColdPackStep(context.Background())
			st := s.ColdPackStats()
			if tc.skip {
				if err != nil || st.Skipped != 1 || st.Blocks != 0 || st.WriteBytes != 0 {
					t.Fatalf("budget %+v %v", st, err)
				}
			} else if err == nil || st.Errors != 1 || st.Error == "" {
				t.Fatalf("silent failure %+v %v", st, err)
			}
		})
	}
}
func TestColdPackPacerAndStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Store{}
		s.coldPack.options, _ = NormalizeColdPack(ColdPackOptions{Mode: "off", CPUFraction: .1, BytesPerSecond: 1 << 20})
		started := time.Now()
		time.Sleep(100 * time.Millisecond)
		s.coldPack.stats.ReadBytes = 1 << 20
		var pacer coldPackPacer
		if delay := s.coldPackDelay(&pacer, ColdPackStats{}, started); delay != 900*time.Millisecond {
			t.Fatalf("pacer delay %s", delay)
		}
		s.startColdPack()
		time.Sleep(time.Second)
		synctest.Wait()
		s.stopColdPack()
	})
}

func TestColdPackPacerAccumulatesShortSteps(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cpu   float64
		bytes uint64
		debt  time.Duration
	}{{"cpu", .25, 0, 300 * time.Microsecond}, {"bytes", 1, 1000, 900 * time.Microsecond}} {
		t.Run(tc.name, func(t *testing.T) {
			o, err := NormalizeColdPack(ColdPackOptions{Mode: "exact", CPUFraction: tc.cpu, BytesPerSecond: 1000000})
			if err != nil {
				t.Fatal(err)
			}
			var p coldPackPacer
			var stats ColdPackStats
			now := time.Unix(1700000000, 0)
			for n := 1; ; n++ {
				before, started := stats, now
				stats.ReadBytes += tc.bytes
				now = now.Add(100 * time.Microsecond)
				delay := p.delay(o, before, stats, started, now)
				debt := time.Duration(n) * tc.debt
				if debt < coldPackPaceQuantum {
					if delay != 0 {
						t.Fatalf("step %d: premature timer %s", n, delay)
					}
					continue
				}
				if delay != debt {
					t.Fatalf("step %d: delay %s, want accumulated %s", n, delay, debt)
				}
				// Oversleep repays all debt but cannot subsidize the next step.
				started = now.Add(delay + time.Second)
				now = started.Add(100 * time.Millisecond)
				before = stats
				stats.ReadBytes += 200000
				want := max(time.Duration(float64(100*time.Millisecond)*(1/tc.cpu-1)), 100*time.Millisecond)
				if got := p.delay(o, before, stats, started, now); got != want {
					t.Fatalf("idle earned credit: delay %s, want %s", got, want)
				}
				break
			}
		})
	}
}

func TestColdPackPacerRetainsIndependentBudgets(t *testing.T) {
	o, _ := NormalizeColdPack(ColdPackOptions{Mode: "exact", CPUFraction: .1, BytesPerSecond: 1000000})
	var p coldPackPacer
	started := time.Unix(1700000000, 0)
	now := started.Add(time.Millisecond)
	stats := ColdPackStats{ReadBytes: 20000}
	// The byte budget wins first, but the CPU debt still accumulates.
	if delay := p.delay(o, ColdPackStats{}, stats, started, now); delay != 19*time.Millisecond {
		t.Fatalf("byte delay: %s", delay)
	}
	started, now = now, now.Add(2*time.Millisecond)
	if delay := p.delay(o, stats, stats, started, now); delay != 27*time.Millisecond {
		t.Fatalf("CPU debt lost behind byte budget: %s", delay)
	}
	// An intentional budget change discards the previous policy's debt.
	o.CPUFraction = 1
	started, now = now, now.Add(time.Microsecond)
	if delay := p.delay(o, stats, stats, started, now); delay != 0 {
		t.Fatalf("reload retained old policy debt: %s", delay)
	}
	after := stats
	after.PressurePauses++
	if delay := p.delay(o, stats, after, now, now); delay != time.Second {
		t.Fatalf("pressure pause: %s", delay)
	}
}

func TestColdPackBackgroundShortStepsAndReload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
		defer closeTestStore(t, s)
		for i := range 130 {
			createTestMetric(t, s, fmt.Sprintf("background.%03d", i))
		}
		// With no virtual worker time and negligible byte debt, several pages
		// must finish before the driver needs its first timer wait.
		o := ColdPackOptions{Mode: "exact", CPUFraction: 1, BytesPerSecond: 1 << 40}
		if err := s.ConfigureColdPack(o); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if st := s.ColdPackStats(); st.Passes != 1 || st.Blocks != 130 {
			t.Fatalf("short steps waited individually: %+v", st)
		}
		// Reload wakes the long between-sweep wait; shutdown must also join it.
		o.Mode = "off"
		if err := s.ConfigureColdPack(o); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if st := s.ColdPackStats(); st.Active || st.Mode != "off" || st.Passes != 1 {
			t.Fatalf("reload did not stop maintenance: %+v", st)
		}
		s.stopColdPack()
	})
}

func TestColdPackOptions(t *testing.T) {
	for _, o := range []ColdPackOptions{{Mode: "unknown"}, {BytesPerSecond: -1}, {CPUFraction: -.1}, {CPUFraction: 2}, {WriteWindow: -1}, {Interval: time.Nanosecond}} {
		if _, err := NormalizeColdPack(o); err == nil {
			t.Fatalf("invalid policy accepted: %+v", o)
		}
	}
}

func TestColdPackReloadPreservesUnpackFailure(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{oversized: true}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	s.stopColdPack()
	createTestMetric(t, s, "unpack.budget")
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "unpack"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ColdPackStep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "unpack", BytesPerSecond: 2 << 20}); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 10; n++ {
		done, err := s.ColdPackStep(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if err := s.DowngradeColdPack(context.Background()); err == nil {
		t.Fatal("budget reload cleared incomplete unpack safety guard")
	}
}

func TestColdPackCancellationRetainsCandidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, TimeBuckets: true, Continuation: testColdCodec{cancel: cancel}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, s)
	s.stopColdPack()
	createTestMetric(t, s, "unpack.cancel")
	if err := s.ConfigureColdPack(ColdPackOptions{Mode: "unpack"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ColdPackStep(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled plan: %v", err)
	}
	if st := s.ColdPackStats(); st.PendingMetrics != 1 || st.Blocks != 0 {
		t.Fatalf("lost candidate after cancellation: %+v", st)
	}
	for n := 0; n < 10; n++ {
		done, err := s.ColdPackStep(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if s.ColdPackStats().Blocks != 1 {
		t.Fatal("cancelled candidate was not retried")
	}
	if err := s.DowngradeColdPack(context.Background()); err == nil {
		t.Fatal("interrupted sweep authorized downgrade")
	}
}

func TestColdPackNoopReloadKeepsPacingWait(t *testing.T) {
	s := &Store{}
	if err := s.ConfigureColdPack(ColdPackOptions{}); err != nil {
		t.Fatal(err)
	}
	s.coldPack.wake = make(chan struct{}, 1)
	if err := s.ConfigureColdPack(ColdPackOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(s.coldPack.wake) != 0 {
		t.Fatal("unrelated reload bypassed pacing wait")
	}
	if err := s.ConfigureColdPack(ColdPackOptions{BytesPerSecond: 2 << 20}); err != nil {
		t.Fatal(err)
	}
	if len(s.coldPack.wake) != 1 {
		t.Fatal("budget change did not wake worker")
	}
}
