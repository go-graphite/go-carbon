package restartcpu

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestRecentSteadyHistoryAndShutdown(t *testing.T) {
	host, _ := os.Hostname()
	path := filepath.Join(t.TempDir(), "cpu.json")
	for _, age := range []time.Duration{time.Minute, 25 * time.Hour, -time.Minute} {
		h := history{Host: host, Steady: sample{time.Now().Add(-age), 12}, Updates: 14000}
		data, _ := json.Marshal(h)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		b := Start(context.Background(), path, 0)
		if age == time.Minute {
			if math.Abs(b.Cores()-12.6) > 1e-9 || b.SteadyRate() != 14000 {
				t.Fatal(b.Cores(), b.SteadyRate())
			}
		} else if b.Cores() != fallbackCores() || b.SteadyRate() != 0 {
			t.Fatal("invalid history accepted")
		}
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
			t.Fatal("sampler save not joined", err)
		}
	}
}

func TestHistoryWinsOverConfiguredFallback(t *testing.T) {
	host, _ := os.Hostname()
	for _, tc := range []struct {
		name  string
		age   time.Duration
		host  string
		cores float64
		fresh bool
	}{
		{"fresh", time.Minute, host, 12, true},
		{"just-fresh", 24*time.Hour - time.Second, host, 12, true},
		{"expired", 24 * time.Hour, host, 12, false},
		{"stale", 25 * time.Hour, host, 12, false},
		{"future", -time.Minute, host, 12, false},
		{"foreign", time.Minute, "another-host", 12, false},
		{"zero", time.Minute, host, 0, false},
		{"negative", time.Minute, host, -12, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cpu.json")
			synctest.Test(t, func(t *testing.T) {
				for _, configured := range []float64{0, 48} {
					h := history{Host: tc.host, Steady: sample{time.Now().Add(-tc.age), tc.cores}, Updates: 14000}
					data, err := json.Marshal(h)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
					b := startHistory(context.Background(), path, configured, func() float64 { return 0 })
					want, source, rate := configured, "fallback", 0.0
					if want == 0 {
						want = fallbackCores()
					}
					if tc.fresh {
						want, source, rate = 12.6, "history", 14000
					}
					if math.Abs(b.Cores()-want) > 1e-9 || b.Source() != source || b.SteadyRate() != rate {
						t.Errorf("configured=%g: got %g/%s/%g, want %g/%s/%g", configured, b.Cores(), b.Source(), b.SteadyRate(), want, source, rate)
					}
					if err := b.Stop(); err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}

func TestConfiguredFallbackWithoutHistory(t *testing.T) {
	for _, contents := range []string{"", "{", "{}"} {
		path := filepath.Join(t.TempDir(), "cpu.json")
		if contents != "" {
			if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
		b := Start(context.Background(), path, 48)
		if b.Cores() != 48 || b.Source() != "fallback" || b.SteadyRate() != 0 {
			t.Error("configured fallback not selected", b.Cores(), b.Source(), b.SteadyRate())
		}
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompletedWarmingStillPacesWriteout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		used := 0.0
		b := NewWithCPU(2, func() float64 { return used })
		b.Warmed()
		time.Sleep(time.Second)
		used = 3
		exit, admitted := make(chan bool), make(chan bool, 1)
		go func() { admitted <- b.WaitWriteout(exit) }()
		synctest.Wait()
		select {
		case <-admitted:
			t.Fatal("completed warming bypassed process CPU debt")
		default:
		}
		time.Sleep(time.Second)
		if !<-admitted {
			t.Fatal("writeout did not resume")
		}
		b.EndWriteout()
	})
}

func TestHistoryRecordsFirstCompleteSteadyMinute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.json")
	synctest.Test(t, func(t *testing.T) {
		began := time.Now()
		b := startHistory(context.Background(), path, 0, func() float64 { return 12 * time.Since(began).Seconds() })
		time.Sleep(time.Minute)
		synctest.Wait()
		b.Warmed()
		time.Sleep(time.Minute)
		synctest.Wait()
		b.written.Add(840000)
		time.Sleep(time.Minute)
		synctest.Wait()
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var saved history
		if err := json.Unmarshal(data, &saved); err != nil {
			t.Fatal(err)
		}
		if math.Abs(saved.Steady.Cores-12) > 1e-9 || math.Abs(saved.Updates-14000) > 1e-9 {
			t.Fatal(saved)
		}
		next := startHistory(context.Background(), path, 48, func() float64 { return 0 })
		defer next.Stop()
		if math.Abs(next.Cores()-12.6) > 1e-9 || next.SteadyRate() != 14000 || next.Source() != "history" {
			t.Fatal(next.Cores(), next.SteadyRate())
		}
	})
}

func TestHistorySaveFailureIsReported(t *testing.T) {
	b := Start(context.Background(), filepath.Join(t.TempDir(), "absent", "cpu.json"), 2)
	if b.Stop() == nil {
		t.Fatal("failed history save was hidden")
	}
}

func TestCheckpointLoadingSharesCPUWithActiveWriters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		used := 0.0
		b := NewWithCPU(2, func() float64 { return used })
		b.BeginWriteout()
		defer b.EndWriteout()
		if err := b.WaitCheckpoint(context.Background()); err != nil {
			t.Fatal(err)
		}
		used = 3
		time.Sleep(time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(time.Millisecond); cancel() }()
		if err := b.WaitCheckpoint(ctx); err != context.Canceled {
			t.Fatal("checkpoint bypassed shared CPU debt", err)
		}
	})
}

func TestHistoryExcludesThroughputShortfall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.json")
	synctest.Test(t, func(t *testing.T) {
		began := time.Now()
		b := startHistory(context.Background(), path, 0, func() float64 { return 12 * time.Since(began).Seconds() })
		b.Warmed()
		b.ObserveWriteout(100, 50)
		time.Sleep(3 * time.Minute)
		synctest.Wait()
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var saved history
		if err := json.Unmarshal(data, &saved); err != nil {
			t.Fatal(err)
		}
		if saved.Steady.Cores != 0 {
			t.Fatal("backlogged process was recorded as steady", saved)
		}
	})
}

func TestDerivedAllowanceUsesHostSizeWithinParallelism(t *testing.T) {
	for _, tc := range []struct {
		cpus, procs int
		want        float64
	}{{192, 192, 16}, {192, 64, 16}, {192, 8, 8}, {384, 384, 32}, {16, 16, 1}, {16, 1, 1}} {
		if got := derivedAllowance(tc.cpus, tc.procs); got != tc.want {
			t.Fatalf("cpus=%d procs=%d: got %g want %g", tc.cpus, tc.procs, got, tc.want)
		}
	}
}

func TestHistoryRecordsBatchedSteadyCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.json")
	synctest.Test(t, func(t *testing.T) {
		core := cache.New()
		defer core.Stop()
		if err := core.SetWriteoutBatching(100, 960*time.Second); err != nil {
			t.Fatal(err)
		}
		core.Add(points.OnePoint("live", 1, time.Now().Unix()))
		startupSize := core.Size()
		began := time.Now()
		b := startHistory(context.Background(), path, 48, func() float64 { return 12 * time.Since(began).Seconds() })
		defer b.Stop()
		b.SetDrain(func() bool {
			return core.PendingOutstanding() == 0 && core.Size() <= startupSize
		})
		b.SetHistoryState(func() (int64, bool) {
			return core.Size(), core.PendingOutstanding() == 0 && !core.Backlogged()
		})
		// Intake grows the ordinary batching population beyond the startup
		// snapshot. Afterwards its point count stays constant between bursts.
		core.Add(points.OnePoint("live", 2, time.Now().Unix()))
		b.Warmed()
		b.ObserveWriteout(float64(core.Len())/960, 0)
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var saved history
		if err := json.Unmarshal(data, &saved); err != nil {
			t.Fatal(err)
		}
		if saved.Steady.At.IsZero() || math.Abs(saved.Steady.Cores-12) > 1e-9 {
			t.Fatalf("steady batched cache never recorded CPU history: %+v", saved.Steady)
		}
	})
}

func TestHistoryStateExcludesWarmingRestorationAndGrowth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.json")
	synctest.Test(t, func(t *testing.T) {
		began := time.Now()
		b := startHistory(context.Background(), path, 48, func() float64 { return 12 * time.Since(began).Seconds() })
		defer b.Stop()
		var queued atomic.Int64
		var ready atomic.Bool
		queued.Store(100)
		ready.Store(true)
		b.SetHistoryState(func() (int64, bool) { return queued.Load(), ready.Load() })
		advance := func() {
			time.Sleep(time.Minute)
			synctest.Wait()
		}
		unsaved := func() {
			t.Helper()
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("recorded an ineligible minute", err)
			}
		}
		advance()
		advance()
		unsaved() // Warming is unfinished even though the queue is stable.
		b.Warmed()
		ready.Store(false)
		advance()
		advance()
		unsaved() // Stable held restoration or cache pressure is not steady.
		ready.Store(true)
		for range 3 {
			queued.Add(100)
			advance()
		}
		unsaved() // Confirmed throughput cannot excuse a growing backlog.
		ready.Store(false)
		advance()
		ready.Store(true)
		advance()
		unsaved() // One eligible endpoint does not certify a full minute.
		b.written.Add(720000)
		advance()
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var saved history
		if err := json.Unmarshal(data, &saved); err != nil {
			t.Fatal(err)
		}
		if !saved.Steady.At.Equal(time.Now()) || saved.Steady.Cores != 12 || saved.Updates != 12000 {
			t.Fatalf("wrong first eligible minute: %+v, updates=%g", saved.Steady, saved.Updates)
		}
	})
}

func TestQuietCPUHistoryPreservesCatchupThroughput(t *testing.T) {
	for _, prior := range []bool{false, true} {
		name := "new"
		if prior {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cpu.json")
			synctest.Test(t, func(t *testing.T) {
				if prior {
					host, _ := os.Hostname()
					h := history{Host: host, Steady: sample{time.Now().Add(-time.Minute), 12}, Updates: 14000}
					data, err := json.Marshal(h)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				began := time.Now()
				b := startHistory(context.Background(), path, 48, func() float64 { return 12 * time.Since(began).Seconds() })
				defer b.Stop()
				b.SetHistoryState(func() (int64, bool) { return 100, true })
				b.Warmed()
				b.ObserveWriteout(10000, 0)
				time.Sleep(2 * time.Minute)
				synctest.Wait()
				check := func(want float64) {
					t.Helper()
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					var saved history
					if err := json.Unmarshal(data, &saved); err != nil {
						t.Fatal(err)
					}
					if !saved.Steady.At.Equal(time.Now()) || saved.Steady.Cores != 12 || saved.Updates != want {
						t.Fatalf("quiet sampling corrupted catch-up throughput: cores=%g updates=%g want=%g", saved.Steady.Cores, saved.Updates, want)
					}
				}
				want := 0.0
				if prior {
					want = 14000
				}
				check(want)
				b.written.Add(60) // A trickle below the floor is still a batching gap.
				time.Sleep(time.Minute)
				synctest.Wait()
				check(want)
				b.written.Add(720000)
				time.Sleep(time.Minute)
				synctest.Wait()
				want = 12000
				if prior {
					want = 13600
				}
				check(want)
			})
		})
	}
}
