package restartcpu

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
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
		next := startHistory(context.Background(), path, 0, func() float64 { return 0 })
		defer next.Stop()
		if math.Abs(next.Cores()-12.6) > 1e-9 || next.SteadyRate() != 14000 {
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
