package restartcpu

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestProcessGaugeIncludesBusySlicesBeforeAQuietAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		read := func() float64 {
			seconds := time.Since(started).Seconds()
			return min(seconds, .9)*(10.6/.9) + max(0, seconds-.9)
		}
		b := NewWithCPU(16, read)
		for range 50 {
			time.Sleep(20 * time.Millisecond)
			b.delay(time.Now())
		}
		if got := b.State().Process; math.Abs(got-10.7) > 1e-6 {
			t.Fatalf("process gauge discarded busy CPU slices: got=%g want=10.7", got)
		}
	})
}

func TestBudgetAccountsForWholeProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		used := 0.0
		b := &Budget{cores: 2, at: time.Now(), read: func() float64 { return used }}
		time.Sleep(time.Second)
		used = 3
		if delay := b.delay(time.Now()); delay == 0 {
			t.Fatal("external CPU was not charged")
		}
		time.Sleep(600 * time.Millisecond)
		if delay := b.delay(time.Now()); delay != 0 {
			t.Fatal("idle time did not repay debt", delay)
		}
		time.Sleep(time.Hour)
		b.delay(time.Now())
		if b.credit > .08 {
			t.Fatal("idle credits permit a restart burst")
		}
		used += 1
		time.Sleep(20 * time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(time.Millisecond); cancel() }()
		if err := b.Wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal("shutdown did not cancel", err)
		}
	})
}

func TestBudgetMedian(t *testing.T) {
	now := time.Now()
	var samples []sample
	for i := range 30 {
		samples = append(samples, sample{now.Add(-time.Duration(i) * time.Minute), float64(10 + i)})
	}
	if got := median(samples, now); got != 24.5 {
		t.Fatal(got)
	}
	if median(samples[:29], now) != 0 || median(samples, now.Add(time.Hour)) != 0 {
		t.Fatal("incomplete/stale history used")
	}
}

func TestBudgetLoadsHostHistory(t *testing.T) {
	host, _ := os.Hostname()
	now := time.Now()
	h := history{Host: host}
	for i := range 30 {
		h.Samples = append(h.Samples, sample{now.Add(-time.Duration(i) * time.Minute), 14})
	}
	path := filepath.Join(t.TempDir(), "cpu.json")
	data, _ := json.Marshal(h)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	b := Start(context.Background(), path, 0)
	if math.Abs(b.Cores()-14*1.05) > 1e-9 {
		t.Fatal(b.Cores())
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}
	b = Start(context.Background(), path, 12)
	if math.Abs(b.Cores()-14*1.05) > 1e-9 || b.Source() != "history" {
		t.Fatal("legacy history lost to configured fallback", b.Cores(), b.Source())
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}
	h.Host = "a-different-host"
	data, _ = json.Marshal(h)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	b = Start(context.Background(), path, 0)
	defer b.Stop()
	if b.Cores() != fallbackCores() || b.Source() != "fallback" {
		t.Fatal("foreign baseline used")
	}

}

func TestBudgetStartupWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(2)
		time.Sleep(31 * time.Minute)
		if !b.Active() {
			t.Fatal("unfinished warming lost its allowance")
		}
		b.Warmed()
		b.SetDrain(func() bool { return false })
		if !b.Active() {
			t.Fatal("late held backlog lost its allowance")
		}
		b.SetDrain(func() bool { return true })
		if b.Active() {
			t.Fatal("startup CPU allowance changed steady writeout")
		}
		if err := b.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
