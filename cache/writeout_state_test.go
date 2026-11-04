package cache

import (
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func TestWriteoutState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		c.SetMaxSize(100)
		if err := c.SetWriteoutBatching(10, 100*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := c.SetWriteoutSpread(.25); err != nil {
			t.Fatal(err)
		}
		// Historical sample timestamps do not make fresh arrivals overdue.
		c.AddPoint([]byte("state.old"), 1, 1)
		c.AddPoint([]byte("state.new"), 1, 1)
		state := c.WriteoutState()
		if state.Metrics != 2 || state.Points != 2 || state.Eligible != 0 || state.OldestAge != 0 ||
			state.MaxPoints != 100 || state.MeanDelay != 87500*time.Millisecond {
			t.Fatalf("initial state: %+v", state)
		}
		time.Sleep(101 * time.Second)
		state = c.WriteoutState()
		if state.Eligible != 2 || state.OldestAge != 101*time.Second {
			t.Fatalf("due state: %+v", state)
		}
		p, _ := c.PopNotConfirmed("state.old")
		c.Confirm(p)
		c.Pop("state.new")
		state = c.WriteoutState()
		if state.Metrics != 0 || state.Points != 0 || state.OldestAge != 0 {
			t.Fatalf("persisted state: %+v", state)
		}
		c.SetMaxBytes(1)
		if !c.WriteoutState().Pressure {
			t.Fatal("byte pressure missing")
		}
	})
}

func BenchmarkWriteoutState(b *testing.B) {
	c := New()
	c.SetMaxSize(0)
	if err := c.SetWriteoutBatching(960, 960*time.Second); err != nil {
		b.Fatal(err)
	}
	if err := c.SetWriteoutSpread(.25); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100_000; i++ {
		c.AddPoint([]byte("state.benchmark."+strconv.Itoa(i)), 1, 1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c.WriteoutState()
	}
}

func TestWriteoutStateStartupClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		if err := c.SetWriteoutBatching(10, time.Minute); err != nil {
			t.Fatal(err)
		}
		c.PauseStartupWriteout()
		c.AddPoint([]byte("held.metric"), 1, 1)
		time.Sleep(time.Hour)
		if s := c.WriteoutState(); s.OldestAge != 0 || s.Eligible != 0 {
			t.Fatalf("held clock advanced: %+v", s)
		}
		c.ResumeStartupWriteout(time.Now())
		time.Sleep(5 * time.Second)
		if s := c.WriteoutState(); s.OldestAge > time.Minute+5*time.Second || s.Eligible > 1 {
			t.Fatalf("release extended the held window: %+v", s)
		}
	})
}
