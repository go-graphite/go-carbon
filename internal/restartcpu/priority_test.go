package restartcpu

import (
	"context"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestBudgetWriteoutPriority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(2, func() float64 { return 0 })
		time.Sleep(time.Second)
		b.BeginWriteout()
		if b.delay(time.Now()) == 0 {
			t.Fatal("warming admitted during mandatory writeout")
		}
		b.EndWriteout()
		b.ObserveWriteout(100, 0)
		if b.delay(time.Now()) == 0 {
			t.Fatal("warming admitted below the writeout floor")
		}
		b.ObserveWriteout(100, 100)
		if err := b.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		s := b.State()
		if s.Warming+s.Writeout > 2 || math.IsInf(s.Writeout, 1) || s.WarmGrants != 1 || s.WriteGrants != 1 {
			t.Fatal(s)
		}
	})
}

func TestBudgetSaturatedAfterWarming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		spent := 0.0
		b := NewWithCPU(2, func() float64 { return spent + 2*time.Since(started).Seconds() })
		spent = 1
		time.Sleep(time.Second)
		if b.delay(time.Now()) == 0 {
			t.Fatal("failed to reproduce CPU debt")
		}
		b.Warmed()
		time.Sleep(10 * time.Minute)
		if !b.Active() || b.delay(time.Now()) == 0 {
			t.Fatal("saturated optional work unexpectedly admitted")
		}
		// Mandatory writes bypass that debt even after warming has finished.
		b.BeginWriteout()
		b.RecordWrite()
		b.EndWriteout()
		if b.Updates() != 1 {
			t.Fatal("writeout depended on optional CPU credit")
		}
	})
}

func TestBudgetWriteoutFloorWarnsOnShortfall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewWithCPU(2, func() float64 { return 0 })
		b.ObserveWriteout(100, 99)
		time.Sleep(10 * time.Second)
		if b.ObserveWriteout(100, 99) {
			t.Fatal("warned before more than 10s")
		}
		time.Sleep(time.Second)
		if !b.ObserveWriteout(100, 99) || b.delay(time.Now()) == 0 {
			t.Fatal("sub-floor writeout did not warn and withhold optional CPU")
		}
	})
}

func TestBudgetPriorityMetricsReplacement(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	for _, cores := range []float64{2, 3} {
		b := NewWithCPU(cores, func() float64 { return 0 })
		b.ObserveWriteout(cores*100, 0)
		b.BeginWriteout()
		b.RecordWrite()
		b.EndWriteout()
		b.InitPrometheus(reg)
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, f := range families {
			if f.GetName() == "restart_cpu_writeout_floor_updates_per_second" && f.Metric[0].GetGauge().GetValue() != cores*100 {
				t.Fatal("registry retained the old controller")
			}
			if f.GetName() == "restart_cpu_granted_cores" {
				for _, m := range f.Metric {
					consumer := m.Label[0].GetValue()
					value := m.GetGauge().GetValue()
					if consumer == "warming" && value != 0 {
						t.Fatal("warming got CPU below floor", value)
					}
					if consumer == "writeout" {
						found = true
						if math.IsInf(value, 1) || value > cores {
							t.Fatal("writeout exceeded the shared allowance", value)
						}
					}
				}
			}
		}
		if !found {
			t.Fatal("missing consumer CPU grant")
		}
	}
}

func TestBudgetWriteoutSharesCPUWhileWarming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		used := 0.0
		b := NewWithCPU(2, func() float64 { return used })
		used = 3
		time.Sleep(time.Second)
		exit := make(chan bool)
		admitted := make(chan bool, 1)
		go func() { admitted <- b.WaitWriteout(exit) }()
		synctest.Wait()
		select {
		case <-admitted:
			t.Fatal("writeout bypassed CPU debt during warming")
		default:
		}
		if s := b.State(); math.IsInf(s.Writeout, 1) || s.Warming+s.Writeout > 2 {
			t.Fatal(s)
		}
		time.Sleep(time.Second)
		if !<-admitted {
			t.Fatal("writeout did not use replenished credit")
		}
		b.EndWriteout()
		used += 10
		time.Sleep(time.Second)
		go func() { admitted <- b.WaitWriteout(exit) }()
		synctest.Wait()
		close(exit)
		if <-admitted {
			t.Fatal("shutdown did not cancel CPU wait")
		}
		b.Warmed()
		if !b.WaitWriteout(make(chan bool)) {
			t.Fatal("warm backlog stalled")
		}
		b.EndWriteout()
	})
}
