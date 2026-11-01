package restartcpu

import (
	"testing"
	"testing/synctest"
	"time"
)

// Mandatory work has the same mean as steady intake/reads/background work but
// arrives in CPU bursts. Short debt clipping must not erase those CPU charges.
func TestBurstyMandatoryCPUConservesAllowance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const allowance = 22.25
		started := time.Now()
		spent := 0.
		mandatory := func(seconds float64) float64 {
			cycles := int(seconds / .1)
			tail := seconds - float64(cycles)*.1
			return float64(cycles)*1.394 + min(tail, .02)*60 + max(0, tail-.02)*2.425
		}
		read := func() float64 { return mandatory(time.Since(started).Seconds()) + spent }
		b := NewWithCPU(allowance, read)
		b.SetSteadyRate(12800)
		next := started
		for range 60000 {
			time.Sleep(time.Millisecond)
			now := time.Now()
			if now.Before(next) {
				continue
			}
			for {
				delay := b.reserveDelay(now, true, 8*b.writeCost())
				if delay > 0 {
					next = now.Add(delay)
					break
				}
				spent += 8 * .0005
			}
		}
		seconds := time.Since(started).Seconds()
		total := read() / seconds
		t.Logf("process=%.3f allowance=%.2f mandatory=%.3f admitted=%.3f", total, allowance, mandatory(seconds)/seconds, spent/seconds)
		if total > allowance+.1 {
			t.Fatalf("CPU debt was discarded: process %.3f > allowance %.2f", total, allowance)
		}
	})
}

func TestMandatoryRiseShrinksAdmittedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		spent := 0.
		used := func() float64 {
			s := time.Since(started).Seconds()
			return 8*min(s, 10) + 14*min(max(0, s-10), 10) + 8*max(0, s-20) + spent
		}
		b := NewWithCPU(22.25, used)
		b.SetSteadyRate(12800)
		next := started
		prior := 0.
		rates := []float64{}
		for step := 1; step <= 30000; step++ {
			time.Sleep(time.Millisecond)
			now := time.Now()
			if !now.Before(next) {
				for {
					delay := b.reserveDelay(now, true, 8*b.writeCost())
					if delay > 0 {
						next = now.Add(delay)
						break
					}
					spent += 8 * .0005
				}
			}
			if step%10000 == 0 {
				rates = append(rates, (spent-prior)/10)
				prior = spent
			}
		}
		t.Logf("admitted cores before/during/after mandatory rise: %.3f %.3f %.3f", rates[0], rates[1], rates[2])
		if rates[1] > rates[0]-5 || rates[2] < rates[1]+5 {
			t.Fatal("admitted work did not follow mandatory demand", rates)
		}
	})
}

func TestOperatorProcessGaugeSmoothsObservedBursts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		used := 0.
		b := NewWithCPU(100, func() float64 { return used })
		for second := range 30 {
			rate := 10.
			if second%2 == 1 {
				rate = 50
			}
			used += rate
			time.Sleep(time.Second)
			b.delay(time.Now())
		}
		// Alternating 10/50-core one-second observations should report their
		// smoothed neighborhood, not expose the last 50-core slice.
		if got := b.State().Process; got < 26 || got > 34 {
			t.Fatal("operator gauge still follows raw slices", got)
		}
	})
}
