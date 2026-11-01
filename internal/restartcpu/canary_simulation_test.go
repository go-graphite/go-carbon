package restartcpu

import (
	"math"
	"testing"
	"testing/synctest"
	"time"
)

// The CPU-work envelope reproduces measured cumulative CPU at 348s and 440s,
// and the 15..75s profile. This is not a captured point stream. Work arrivals
// follow the observed execution schedule; the new controller may defer them.
// Duplicate share is deliberately a sensitivity parameter: CPU stacks cannot
// identify values or prove their provenance. The 25% no-op cost reduction is
// below both measured duplicate-update CPU improvements (one/two archives).
func TestRecoveryWithIntakeCatchupAndLatePlanning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paced  bool
		factor float64
		target bool
	}{
		{"observed_work", false, 1, false},
		{"admission_only", true, 1, false},
		{"half_unchanged", true, 1 - .25*.5 + .04*.5, false},
		{"two_thirds_unchanged", true, 1 - .25*(2./3) + .04/3, false},
		{"three_quarters_unchanged", true, 1 - .25*.75 + .04*.25, false},
		{"four_fifths_unchanged", true, 1 - .25*.8 + .04*.2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const allowance = 22.25
				const steady = 14.94
				const profile = 1465.97 / 60
				const mandatoryProfile = (314.85 + 156.75 + 129.48 + 235.26) / 60
				const mandatoryFraction = mandatoryProfile / profile
				const writeFraction = 546.45 / (546.45 + 62.30 + 20.77)
				process := func(s float64) float64 {
					switch {
					case s < 15:
						return 25
					case s < 75:
						return profile
					case s < 348:
						return (6564. - 15*25 - 1465.97) / (348 - 75)
					default:
						return (7991. - 6564) / (440 - 348)
					}
				}
				wave := func(s float64) float64 {
					cycles := math.Floor(s / .1)
					tail := s - cycles*.1
					return cycles*1.394 + min(tail, .02)*60 + max(0, tail-.02)*2.425
				}
				mandatory := func(s float64) float64 {
					total := min(s, 15) * 25
					ends := []float64{75, 348, 440}
					from := 15.
					for _, end := range ends {
						if s > from {
							n := min(s, end) - from
							total += wave(n) * process(from) * mandatoryFraction / 13.94
						}
						from = end
					}
					if s > 440 {
						total += (s - 440) * steady * mandatoryFraction
					}
					return total
				}
				started := time.Now()
				spent := 0.
				read := func() float64 { return mandatory(time.Since(started).Seconds()) + spent }
				b := NewWithCPU(allowance, read)
				b.SetSteadyRate(12800)
				type work struct{ units, cost float64 }
				var queue []work
				head := 0
				next := started
				previousCPU, extra, peak := 0., 0., 0.
				profileStart, profileEnd := 0., 0.
				deferredAt := 0.
				for step := 1; step <= 450000; step++ {
					time.Sleep(time.Millisecond)
					now := time.Now()
					s := time.Since(started).Seconds()
					if s > 15 && s <= 440 {
						// The profile's cold cost per confirmed update, then warm catch-up
						// and steady-size batches. Eight updates share one simulated job.
						cost := .000761 / writeFraction
						if s > 100 {
							cost = .000350 / writeFraction
						}
						if s > 350 {
							cost = .000500 / writeFraction
						}
						units := process(s-.0005) * (1 - mandatoryFraction) * .001 / (8 * cost)
						if len(queue) > head && queue[len(queue)-1].cost == cost {
							queue[len(queue)-1].units += units
						} else {
							queue = append(queue, work{units, cost})
						}
					}
					if !now.Before(next) {
						for head < len(queue) {
							n := min(queue[head].units, 1.)
							cost := 8 * queue[head].cost * n
							if tc.paced {
								cost *= 1 - writeFraction*(1-tc.factor)
							}
							if tc.paced {
								if delay := b.reserveDelay(now, true, cost); delay > 0 {
									next = now.Add(delay)
									break
								}
							}
							spent += cost
							queue[head].units -= n
							if queue[head].units < 1e-9 {
								head++
							}
						}
					}
					if step == 15000 {
						profileStart = read()
					}
					if step == 75000 {
						profileEnd = read()
					}
					if step%1000 == 0 {
						cpu := read()
						cores := cpu - previousCPU
						extra += max(0, cores-steady) / 60
						if s > 15 {
							peak = max(peak, cores)
						}
						previousCPU = cpu
					}
					if s >= 75 && head == len(queue) && deferredAt == 0 {
						deferredAt = s
					}
					if s >= 440 && head == len(queue) {
						break
					}
				}
				elapsed := time.Since(started).Seconds()
				net := (read() - steady*elapsed) / 60
				minute := (profileEnd - profileStart) / 60
				t.Logf("elapsed=%.3fs first_post_profile_empty=%.3fs profile_minute=%.3f cores allowance=22.25 peak_1s_after_startup=%.3f positive_extra=%.3f net_extra=%.3f core-min", elapsed, deferredAt, minute, peak, extra, net)
				if !tc.paced {
					if math.Abs(net-23.6233) > .03 {
						t.Fatal("model did not reproduce captured CPU envelope", net)
					}
				} else {
					if minute > allowance+.25 {
						t.Fatal("whole-minute admission escaped allowance", minute)
					}
					if head != len(queue) {
						t.Fatal("CPU work did not drain")
					}
					if tc.target && extra > 16 {
						t.Fatal("modeled recovery missed target", extra)
					}
				}
			})
		})
	}
}
