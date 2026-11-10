package persister

import (
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/hostcapacity"
)

func TestCapacityProductionLatencyCannotStarveSteadyDemand(t *testing.T) {
	now := time.Now()
	var reads hostcapacity.Latency
	var c autoCapacityController
	s := healthyCapacity()
	q := cache.WriteoutState{Metrics: 80_000_000, Eligible: 80_000_000, MeanDelay: 1000 * time.Second, MaxDelay: 1000 * time.Second}
	var demand autoUpdateController
	minimum := int(math.Ceil(float64(q.Metrics) / q.MeanDelay.Seconds() * autoUpdateMargin))
	rate, lowest := 48000, math.MaxInt
	// Stress the controller even with the old request-shaped distribution as
	// its *storage* input. Real request wall time is excluded by carbonserver.
	for i := range 2 * 60 * 12 { // two hours of five-second observations
		d := time.Duration(60+i%11) * time.Millisecond
		if i%17 == 0 {
			d = 30 * time.Millisecond
		}
		for range 100 {
			reads.Observe(d)
		}
		s.Reads = reads.Snapshot()
		s.updates += uint64(rate * 5)
		s.Host.ProcessCPU += float64(rate*5) * .0002
		s.writeSeconds += float64(rate*5) * .0003
		s.Host.Idle = .65
		if i%100 == 0 { // CPU alone must also retain required throughput.
			s.Host.Idle = .10
		}
		at := now.Add(time.Duration(i) * 5 * time.Second)
		demand.step(at, q, 1, 0)
		rate = c.step(at, s, q, 1, 96, 192, rate, 81, demand.target, demand.need*autoUpdateMargin)
		lowest = min(lowest, rate)
		if rate < minimum {
			t.Fatalf("starved steady cache demand at interval %d: rate=%d need=%d p99=%s reference=%s reason=%s", i, rate, minimum, c.p99, c.baseline, c.reason)
		}
	}
	if c.baseline < 60*time.Millisecond || c.baseline > 80*time.Millisecond {
		t.Fatal("quiet intervals ratcheted rolling reference down", c.baseline)
	}
	t.Logf("production-shaped 30/60-70ms latency: minimum capacity=%d/s steady demand=%d/s reference=%s", lowest, minimum, c.baseline)
}

func TestCapacityQuietReadReferenceCanRiseFallAndExpire(t *testing.T) {
	var r capacityReadReference
	now := time.Now()
	for i := range 60 {
		r.observe(now.Add(time.Duration(i)*time.Minute), 30*time.Millisecond)
	}
	for i := 60; i < 100; i++ {
		r.observe(now.Add(time.Duration(i)*time.Minute), 70*time.Millisecond)
	}
	if got := r.value(now.Add(99 * time.Minute)); got != 70*time.Millisecond {
		t.Fatal("reference could not follow a slower quiet workload", got)
	}
	for i := 100; i < 140; i++ {
		r.observe(now.Add(time.Duration(i)*time.Minute), 30*time.Millisecond)
	}
	if got := r.value(now.Add(139 * time.Minute)); got != 30*time.Millisecond {
		t.Fatal("reference could not follow a faster quiet workload", got)
	}
	if got := r.value(now.Add(200 * time.Minute)); got != 0 {
		t.Fatal("old workload never expired", got)
	}
}

func TestCapacityReadRegressionOnlyRollsBackConfirmedProbeOnce(t *testing.T) {
	now := time.Now()
	for _, probe := range []bool{false, true} {
		c := autoCapacityController{limit: 100000}
		s := healthyCapacity()
		q := cache.WriteoutState{Eligible: 1}
		c.step(now, s, q, 1, 96, 192, 100000, 81, 1e6, 80000)
		c.reads.observe(now, time.Millisecond)
		if probe {
			c.probeUntil, c.increasedAt = now.Add(capacityReadProbeWindow), now
			c.probeLimit, c.probeRate, c.probeP99 = 90000, 90000, time.Millisecond
		}
		var reads hostcapacity.Latency
		for i := 1; i <= 12; i++ {
			for range 100 {
				reads.Observe(70 * time.Millisecond)
			}
			s.Reads = reads.Snapshot()
			s.updates += uint64(c.limit * 5)
			before := c.limit
			c.step(now.Add(time.Duration(i)*5*time.Second), s, q, 1, 96, 192, before, 81, 1e6, 80000)
			want := 100000
			if probe {
				want = 90000
			}
			if c.limit != want {
				t.Fatalf("probe=%v interval=%d: read pressure compounded or cut without correlation: %d want %d", probe, i, c.limit, want)
			}
		}
	}
}

func TestCapacityOnlyHardPressureMayCrossSteadyDemand(t *testing.T) {
	for _, mode := range []string{"cpu", "gc", "busy disk", "debt", "invalid", "stall", "failure", "disk full", "memory", "l0 stop"} {
		t.Run(mode, func(t *testing.T) {
			c := autoCapacityController{limit: 100000}
			s := healthyCapacity()
			now := time.Now()
			q := cache.WriteoutState{Eligible: 1}
			c.step(now, s, q, 1, 96, 192, 100000, 81, 1e6, 80000)
			hard := false
			switch mode {
			case "cpu":
				s.Host.Idle = .05
			case "gc":
				s.Host.GC = .4
			case "busy disk":
				s.Host.DiskBusy = .95
			case "debt":
				s.Debt = 1 << 30
			case "invalid":
				s.Host.Valid = false
			case "stall":
				s.Stalled, hard = true, true
			case "failure":
				s.failures, hard = 1, true
			case "disk full":
				s.FreeBytes, hard = 1, true
			case "memory":
				s.Host.Memory, hard = .01, true
			case "l0 stop":
				s.L0, hard = s.L0Stop, true
			}
			got := c.step(now.Add(5*time.Second), s, q, 1, 96, 192, 100000, 81, 1e6, 80000)
			if hard && got >= 80000 || !hard && got < 80000 {
				t.Fatal("incorrect steady demand floor", mode, got)
			}
		})
	}
}

func TestCapacityCanaryCalibrationRamp(t *testing.T) {
	c := autoCapacityController{}
	s := healthyCapacity()
	s.Host.Idle, s.Host.DiskBusy = .65, .20 // spare capacity like the canary snapshot
	q := cache.WriteoutState{Eligible: 40_000_000, RecoveryBlocked: true}
	now := time.Now()
	rate := c.step(now, s, q, 1, 96, 192, 48000, 81, 500000, 25750)
	initial := rate
	reached := map[int]time.Duration{}
	var reads hostcapacity.Latency
	for i := 1; i <= 36; i++ {
		s.updates += uint64(rate * 5)
		// Modeled service costs, not device throughput measured in production.
		s.Host.ProcessCPU += float64(rate*5) * .0002
		s.writeSeconds += float64(rate*5) * .0003
		for range 100 {
			reads.Observe(time.Millisecond)
		}
		s.Reads = reads.Snapshot()
		rate = c.step(now.Add(time.Duration(i)*5*time.Second), s, q, 1, 96, 192, rate, 81, 500000, 25750)
		for _, target := range []int{66000, 72000, 75000, 80000, 100000, 3 * initial} {
			if rate >= target && reached[target] == 0 {
				reached[target] = time.Duration(i) * 5 * time.Second
			}
		}
	}
	if reached[3*initial] == 0 || reached[3*initial] > 3*time.Minute {
		t.Fatal("idle token-bound ramp is too slow", initial, rate, reached)
	}
	t.Logf("48k seed canary calibration: targets reached=%v final=%d/s", reached, rate)
}
