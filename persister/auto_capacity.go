package persister

import (
	"math"
	"runtime"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/hostcapacity"
	"go.uber.org/zap"
)

// AutoCapacitySample is a read-only hook. Supply host samples and actual read
// latency plus storage stalls/debt. The persister adds its own confirmed writes
// and token waiters, so structural/in-place reloads never observe another pool.
type AutoCapacitySample struct {
	Host                 hostcapacity.Sample
	Reads                hostcapacity.LatencySnapshot
	Stalled              bool
	Stalls, Debt         uint64
	L0, L0Stop           int64
	SpaceValid           bool
	FreeBytes, DiskBytes uint64
	updates, failures    uint64
	writeSeconds         float64
}

type autoCapacityController struct {
	limit, hardCeiling, minimum int
	at                          time.Time
	previous                    AutoCapacitySample
	baseline, p99               time.Duration
	reads                       capacityReadReference
	increasedAt                 time.Time
	probeUntil                  time.Time
	probeLimit                  int
	probeRate                   float64
	probeP99                    time.Duration
	healthy                     int
	reason                      string
}

// SetAutoCapacity supplies local resource/latency observations before Start.
// It is used only with auto-max=-1. Positive maxima remain fixed overrides;
// zero retains the pre-existing unlimited demand model.
func (p *Whisper) SetAutoCapacity(sample func() AutoCapacitySample) {
	p.autoUpdate.capacitySource = sample
}

func (p *Whisper) waitUpdateToken(exit <-chan bool) (open, stopped bool) {
	tokens := p.updateTokens()
	if !p.autoUpdate.observeTokens.Load() {
		select {
		case _, open = <-tokens:
			return open, false
		case <-exit:
			return false, true
		}
	}
	select {
	case _, open = <-tokens:
		return open, false
	default:
	}
	p.autoUpdate.tokenWaiters.Add(1)
	defer p.autoUpdate.tokenWaiters.Add(-1)
	select {
	case _, open = <-tokens:
		return open, false
	case <-exit:
		return false, true
	}
}

func (p *Whisper) capacityState(source func() AutoCapacitySample) AutoCapacitySample {
	var sample AutoCapacitySample
	if source != nil {
		sample = source()
	}
	shared, files := p.WriteStats()
	sample.updates = shared.Updates + files.Updates
	sample.failures = shared.Failures + files.Failures
	sample.writeSeconds = shared.Seconds + files.Seconds
	return sample
}

func (c *autoCapacityController) step(now time.Time, s AutoCapacitySample, queue cache.WriteoutState, floor, workers, procs, applied int, waiting int64, demand, need float64) int {
	procs = max(1, procs)
	floor = max(1, floor)
	first := c.at.IsZero() || s.updates < c.previous.updates || s.failures < c.previous.failures || s.Stalls < c.previous.Stalls
	// Only confirmed hard resource pressure may underfund steady demand.
	// Busy CPUs and slow reads alone must not starve the cache's drain.
	hardPressure := s.Stalled || !first && (s.Stalls > c.previous.Stalls || s.failures > c.previous.failures) ||
		s.SpaceValid && s.FreeBytes < max(1<<30, s.DiskBytes/100) || s.Host.MemoryValid && s.Host.Memory < .05 || s.L0Stop > 0 && s.L0 >= s.L0Stop
	minimum := floor
	if !hardPressure {
		minimum = max(minimum, int(math.Ceil(min(need, float64(int(^uint(0)>>1)/2)))))
	}
	c.minimum = minimum
	// Hard safety envelope, refined by measured CPU/update and worker service
	// time below. Required steady demand wins over optional capacity estimates,
	// until actual hard pressure says the host cannot safely keep up.
	c.hardCeiling = max(minimum, min(2_000_000, max(1, workers)*10000, procs*10000))
	if c.limit == 0 {
		c.limit = min(c.hardCeiling, max(floor, max(1, workers)*500))
		c.reason = "initial"
	}
	c.limit = max(minimum, min(c.limit, c.hardCeiling))
	dt := now.Sub(c.at).Seconds()
	if first || dt <= 0 {
		// A new pool's first snapshot may contain old listener history. Begin
		// with the next interval, rather than a lifetime read percentile.
		c.p99 = 0
		c.at, c.previous = now, s
		return c.limit
	}
	p99, reads := s.Reads.P99Since(c.previous.Reads)
	c.p99, c.baseline = p99, c.reads.value(now)
	readPressure := reads > 0 && c.baseline > 0 && p99 > max(5*time.Millisecond, c.baseline*3/2)
	count := s.updates - c.previous.updates
	rate := float64(count) / dt
	if count > 0 {
		if cpu := s.Host.ProcessCPU - c.previous.Host.ProcessCPU; cpu > 0 {
			c.hardCeiling = min(c.hardCeiling, max(minimum, int(min(float64(c.hardCeiling), .75*float64(procs)*float64(count)/cpu))))
		}
		if seconds := s.writeSeconds - c.previous.writeSeconds; seconds > 0 {
			c.hardCeiling = min(c.hardCeiling, max(minimum, int(min(float64(c.hardCeiling), max(rate, .8*float64(max(1, workers))*float64(count)/seconds)))))
		}
	}
	resourcePressure := !s.Host.Valid || !s.Host.MemoryValid || !s.Host.DiskValid || !s.SpaceValid ||
		s.Host.Idle < .15 || s.Host.IOWait > .05 || s.Host.GC > .25 || s.Host.DiskBusy > .85 ||
		s.L0Stop > 0 && s.L0 >= s.L0Stop*3/4 || s.Debt > c.previous.Debt && s.Debt-c.previous.Debt > max(512<<20, c.previous.Debt/4)
	if reads > 0 && !hardPressure && !resourcePressure && (c.increasedAt.IsZero() || now.Sub(c.increasedAt) >= capacityReadProbeWindow) {
		c.reads.observe(now, p99)
		c.baseline = c.reads.value(now)
	}
	switch {
	case hardPressure:
		c.limit = max(floor, int(float64(c.limit)*.7))
		c.healthy = 0
		c.probeUntil = time.Time{}
		c.reason = "hard pressure"
	case resourcePressure:
		c.limit = max(minimum, int(float64(c.limit)*.7))
		c.healthy = 0
		c.probeUntil = time.Time{}
		c.reason = "resource pressure"
	case readPressure:
		c.healthy = 0
		c.reason = "read hold"
		// A slow interval alone only stops growth. Roll back one recent probe
		// only if confirmed writes actually increased and latency rose versus
		// both its pre-probe value and the rolling quiet-write reference.
		if !c.probeUntil.IsZero() && !now.After(c.probeUntil) && c.probeP99 > 0 && rate > c.probeRate*1.05 && p99 > max(5*time.Millisecond, c.probeP99*3/2) {
			c.limit = max(minimum, min(c.limit, c.probeLimit))
			c.probeUntil = time.Time{}
			c.reason = "read regression after increase"
		}
	case (queue.Eligible > 0 || queue.RecoveryBlocked) && waiting > 0 && demand >= float64(c.limit)*.95 && applied > 0 && rate >= float64(applied)*.8 && s.Host.Idle > .25 && s.Host.IOWait < .02 && s.Host.Memory > .10 && s.Host.GC < .10 && s.Host.DiskBusy < .70:
		c.healthy++
		c.reason = "observing"
		if c.healthy >= 2 {
			c.healthy = 0
			if next := min(c.hardCeiling, c.limit+max(1000, c.limit/10)); next > c.limit {
				c.probeLimit, c.probeRate, c.probeP99 = c.limit, rate, p99
				c.increasedAt, c.probeUntil = now, now.Add(capacityReadProbeWindow)
				c.limit = next
				c.reason = "token wait with idle hardware"
			} else {
				c.reason = "host ceiling"
			}
		}
	default:
		c.healthy = 0
		c.reason = "hold"
	}
	c.limit = max(minimum, min(c.limit, c.hardCeiling))
	c.at, c.previous = now, s
	return c.limit
}

// Caller holds autoUpdate.mu. Mandatory durability floors are still applied
// separately and may exceed this optional catch-up ceiling under pressure.
func (p *Whisper) capacityLimitLocked(s cache.WriteoutState) {
	a := &p.autoUpdate
	if a.ceiling != -1 {
		return
	}
	previous := a.capacity.limit
	previousHard := a.capacity.hardCeiling
	limit := a.capacity.step(time.Now(), a.capacitySample, s, a.floor, p.workersCount, min(runtime.NumCPU(), runtime.GOMAXPROCS(0)), a.applied, a.tokenWaiters.Load(), a.controller.target, a.controller.need*autoUpdateMargin)
	if a.ordinary > limit {
		a.ordinary = limit
	}
	if (previous != limit || previousHard != a.capacity.hardCeiling) && p.logger != nil && a.capacityLog.allow(time.Now(), capacityLogReason(a.capacity.reason), limit, 0) {
		sample := a.capacitySample
		p.logger.Info("automatic writeout capacity adjusted", zap.Int("updates_per_second", limit), zap.Int("previous_updates_per_second", previous), zap.Int("hard_ceiling_updates_per_second", a.capacity.hardCeiling), zap.Int("minimum_updates_per_second", a.capacity.minimum), zap.String("reason", a.capacity.reason), zap.Duration("read_p99", a.capacity.p99), zap.Duration("read_reference", a.capacity.baseline), zap.Bool("recovery_blocked", s.RecoveryBlocked),
			zap.Bool("host_valid", sample.Host.Valid), zap.Bool("disk_valid", sample.Host.DiskValid && sample.SpaceValid), zap.Float64("cpu_idle", sample.Host.Idle), zap.Float64("io_wait", sample.Host.IOWait), zap.Float64("memory_headroom", sample.Host.Memory), zap.Float64("gc_cpu", sample.Host.GC), zap.Float64("disk_busy", sample.Host.DiskBusy), zap.Uint64("disk_free_bytes", sample.FreeBytes), zap.Bool("write_stalled", sample.Stalled), zap.Uint64("compaction_debt_bytes", sample.Debt), zap.Int64("l0_sublevels", sample.L0))
	}
	// The effective reason distinguishes a capacity cap from ordinary demand.
	if float64(limit) < a.controller.target {
		a.controller.reason = "capacity"
	}
}
