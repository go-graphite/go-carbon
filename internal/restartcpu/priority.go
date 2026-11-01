package restartcpu

import (
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// BeginWriteout marks urgent work that must run before input is ready.
func (b *Budget) BeginWriteout() {
	b.writers.Add(1)
	b.writeGrants.Add(1)
}

// WaitWriteout shares process CPU credit throughout restart and backlog drain.
// Waiting writers reserve priority over optional catalog warming.
// A successful admission must be paired with EndWriteout.
func (b *Budget) WaitWriteout(exit <-chan bool) bool {
	return b.WaitWriteoutBatch(exit, 1)
}

// WaitWriteoutBatch reserves every update sharing a store commit.
func (b *Budget) WaitWriteoutBatch(exit <-chan bool, updates int) bool {
	return b.waitWork(exit, false, max(1, updates))
}

// WaitPrefetch admits required queue preparation or cold-state prefetch from
// the same process credit. It cannot wait behind the writes that need it.
// Pair a successful admission with EndPrefetch.
func (b *Budget) WaitPrefetch(exit <-chan bool) bool { return b.waitWork(exit, true, 1) }
func (b *Budget) BeginPrefetch() {
	b.prefetchers.Add(1)
	b.warmGrants.Add(1)
}
func (b *Budget) EndPrefetch() { b.prefetchers.Add(-1) }

func (b *Budget) waitWork(exit <-chan bool, warming bool, updates int) bool {
	pending, grants := &b.writers, &b.writeGrants
	if warming {
		pending, grants = &b.prefetchers, &b.warmGrants
	}
	pending.Add(1)
	for b.Active() {
		delay := b.reserveDelay(time.Now(), true, float64(updates)*b.writeCost())
		if delay == 0 {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-exit:
			timer.Stop()
			pending.Add(-1)
			return false
		case <-timer.C:
		}
	}
	grants.Add(1)
	return true
}

func (b *Budget) EndWriteout()    { b.writers.Add(-1) }
func (b *Budget) RecordWrite()    { b.written.Add(1) }
func (b *Budget) Updates() uint64 { return b.written.Load() }

// ObserveWriteout samples confirmed writes, not attempts or queue admissions.
// The caller logs a warning when a floor violation persists for more than 10s.
func (b *Budget) ObserveWriteout(floor, rate float64) bool {
	now := time.Now()
	if b.cores > 0 {
		b.delay(now)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.released, b.floor, b.rate = true, floor, rate
	if floor <= 0 || rate >= floor {
		b.belowAt = time.Time{}
		return false
	}
	if b.belowAt.IsZero() {
		b.belowAt = now
	}
	if now.Sub(b.belowAt) > 10*time.Second && (b.warnedAt.IsZero() || now.Sub(b.warnedAt) >= 10*time.Second) {
		b.warnedAt = now
		return true
	}
	return false
}

type State struct {
	Allowance, Process, Warming, Writeout, Checkpoint, Floor, Rate, Ceiling    float64
	WarmGrants, WriteGrants, CheckpointGrants, CheckpointWork, CheckpointTotal uint64
	CheckpointLoading                                                          bool
	BelowFor                                                                   time.Duration
}

func (b *Budget) SetRateCeiling(rate float64) { b.rateCeiling.Store(math.Float64bits(rate)) }

// State describes admission grants, not attributed CPU consumption. During
// warming consumers share a finite allowance; their grants never add above it.
func (b *Budget) State() State {
	active := b.Active()
	b.mu.Lock()
	defer b.mu.Unlock()
	s := State{Allowance: b.cores, Process: b.process, Floor: b.floor, Rate: b.rate,
		Ceiling:    math.Float64frombits(b.rateCeiling.Load()),
		WarmGrants: b.warmGrants.Load(), WriteGrants: b.writeGrants.Load(),
		CheckpointGrants: b.checkpointGrants.Load(), CheckpointWork: b.checkpointWork.Load(),
		CheckpointTotal: b.checkpointTotal.Load(), CheckpointLoading: b.checkpoints.Load() > 0}
	writers, prefetchers := b.writers.Load(), b.prefetchers.Load()
	if !active {
		s.Warming = math.Inf(1)
		if b.released || writers > 0 {
			s.Writeout = math.Inf(1)
		}
	} else {
		available := max(0, b.cores-b.process)
		s.Checkpoint = min(available, b.checkpointShareLocked())
		available -= s.Checkpoint
		if prefetchers > 0 {
			// Report headroom among required jobs, retaining writeout demand
			// across the rate-token and queue waits between registered writers.
			s.Warming = available
			if b.released || writers > 0 {
				s.Warming *= float64(prefetchers) / float64(prefetchers+writers)
				if writers == 0 {
					// A rate-token or queue wait is still writeout demand after
					// release. Required preparation must not claim all headroom.
					s.Warming = available / 2
				}
				s.Writeout = available - s.Warming
			}
		} else if b.released || writers > 0 {
			s.Writeout = available
		} else if b.belowAt.IsZero() && b.credit >= 0 {
			s.Warming = available
		}
	}
	if !b.belowAt.IsZero() {
		s.BelowFor = time.Since(b.belowAt)
	}
	return s
}

var (
	grantDesc              = prometheus.NewDesc("restart_cpu_granted_cores", "CPU admission grant by consumer; finite shared allowance while warming, +Inf after pacing ends.", []string{"consumer"}, nil)
	countDesc              = prometheus.NewDesc("restart_cpu_grants_total", "Admitted work windows or writeout jobs by consumer, not CPU consumption.", []string{"consumer"}, nil)
	floorDesc              = prometheus.NewDesc("restart_cpu_writeout_floor_updates_per_second", "Minimum restart writeout rate: predecessor throughput or queued metrics divided by max delay, whichever is greater.", nil, nil)
	rateDesc               = prometheus.NewDesc("restart_cpu_writeout_updates_per_second", "Confirmed writeout updates per second during restart.", nil, nil)
	processDesc            = prometheus.NewDesc("restart_cpu_process_cores", "Whole-process user plus system CPU cores, five-second EWMA updated once per second; admission uses raw CPU deltas.", nil, nil)
	allowanceDesc          = prometheus.NewDesc("restart_cpu_allowance_cores", "Whole-process restart CPU allowance.", nil, nil)
	ceilingDesc            = prometheus.NewDesc("restart_cpu_writeout_ceiling_updates_per_second", "Restart rate-token ceiling, before whole-process CPU admission; zero means unlimited.", nil, nil)
	checkpointProgressDesc = prometheus.NewDesc("restart_cpu_checkpoint_load_bytes", "Checkpoint stream progress in file bytes; total is zero until known.", []string{"kind"}, nil)
	checkpointLoadingDesc  = prometheus.NewDesc("restart_cpu_checkpoint_loading", "One while a checkpoint accelerator load is pending or running.", nil, nil)
)

type metrics struct{ budget atomic.Pointer[Budget] }

func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{grantDesc, countDesc, floorDesc, rateDesc, processDesc, allowanceDesc, ceilingDesc, checkpointProgressDesc, checkpointLoadingDesc} {
		ch <- d
	}
}

func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	s := m.budget.Load().State()
	ch <- prometheus.MustNewConstMetric(grantDesc, prometheus.GaugeValue, s.Warming, "warming")
	ch <- prometheus.MustNewConstMetric(grantDesc, prometheus.GaugeValue, s.Writeout, "writeout")
	ch <- prometheus.MustNewConstMetric(grantDesc, prometheus.GaugeValue, s.Checkpoint, "checkpoint_load")
	ch <- prometheus.MustNewConstMetric(countDesc, prometheus.CounterValue, float64(s.WarmGrants), "warming")
	ch <- prometheus.MustNewConstMetric(countDesc, prometheus.CounterValue, float64(s.WriteGrants), "writeout")
	ch <- prometheus.MustNewConstMetric(countDesc, prometheus.CounterValue, float64(s.CheckpointGrants), "checkpoint_load")
	ch <- prometheus.MustNewConstMetric(floorDesc, prometheus.GaugeValue, s.Floor)
	ch <- prometheus.MustNewConstMetric(rateDesc, prometheus.GaugeValue, s.Rate)
	ch <- prometheus.MustNewConstMetric(processDesc, prometheus.GaugeValue, s.Process)
	ch <- prometheus.MustNewConstMetric(allowanceDesc, prometheus.GaugeValue, s.Allowance)
	ch <- prometheus.MustNewConstMetric(ceilingDesc, prometheus.GaugeValue, s.Ceiling)
	ch <- prometheus.MustNewConstMetric(checkpointProgressDesc, prometheus.GaugeValue, float64(s.CheckpointWork), "completed")
	ch <- prometheus.MustNewConstMetric(checkpointProgressDesc, prometheus.GaugeValue, float64(s.CheckpointTotal), "total")
	loading := 0.
	if s.CheckpointLoading {
		loading = 1
	}
	ch <- prometheus.MustNewConstMetric(checkpointLoadingDesc, prometheus.GaugeValue, loading)
}

func (b *Budget) InitPrometheus(reg prometheus.Registerer) {
	m := new(metrics)
	m.budget.Store(b)
	if err := reg.Register(m); err != nil {
		var registered prometheus.AlreadyRegisteredError
		if !errors.As(err, &registered) {
			panic(err)
		}
		registered.ExistingCollector.(*metrics).budget.Store(b)
	}
}
