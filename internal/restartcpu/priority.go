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
	b.floorWork(1)
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

// WaitPrefetch shares process credit for required queue/cold-state preparation,
// with bounded progress windows when CPU debt would block the writeout floor.
// Pair a successful admission with EndPrefetch.
func (b *Budget) WaitPrefetch(exit <-chan bool) bool { return b.waitWork(exit, true, 1) }
func (b *Budget) BeginPrefetch() {
	b.floorWork(1)
	b.prefetchers.Add(1)
	b.warmGrants.Add(1)
}
func (b *Budget) EndPrefetch() {
	b.prefetchers.Add(-1)
	b.floorWork(-1)
}

func (b *Budget) waitWork(exit <-chan bool, warming bool, updates int) bool {
	pending, grants := &b.writers, &b.writeGrants
	if warming {
		pending, grants = &b.prefetchers, &b.warmGrants
	}
	pending.Add(1)
	b.floorWork(1)
	for b.Active() {
		writes, class := updates, writeoutWork
		if warming {
			writes, class = 0, preparationWork
		}
		delay := b.reserveAdmission(time.Now(), class, float64(updates)*b.writeCost(), writes)
		if delay == 0 {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-exit:
			timer.Stop()
			pending.Add(-1)
			b.floorWork(-1)
			return false
		case <-timer.C:
		}
	}
	grants.Add(1)
	return true
}

func (b *Budget) EndWriteout() {
	b.writers.Add(-1)
	b.floorWork(-1)
}
func (b *Budget) RecordWrite()    { b.written.Add(1) }
func (b *Budget) Updates() uint64 { return b.written.Load() }

// ObserveWriteout samples confirmed writes, not attempts or queue admissions.
// Tests and callers without a queue sample treat a positive floor as backlog.
func (b *Budget) ObserveWriteout(floor, rate float64) bool {
	warn, _ := b.ObserveWriteoutBacklog(floor, rate, floor > 0)
	return warn
}

// ObserveWriteoutBacklog reports a warning after 10s, then at most every five
// minutes. A sustained severe shortfall escalates once after three minutes.
// Only eligible backlog counts: ordinary batching may legitimately be quiet.
func (b *Budget) ObserveWriteoutBacklog(floor, rate float64, backlog bool) (warn, failed bool) {
	// An in-flight commit may own the last eligible cache item.
	backlog = backlog || b.writers.Load() > 0
	now := time.Now()
	if b.cores > 0 {
		b.delay(now)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.released, b.floor, b.rate = true, floor, rate
	if !backlog || floor <= 0 || rate >= .5*floor {
		b.severeAt, b.stalled = time.Time{}, false
	} else {
		if b.severeAt.IsZero() {
			b.severeAt = now
		}
		if !b.stalled && now.Sub(b.severeAt) >= 3*time.Minute {
			b.stalled = true
			b.stalls++
			failed = true
		}
	}
	if !backlog || floor <= 0 || rate >= floor {
		b.belowAt = time.Time{}
		return false, failed
	}
	if b.belowAt.IsZero() {
		b.belowAt = now
	}
	if now.Sub(b.belowAt) > 10*time.Second && (b.warnedAt.IsZero() || now.Sub(b.warnedAt) >= 5*time.Minute) {
		b.warnedAt = now
		warn = true
	}
	return warn, failed
}

type State struct {
	Allowance, Process, Warming, Writeout, Checkpoint, Floor, Rate, Ceiling    float64
	WarmGrants, WriteGrants, CheckpointGrants, CheckpointWork, CheckpointTotal uint64
	CheckpointLoading                                                          bool
	BelowFor                                                                   time.Duration
	Stalled                                                                    bool
	Stalls, FloorGrants                                                        uint64
}

func (b *Budget) SetRateCeiling(rate float64) { b.rateCeiling.Store(math.Float64bits(rate)) }

// State describes admission grants, not attributed CPU consumption. During
// warming consumers share a finite allowance; their grants never add above it.
func (b *Budget) State() State {
	active := b.Active()
	b.mu.Lock()
	defer b.mu.Unlock()
	s := State{Allowance: b.cores, Process: b.process, Floor: b.floor, Rate: b.rate,
		Ceiling: math.Float64frombits(b.rateCeiling.Load()),
		Stalled: b.stalled && active, Stalls: b.stalls, FloorGrants: b.floorGrants,
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
		s.Checkpoint = b.checkpointShareLocked()
		available = max(0, available-s.Checkpoint)
		switch {
		case prefetchers > 0:
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
		case b.released || writers > 0:
			s.Writeout = available
		case b.belowAt.IsZero() && b.credit >= 0:
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
	ceilingDesc            = prometheus.NewDesc("restart_cpu_writeout_ceiling_updates_per_second", "Restart CPU reservation rate; fixed mode also uses it for rate tokens, auto mode has its own token limit. Zero selects saved/default write cost.", nil, nil)
	checkpointProgressDesc = prometheus.NewDesc("restart_cpu_checkpoint_load_bytes", "Checkpoint stream progress in file bytes; total is zero until known.", []string{"kind"}, nil)
	checkpointLoadingDesc  = prometheus.NewDesc("restart_cpu_checkpoint_loading", "One while a checkpoint accelerator load is pending or running.", nil, nil)
	checkpointFailuresDesc = prometheus.NewDesc("restart_cpu_checkpoint_load_failures_total", "Invalid or failed enabled checkpoint loads, including unavailable checkpoints and timeouts.", nil, nil)
	checkpointTimeoutsDesc = prometheus.NewDesc("restart_cpu_checkpoint_load_timeouts_total", "Checkpoint loads or legacy validation that exceeded their deadline.", nil, nil)
	stalledDesc            = prometheus.NewDesc("restart_cpu_writeout_stalled", "One after three minutes below half the writeout floor with eligible backlog during restart pacing.", nil, nil)
	stallsDesc             = prometheus.NewDesc("restart_cpu_writeout_stalls_total", "Sustained severe restart writeout shortfalls, counted once per escalation.", nil, nil)
	floorGrantsDesc        = prometheus.NewDesc("restart_cpu_writeout_floor_grants_total", "Writeout batches admitted at the mandatory rate floor despite process CPU debt.", nil, nil)
)

type metrics struct{ budget atomic.Pointer[Budget] }

func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{grantDesc, countDesc, floorDesc, rateDesc, processDesc, allowanceDesc, ceilingDesc, checkpointProgressDesc, checkpointLoadingDesc, checkpointFailuresDesc, checkpointTimeoutsDesc, stalledDesc, stallsDesc, floorGrantsDesc} {
		ch <- d
	}
}

func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	b := m.budget.Load()
	s := b.State()
	ch <- prometheus.MustNewConstMetric(checkpointFailuresDesc, prometheus.CounterValue, float64(b.checkpointFailures.Load()))
	ch <- prometheus.MustNewConstMetric(checkpointTimeoutsDesc, prometheus.CounterValue, float64(b.checkpointTimeouts.Load()))
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
	stalled := 0.
	if s.Stalled {
		stalled = 1
	}
	ch <- prometheus.MustNewConstMetric(stalledDesc, prometheus.GaugeValue, stalled)
	ch <- prometheus.MustNewConstMetric(stallsDesc, prometheus.CounterValue, float64(s.Stalls))
	ch <- prometheus.MustNewConstMetric(floorGrantsDesc, prometheus.CounterValue, float64(s.FloorGrants))
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
