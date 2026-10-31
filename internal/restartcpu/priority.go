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

// WaitWriteout shares process CPU credit while warming is unfinished. Waiting
// writers reserve priority over optional validation. After warming, ordinary
// writeout limits take over, so optional CPU debt cannot stall a warm backlog.
// A successful admission must be paired with EndWriteout.
func (b *Budget) WaitWriteout(exit <-chan bool) bool {
	b.writers.Add(1)
	for b.Active() && !b.warmed.Load() {
		delay := b.admissionDelay(time.Now(), true)
		if delay == 0 {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-exit:
			timer.Stop()
			b.writers.Add(-1)
			return false
		case <-timer.C:
		}
	}
	b.writeGrants.Add(1)
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
	Allowance, Process, Warming, Writeout, Floor, Rate float64
	WarmGrants, WriteGrants                            uint64
	BelowFor                                           time.Duration
}

// State describes admission grants, not attributed CPU consumption. During
// warming consumers share a finite allowance; their grants never add above it.
func (b *Budget) State() State {
	active := b.Active()
	b.mu.Lock()
	defer b.mu.Unlock()
	s := State{Allowance: b.cores, Process: b.process, Floor: b.floor, Rate: b.rate,
		WarmGrants: b.warmGrants.Load(), WriteGrants: b.writeGrants.Load()}
	if b.released || b.writers.Load() > 0 {
		if active && !b.warmed.Load() {
			s.Writeout = max(0, b.cores-b.process)
		} else {
			s.Writeout = math.Inf(1)
		}
	}
	if !active {
		s.Warming = math.Inf(1)
	} else if b.writers.Load() == 0 && b.belowAt.IsZero() && b.credit >= 0 {
		s.Warming = max(0, b.cores-b.process-s.Writeout)
	}
	if !b.belowAt.IsZero() {
		s.BelowFor = time.Since(b.belowAt)
	}
	return s
}

var (
	grantDesc   = prometheus.NewDesc("restart_cpu_granted_cores", "CPU admission grant by consumer; finite shared allowance while warming, +Inf after pacing ends.", []string{"consumer"}, nil)
	countDesc   = prometheus.NewDesc("restart_cpu_grants_total", "Admitted work windows or writeout jobs by consumer, not CPU consumption.", []string{"consumer"}, nil)
	floorDesc   = prometheus.NewDesc("restart_cpu_writeout_floor_updates_per_second", "Minimum restart writeout rate derived from queued metrics and max delay.", nil, nil)
	rateDesc    = prometheus.NewDesc("restart_cpu_writeout_updates_per_second", "Confirmed writeout updates per second during restart.", nil, nil)
	processDesc = prometheus.NewDesc("restart_cpu_process_cores", "Latest measured whole-process user plus system CPU cores.", nil, nil)
)

type metrics struct{ budget atomic.Pointer[Budget] }

func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{grantDesc, countDesc, floorDesc, rateDesc, processDesc} {
		ch <- d
	}
}

func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	s := m.budget.Load().State()
	ch <- prometheus.MustNewConstMetric(grantDesc, prometheus.GaugeValue, s.Warming, "warming")
	ch <- prometheus.MustNewConstMetric(grantDesc, prometheus.GaugeValue, s.Writeout, "writeout")
	ch <- prometheus.MustNewConstMetric(countDesc, prometheus.CounterValue, float64(s.WarmGrants), "warming")
	ch <- prometheus.MustNewConstMetric(countDesc, prometheus.CounterValue, float64(s.WriteGrants), "writeout")
	ch <- prometheus.MustNewConstMetric(floorDesc, prometheus.GaugeValue, s.Floor)
	ch <- prometheus.MustNewConstMetric(rateDesc, prometheus.GaugeValue, s.Rate)
	ch <- prometheus.MustNewConstMetric(processDesc, prometheus.GaugeValue, s.Process)
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
