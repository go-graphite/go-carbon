package persister

import (
	"errors"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

var autoUpdateReasons = [...]string{"steady", "growth", "pressure", "deadline", "floor", "ceiling", "restart", "recovery", "recovery_blocked", "capacity"}

type autoUpdateMetrics struct {
	owner                                                                atomic.Pointer[Whisper]
	adjustments                                                          [len(autoUpdateReasons)]atomic.Uint64
	rate, target, reason, changes                                        *prometheus.Desc
	capacity, hardCeiling, minimum, readP99, readReference, tokenWaiters *prometheus.Desc
}

func (m *autoUpdateMetrics) adjust(reason string) {
	for i, label := range autoUpdateReasons {
		if label == reason {
			m.adjustments[i].Add(1)
			return
		}
	}
}

func (p *Whisper) initAutoUpdateMetrics(reg prometheus.Registerer) {
	m := &autoUpdateMetrics{
		rate:          prometheus.NewDesc("persister_auto_update_rate", "Effective automatic writeout limit in updates/s, including restart and recovery.", nil, nil),
		target:        prometheus.NewDesc("persister_auto_update_rate_target", "Automatic writeout target in updates/s before downward smoothing.", nil, nil),
		reason:        prometheus.NewDesc("persister_auto_update_rate_reason", "Reason for the current automatic rate target (one-hot).", []string{"reason"}, nil),
		changes:       prometheus.NewDesc("persister_auto_update_rate_adjustments_total", "Automatic writeout rate changes, by reason, across persister reloads.", []string{"reason"}, nil),
		capacity:      prometheus.NewDesc("persister_auto_capacity_updates_per_second", "Current hardware feedback ceiling (-1 auto-max mode); zero in other modes.", nil, nil),
		hardCeiling:   prometheus.NewDesc("persister_auto_capacity_hard_ceiling_updates_per_second", "Envelope from workers, execution threads and confirmed CPU/service cost, subject to the protected steady-demand floor unless hard pressure exists.", nil, nil),
		minimum:       prometheus.NewDesc("persister_auto_capacity_minimum_updates_per_second", "Protected steady-demand floor (controller need times margin); only configured auto-min during hard storage, space or memory pressure.", nil, nil),
		readP99:       prometheus.NewDesc("persister_auto_capacity_read_p99_seconds", "Per-series storage-fetch p99 in the last controller interval, excluding request assembly and network time; zero when no reads completed.", nil, nil),
		readReference: prometheus.NewDesc("persister_auto_capacity_read_reference_seconds", "Median of quiet-write per-series fetch p99 samples from the last hour; at most one sample per minute.", nil, nil),
		tokenWaiters:  prometheus.NewDesc("persister_update_token_waiters", "Workers currently blocked waiting for update-rate tokens in hardware capacity mode; zero in other modes.", nil, nil),
	}
	if err := reg.Register(m); err != nil {
		var registered prometheus.AlreadyRegisteredError
		if !errors.As(err, &registered) {
			panic(err)
		}
		m = registered.ExistingCollector.(*autoUpdateMetrics)
	}
	p.autoUpdate.metrics = m
	m.owner.Store(p)
}

func (p *Whisper) autoUpdateObservation() (rate, target float64, reason string) {
	if s := p.autoUpdate.observed.Load(); s != nil {
		return s.rate, s.target, s.reason
	}
	return
}

func (m *autoUpdateMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{m.rate, m.target, m.reason, m.changes, m.capacity, m.hardCeiling, m.minimum, m.readP99, m.readReference, m.tokenWaiters} {
		ch <- d
	}
}

func (m *autoUpdateMetrics) Collect(ch chan<- prometheus.Metric) {
	p := m.owner.Load()
	if p == nil {
		return
	}
	// The registry can expose a replacement before Start publishes settings.
	// Read only immutable constructor data until then, not uninitialized tickers.
	mode := p.maxUpdatesPerSecond
	if settings := p.liveSettings.Load(); settings != nil {
		mode = settings.maxUpdatesPerSecond
	}
	if mode != -1 {
		return
	}
	rate, target, reason := p.autoUpdateObservation()
	p.autoUpdate.mu.Lock()
	capacity, hard, minimum, p99, reference := 0, 0, 0, 0., 0.
	if p.autoUpdate.ceiling == -1 {
		capacity, hard, p99 = p.autoUpdate.capacity.limit, p.autoUpdate.capacity.hardCeiling, p.autoUpdate.capacity.p99.Seconds()
		reference = p.autoUpdate.capacity.baseline.Seconds()
		minimum = p.autoUpdate.capacity.minimum
	}
	p.autoUpdate.mu.Unlock()
	ch <- prometheus.MustNewConstMetric(m.capacity, prometheus.GaugeValue, float64(capacity))
	ch <- prometheus.MustNewConstMetric(m.hardCeiling, prometheus.GaugeValue, float64(hard))
	ch <- prometheus.MustNewConstMetric(m.minimum, prometheus.GaugeValue, float64(minimum))
	ch <- prometheus.MustNewConstMetric(m.readP99, prometheus.GaugeValue, p99)
	ch <- prometheus.MustNewConstMetric(m.readReference, prometheus.GaugeValue, reference)
	ch <- prometheus.MustNewConstMetric(m.tokenWaiters, prometheus.GaugeValue, float64(p.autoUpdate.tokenWaiters.Load()))
	ch <- prometheus.MustNewConstMetric(m.rate, prometheus.GaugeValue, rate)
	ch <- prometheus.MustNewConstMetric(m.target, prometheus.GaugeValue, target)
	for i, label := range autoUpdateReasons {
		value := 0.0
		if reason == label {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(m.reason, prometheus.GaugeValue, value, label)
		ch <- prometheus.MustNewConstMetric(m.changes, prometheus.CounterValue, float64(m.adjustments[i].Load()), label)
	}
}
