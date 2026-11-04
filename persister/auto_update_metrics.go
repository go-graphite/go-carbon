package persister

import (
	"errors"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

var autoUpdateReasons = [...]string{"steady", "growth", "pressure", "deadline", "floor", "ceiling", "restart", "recovery"}

type autoUpdateMetrics struct {
	owner                         atomic.Pointer[Whisper]
	adjustments                   [len(autoUpdateReasons)]atomic.Uint64
	rate, target, reason, changes *prometheus.Desc
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
		rate:    prometheus.NewDesc("persister_auto_update_rate", "Effective automatic writeout limit in updates/s, including restart and recovery.", nil, nil),
		target:  prometheus.NewDesc("persister_auto_update_rate_target", "Automatic writeout target in updates/s before downward smoothing.", nil, nil),
		reason:  prometheus.NewDesc("persister_auto_update_rate_reason", "Reason for the current automatic rate target (one-hot).", []string{"reason"}, nil),
		changes: prometheus.NewDesc("persister_auto_update_rate_adjustments_total", "Automatic writeout rate changes, by reason, across persister reloads.", []string{"reason"}, nil),
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
	a := &p.autoUpdate
	a.mu.Lock()
	rate, target, reason = float64(a.applied), a.controller.target, a.reason
	a.mu.Unlock()
	return
}

func (m *autoUpdateMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{m.rate, m.target, m.reason, m.changes} {
		ch <- d
	}
}

func (m *autoUpdateMetrics) Collect(ch chan<- prometheus.Metric) {
	p := m.owner.Load()
	if p == nil || p.maxUpdatesPerSecond != -1 {
		return
	}
	rate, target, reason := p.autoUpdateObservation()
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
