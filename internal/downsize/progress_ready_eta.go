package downsize

import (
	"math"
	"time"
)

// READY is an attestation, not the end of the current copy/verify phase. Keep
// unmeasured work explicit: -1 is unknown; known is only a partial projection.
// In particular, a verify pass can discover more repairs until its last batch.
// Neither a hole-point counter nor a previous round predicts that future work.
type readyETAEstimate struct {
	seconds, known  float64
	unknown, phases int
}

type readyETAModel struct {
	path, phase string
	round       int
	samples     []progressMetricSample
	rates       map[int]float64
	policy      effectiveSyncPolicy
	policyKnown bool
}

// estimate is called with progressMetrics.mu held. Rates are local to this
// client lifetime and use only fresh counters; replay is never throughput.
func (m *readyETAModel) estimate(now time.Time, path string, s progressSnapshot) readyETAEstimate {
	e := readyETAEstimate{seconds: -1}
	code, round := progressPhase(s)
	if code == 5 && s.Status == "complete" {
		return readyETAEstimate{}
	}
	if code < 1 || code == 6 || code == 7 || code == 9 || s.Status != "running" {
		m.samples = nil
		e.unknown, e.phases = 1, -1
		return e
	}
	// Minimum known remaining stages, including final READY checks. Further
	// repair/convergence may add arbitrarily many stages, so this is not a cap.
	e.phases = map[int]int{1: 4, 2: 3, 3: 2, 4: 3, 5: 1}[code]
	if code == 5 {
		m.samples = nil
		e.unknown = 1 // final parity/read-back is still running, not READY yet
		return e
	}
	if s.Effective != nil && (!m.policyKnown || m.policy != *s.Effective) {
		m.samples, m.rates = nil, nil
		m.policy, m.policyKnown = *s.Effective, true
	}
	if m.rates == nil {
		m.rates = map[int]float64{}
	}
	phase := metricToken(s.Phase)
	fresh := float64(s.FreshDone)
	if m.path != path || m.phase != phase || m.round != round ||
		len(m.samples) > 0 && fresh < m.samples[len(m.samples)-1].done {
		m.samples = nil
	}
	m.path, m.phase, m.round = path, phase, round
	currentRate := float64(0)
	if s.FreshDoneAvailable {
		m.samples = append(m.samples, progressMetricSample{now, fresh})
		cutoff := now.Add(-3 * time.Minute)
		for len(m.samples) > 1 && m.samples[0].at.Before(cutoff) {
			m.samples = m.samples[1:]
		}
		first := m.samples[0]
		if seconds := now.Sub(first.at).Seconds(); seconds > 0 {
			currentRate = (fresh - first.done) / seconds
		}
	} else {
		m.samples = nil
	}
	if currentRate > 0 && !math.IsInf(currentRate, 0) && !math.IsNaN(currentRate) {
		m.rates[code] = currentRate
	} else {
		currentRate = 0
		delete(m.rates, code)
	}
	add := func(names int64, rate float64) {
		if names == 0 {
			return
		}
		if names < 0 || rate <= 0 {
			e.unknown++
			return
		}
		seconds := float64(names) / rate
		if math.IsInf(seconds, 0) || math.IsNaN(seconds) {
			e.unknown++
			return
		}
		e.known += seconds
	}
	if s.Total <= 0 {
		e.unknown++
	} else {
		add(max(0, s.Total-s.Done), currentRate)
	}
	switch code {
	case 1:
		e.unknown++ // discovery has not established copy/verify denominators
	case 2, 4:
		if s.NextVerifyNames == nil {
			e.unknown++
		} else {
			add(*s.NextVerifyNames, m.rates[3])
		}
	}
	e.unknown++ // repairs/residuals revealed by the next verification
	e.unknown++ // final READY parity, gap coverage and read-back checks
	// Unknown parts never silently become a finite end-to-end duration.
	if e.unknown == 0 {
		e.seconds = e.known
	}
	return e
}
