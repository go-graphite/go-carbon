package downsize

import "math"

type latencyStatus struct {
	Baseline       *backlogBaseline `json:"baseline_microseconds"`
	Limit, Current float64
	High           int
	Soft           string `json:"soft,omitempty"`
}

func (h *hostLimit) observeLatency(sample hostSample, collecting bool) latencyStatus {
	b := h.latencyBaseline
	s := latencyStatus{Baseline: b, Current: sample.DiskAwaitMS}
	if math.IsNaN(sample.DiskAwaitMS) || math.IsInf(sample.DiskAwaitMS, 0) || sample.DiskAwaitMS < 0 || sample.DiskAwaitMS > float64(math.MaxInt64)/1000 {
		s.Soft = "invalid disk latency telemetry"
		return s
	}
	if collecting && b == nil {
		if len(h.latencySamples) == 0 {
			h.latencyStart = sample.Time
		}
		h.latencySamples = append(h.latencySamples, int64(math.Ceil(sample.DiskAwaitMS*1000)))
	}
	if b == nil {
		return s
	}
	floor := 10.0
	if sample.DiskUtil > 90 {
		floor = 2
	}
	s.Limit = max(floor, float64(b.Max)/1000*1.25)
	if sample.DiskAwaitMS > s.Limit {
		h.latencyHigh++
	} else {
		h.latencyHigh = 0
	}
	s.High = h.latencyHigh
	if h.latencyHigh >= 3 {
		s.Soft = "data-drive latency"
	}
	return s
}
