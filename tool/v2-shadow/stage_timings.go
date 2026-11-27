package main

import "time"

// Tail/admission/buffer are point-weighted frame intervals. Core phases are
// operation-weighted owner seals, and may overlap in time across owners.
// seal_total includes rollup_plan and publication; quantiles cannot be added.
type stageTimings struct {
	tail, admission, buffer   visibilityHistogram
	seal, rollup, publication visibilityHistogram
}

func (s *stageTimings) observe(phase string, d time.Duration) {
	var h *visibilityHistogram
	switch phase {
	case "seal_total":
		h = &s.seal
	case "rollup_plan":
		h = &s.rollup
	case "publication":
		h = &s.publication
	default:
		return
	}
	end := time.Now()
	start := end.Add(-d)
	h.record([]visibilityFrame{{Stamp: appendStamp{start, start}, Points: 1}}, end)
}
func (s *stageTimings) report() map[string]any {
	out := map[string]any{}
	for name, h := range map[string]*visibilityHistogram{"input_tail": &s.tail, "admission": &s.admission, "admission_buffer": &s.buffer, "seal_total": &s.seal, "rollup_plan": &s.rollup, "publication": &s.publication} {
		r := h.report()
		r["definition"] = "duration between stage boundaries; conservative 10ms buckets"
		r["weighting"] = "points"
		if name == "seal_total" || name == "rollup_plan" || name == "publication" {
			r["weighting"] = "owner seal operations"
		}
		out[name] = r
	}
	return out
}
