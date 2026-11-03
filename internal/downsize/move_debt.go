package downsize

import "math"

// Debt uses the same min/median/max observation and durable admission record as
// backlog. A short compaction spike is normal; sustained excess or a monotonic
// rise outside the measured swing parks copy admission.
type debtStatus struct {
	Baseline                     *backlogBaseline `json:"baseline"`
	Current                      uint64           `json:"current_bytes"`
	Limit                        float64          `json:"soft_limit_bytes"`
	High, Growth, GrowthRequired int
	Soft                         string `json:"soft,omitempty"`
}

func (h *hostLimit) observeDebt(sample hostSample, store storageSample, required int, collecting bool) debtStatus {
	b := h.debtBaseline
	if collecting && b == nil && store.CompactionDebt <= math.MaxInt64 {
		if len(h.debtSamples) == 0 {
			h.debtStart = sample.Time
		}
		h.debtSamples = append(h.debtSamples, int64(store.CompactionDebt))
	}
	s := debtStatus{Baseline: b, Current: store.CompactionDebt, GrowthRequired: required}
	if b == nil {
		return s
	}
	s.Limit = max(float64(8<<30), float64(b.Max)*1.25)
	if float64(store.CompactionDebt) > s.Limit {
		h.debtHigh++
	} else {
		h.debtHigh = 0
	}
	current := int64(min(store.CompactionDebt, uint64(math.MaxInt64)))
	if beyondBacklogSwing(current, b) {
		if h.storePrevious != nil && beyondBacklogSwing(int64(min(h.storePrevious.CompactionDebt, uint64(math.MaxInt64))), b) && store.CompactionDebt > h.storePrevious.CompactionDebt {
			h.debtGrowth++
		} else {
			h.debtGrowth = 1
		}
	} else {
		h.debtGrowth = 0
	}
	s.High, s.Growth = h.debtHigh, h.debtGrowth
	if h.debtHigh >= 3 {
		s.Soft = "compaction debt ratio"
	} else if h.debtGrowth >= required {
		s.Soft = "compaction debt growth"
	}
	return s
}
