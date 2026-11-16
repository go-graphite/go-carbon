package v2life

import (
	"errors"

	"github.com/go-graphite/go-carbon/points"
)

// QuotaIndex is the existing host-wide admission index. It must represent the
// union of ALL hosted vshards and pending names, not one budget per owner.
// The current CarbonserverListener implements it without any writer-store read.
// Catalog publication/expiry must update this same index; constructing a new
// one per seal would incorrectly reset throughput usage.
type QuotaIndex interface {
	ShouldThrottleMetric(*points.Points, bool) bool
}

// Quotas keeps the existing classic index as the authority for admission,
// including empty batches, existing names, cache bypass, throughput windows,
// last-matching rule precedence and all storage usage comparisons. The host
// supplies one coherent index; this adapter never creates per-vshard budgets.
// Readiness fails closed until recovery and catalog publication finish.
type Quotas struct {
	index QuotaIndex
	ready func() bool
}

func NewQuotas(index QuotaIndex, ready func() bool) (*Quotas, error) {
	if index == nil || ready == nil {
		return nil, errors.New("host-wide quota index and readiness fence required")
	}
	return &Quotas{index: index, ready: ready}, nil
}
func (q *Quotas) Throttle(ps *points.Points, inCache bool) bool {
	return !q.ready() || q.index.ShouldThrottleMetric(ps, inCache)
}
