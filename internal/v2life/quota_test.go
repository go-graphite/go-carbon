package v2life

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

type admitIndex struct{ calls int }

func (i *admitIndex) ShouldThrottleMetric(*points.Points, bool) bool { i.calls++; return false }
func TestQuotaRecoveryFence(t *testing.T) {
	i := &admitIndex{}
	ready := false
	q, err := NewQuotas(i, func() bool { return ready })
	if err != nil {
		t.Fatal(err)
	}
	if !q.Throttle(points.OnePoint("service.cpu", 1, 1), true) || i.calls != 0 {
		t.Fatal("quota admitted during recovery")
	}
	ready = true
	if q.Throttle(points.OnePoint("service.cpu", 1, 1), true) || i.calls != 1 {
		t.Fatal("ready quota did not delegate")
	}
	if _, err = NewQuotas(nil, func() bool { return true }); err == nil {
		t.Fatal("missing index")
	}
	if _, err = NewQuotas(i, nil); err == nil {
		t.Fatal("missing readiness fence")
	}
}
