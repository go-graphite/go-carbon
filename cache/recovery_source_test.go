package cache

import (
	"github.com/go-graphite/go-carbon/points"
	"testing"
)

func TestRecoverySourceTracksMergedAndInFlightBatches(t *testing.T) {
	c := New()
	var stream RecoveryStream
	a, b := stream.Next(), stream.Next()
	c.AddRecoveredFrom(points.OnePoint("metric", 1, 1), a)
	c.AddRecoveredFrom(points.OnePoint("metric", 2, 1), b)
	if a.Pending() != 1 || b.Pending() != 0 {
		t.Fatal(a.Pending(), b.Pending())
	}
	p, _ := c.PopNotConfirmed("metric")
	c.AddRecoveredFrom(points.OnePoint("metric", 3, 1), b)
	if a.Pending() != 1 || b.Pending() != 1 {
		t.Fatal(a.Pending(), b.Pending())
	}
	c.Requeue(p)
	if a.Pending() != 1 || b.Pending() != 0 {
		t.Fatal("requeue lost earliest authority", a.Pending(), b.Pending())
	}
	p, _ = c.PopNotConfirmed("metric")
	for i, d := range p.Data {
		if d.Value != float64(i+1) {
			t.Fatal("requeue FIFO changed", p)
		}
	}
	c.Confirm(p)
	if a.Pending() != 0 || b.Pending() != 0 {
		t.Fatal("confirmation did not release sources")
	}
	c.AddRecoveredFrom(points.OnePoint("metric", 4, 1), b)
	c.Pop("metric")
	if b.Pending() != 0 {
		t.Fatal("discarded batch retained source")
	}
}
