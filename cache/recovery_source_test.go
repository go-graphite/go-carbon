package cache

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
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

func TestCapturedStreamDoesNotPinFiniteCheckpoint(t *testing.T) {
	c := New()
	if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
		t.Fatal(err)
	}
	var stream RecoveryStream
	source := stream.Next()
	c.AddRecoveredFrom(points.OnePoint("metric", 9, 1), source)
	c.AddCapturedFrom(points.OnePoint("fresh", 10, 1), source)
	if err := c.RecoverPending(make(chan struct{}), 0); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"metric", "other"} {
		p, ok := c.PopNotConfirmed(name)
		if !ok {
			t.Fatal(name)
		}
		c.Confirm(p)
	}
	if c.PendingOutstanding() != 0 {
		t.Fatal("ongoing capture pinned old checkpoint", c.PendingOutstanding())
	}
	if err := c.FinishPendingRecovery(); err != nil {
		t.Fatal(err)
	}
	if c.HasPendingRecovery() || source.Pending() != 1 {
		t.Fatal("independent authority lost")
	}
	p, _ := c.PopNotConfirmed("fresh")
	c.Confirm(p)
	if source.Pending() != 0 {
		t.Fatal(source.Pending())
	}
}
