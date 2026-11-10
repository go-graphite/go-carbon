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

func TestIndependentCaptureDoesNotPinOrRelogOlderJournal(t *testing.T) {
	c := New()
	c.EnableInputLog()
	var journal, spool RecoveryStream
	logged := journal.NextJournal()
	captured := spool.Next()
	c.AddRecoveredFrom(points.OnePoint("old.journal", 1, 1), logged)
	c.AddRecoveredFrom(points.OnePoint("new.spool", 2, 1), captured)
	for i := range c.data {
		c.TakeLogEntries(i, 1, nil)
	}
	if c.LogLowWater() != 0 {
		t.Fatal("old journal retired before confirmation")
	}
	p, _ := c.PopNotConfirmed("old.journal")
	c.Confirm(p)
	p, _ = c.PopNotConfirmed("new.spool")
	c.Requeue(p)
	if c.LogLowWater() != 1 {
		t.Fatal("independent capture/requeue pinned the predecessor journal")
	}
	if count := c.RelogBefore(2, 1000); count != 0 {
		t.Fatal("independent source redundantly relogged", count)
	}
	// A normal, journal-owned point merging into the same batch must still
	// acquire tracking and keep that generation until its confirmation.
	c.Add(points.OnePoint("new.spool", 3, 1))
	for i := range c.data {
		c.TakeLogEntries(i, 2, nil)
	}
	if c.LogLowWater() != 1 {
		t.Fatal("mixed live journal ownership lost")
	}
	p, _ = c.PopNotConfirmed("new.spool")
	c.Confirm(p)
	if c.LogLowWater() != 2 || logged.Pending() != 0 || captured.Pending() != 0 {
		t.Fatal("source/generation not released")
	}
}
