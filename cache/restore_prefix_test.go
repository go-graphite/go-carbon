package cache

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestRestorePrefixSnapshotConcurrentConfirm(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	var flights []*points.Points
	for i := range 4096 {
		name := fmt.Sprintf("metric%d", i)
		c.AddRestoredBeforeInput(points.OnePoint(name, 1, 1))
		p, _ := c.PopNotConfirmed(name)
		flights = append(flights, p)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for _, p := range flights {
			c.Confirm(p)
		}
	})
	c.TrackRestorePrefix()
	wg.Wait()
	if c.RestoreOutstanding() != 0 {
		t.Fatal("concurrent confirmations stranded saved history", c.RestoreOutstanding())
	}
}

func TestRestorePrefixSurvivesRetryAndLiveTraffic(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.AddRestoredBeforeInput(points.OnePoint("history", 1, 1))
	flight, _ := c.PopNotConfirmed("history")
	c.AddRestoredBeforeInput(points.OnePoint("history", 2, 2))
	c.TrackRestorePrefix()
	c.Add(points.OnePoint("history", 3, 3))
	c.Add(points.OnePoint("live", 4, 4))
	c.Requeue(flight)
	if c.RestoreOutstanding() != 2 || !c.RestorePending("history") || c.RestorePending("live") {
		t.Fatal("retry changed saved prefix")
	}
	p, _ := c.PopNotConfirmed("history")
	if len(p.Data) != 3 || p.Data[0].Value != 1 || p.Data[2].Value != 3 {
		t.Fatal("source order changed", p)
	}
	c.Confirm(p)
	c.Confirm(p) // A duplicate confirmation must not settle another batch.
	if c.RestoreOutstanding() != 0 || c.RestorePending("history") || c.IsEmpty() {
		t.Fatal("live traffic extended the finite drain")
	}
}

func TestRestorePrefixNamesIncludeConfirmedFlights(t *testing.T) {
	c := New()
	c.AddRestoredBeforeInput(points.OnePoint("inflight.only", 1, 1))
	p, _ := c.PopNotConfirmed("inflight.only")
	names := c.TrackRestorePrefix()
	c.Confirm(p)
	for range 2 {
		if got := slices.Collect(names); !slices.Equal(got, []string{"inflight.only"}) {
			t.Fatal("confirmation hid a name from index preparation", got)
		}
	}
}

func TestRestorePrefixPrioritizesHistory(t *testing.T) {
	for _, strategy := range []string{"max", "sorted", "noop", "lexical"} {
		t.Run(strategy, func(t *testing.T) {
			c := New()
			c.SetMaxSize(0)
			c.SetWriteStrategy(strategy)
			c.AddRestoredBeforeInput(points.OnePoint("z.history", 1, 100))
			c.TrackRestorePrefix()
			for i := range 10 {
				c.Add(&points.Points{Metric: fmt.Sprintf("a.live%d", i), Data: []points.Point{{Value: 2, Timestamp: 1}, {Value: 3, Timestamp: 2}}})
			}
			q, _ := c.makeQueueAt(time.Now())
			if first := <-q; first != "z.history" {
				t.Fatal("live points displaced history", first)
			}
			c.Pop("z.history") // A rejected metric also settles its prefix.
			if c.RestoreOutstanding() != 0 {
				t.Fatal("rejected metric stranded prefix")
			}
		})
	}
}
