package cache

import (
	"github.com/go-graphite/go-carbon/points"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWarmFirstQueuePartitionsByReadinessThenAge(t *testing.T) {
	c := New()
	defer c.Stop()
	c.SetRestartOrder(func() bool { return true })
	c.SetRestartWarmFirst(func(name string) bool { return name == "warm-new" || name == "warm-old" }, nil, nil)
	c.Add(points.OnePoint("cold-new", 1, 20))
	c.Add(points.OnePoint("warm-new", 1, 40))
	c.Add(points.OnePoint("cold-old", 1, 10))
	c.Add(points.OnePoint("warm-old", 1, 30))
	queue := c.makeQueue()
	for _, want := range []string{"warm-old", "warm-new", "cold-old", "cold-new"} {
		if got := <-queue; got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	}
	if c.Size() != 4 {
		t.Fatal("queue took point ownership")
	}
}

func TestWarmFirstQueueRefreshFindsLaterWarmArrival(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New()
		defer c.Stop()
		c.SetRestartOrder(func() bool { return true })
		c.SetRestartWarmFirst(func(name string) bool { return name == "warm-later" }, nil, nil)
		c.Add(points.OnePoint("cold-one", 1, 1))
		c.Add(points.OnePoint("cold-two", 1, 2))
		q := c.WriteoutQueue()
		abort := make(chan bool)
		defer close(abort)
		if got := q.Get(abort); got != "cold-one" {
			t.Fatal(got)
		}
		c.Add(points.OnePoint("warm-later", 1, 100))
		time.Sleep(time.Second + time.Millisecond)
		if got := q.Get(abort); got != "warm-later" {
			t.Fatal("later warm metric remained behind cold snapshot", got)
		}
		if c.Size() != 3 {
			t.Fatal("refresh lost cached points")
		}
	})
}

func TestWarmFirstQueueAdmissionCancelsWithoutLocks(t *testing.T) {
	c := New()
	defer c.Stop()
	c.SetRestartOrder(func() bool { return true })
	entered := make(chan struct{})
	abort := make(chan bool)
	ended := make(chan struct{}, 1)
	c.SetRestartWarmFirst(func(string) bool { return true }, func(exit <-chan bool) bool { close(entered); <-exit; return false }, func() { ended <- struct{}{} })
	c.Add(points.OnePoint("warm", 1, 1))
	got := make(chan string, 1)
	go func() { got <- c.WriteoutQueue().Get(abort) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("queue did not request shared CPU")
	}
	// Admission holds neither cache nor queue locks.
	c.Add(points.OnePoint("other", 2, 2))
	close(abort)
	select {
	case name := <-got:
		if name != "" {
			t.Fatal(name)
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel queue admission")
	}
	select {
	case <-ended:
		t.Fatal("ungranted queue CPU was released")
	default:
	}
}

func TestWarmFirstQueueRestartsAfterCancelledAdmission(t *testing.T) {
	c := New()
	defer c.Stop()
	c.SetRestartOrder(func() bool { return true })
	entered := make(chan struct{})
	oldAbort := make(chan bool)
	var calls atomic.Int64
	c.SetRestartWarmFirst(func(string) bool { return true }, func(exit <-chan bool) bool {
		if calls.Add(1) == 1 {
			close(entered)
			<-exit
			return false
		}
		return true
	}, func() {})
	c.Add(points.OnePoint("warm", 1, 1))
	q := c.WriteoutQueue()
	oldResult := make(chan string, 1)
	go func() { oldResult <- q.Get(oldAbort) }()
	<-entered
	close(oldAbort)
	select {
	case <-oldResult:
	case <-time.After(time.Second):
		t.Fatal("old consumer did not stop")
	}
	newAbort := make(chan bool)
	defer close(newAbort)
	newResult := make(chan string, 1)
	go func() { newResult <- q.Get(newAbort) }()
	select {
	case name := <-newResult:
		if name != "warm" {
			t.Fatal(name)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled callback stranded the replacement consumer")
	}
	if calls.Load() < 2 {
		t.Fatal("replacement skipped queue CPU admission")
	}
}

func TestWarmFirstQueueKeepsFullyWarmSnapshot(t *testing.T) {
	c := New()
	defer c.Stop()
	c.SetRestartOrder(func() bool { return true })
	c.SetRestartWarmFirst(func(string) bool { return true }, nil, nil)
	c.Add(points.OnePoint("warm", 1, 1))
	q := c.WriteoutQueue()
	abort := make(chan bool)
	defer close(abort)
	if got := q.Get(abort); got != "warm" {
		t.Fatal(got)
	}
	q.RLock()
	refreshAt := q.refreshAt
	q.RUnlock()
	if !refreshAt.IsZero() {
		t.Fatal("fully warm work was forced to rescan before draining")
	}
}
