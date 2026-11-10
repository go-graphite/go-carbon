package cache

import (
	"slices"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestInputReplayKeepsLiveBehindHistoryAndRetries(t *testing.T) {
	c, history := New(), New()
	defer c.Stop()
	defer history.Stop()
	c.BeginInputReplay(history)
	c.Add(points.OnePoint("same", 30, 3))
	if c.WriteoutReady("same") {
		t.Fatal("live input eligible before history is decoded")
	}
	abort := make(chan bool)
	defer close(abort)
	next := make(chan string, 1)
	go func() { next <- c.WriteoutQueue().Get(abort) }()
	history.AddRecovered(points.OnePoint("same", 10, 1))
	select {
	case name := <-next:
		if name != "same" {
			t.Fatal(name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("history queue stalled")
	}
	old, ok := c.PopNotConfirmed("same")
	if !ok || old.Data[0].Value != 10 {
		t.Fatalf("popped live input: %v", old)
	}
	history.AddRecovered(points.OnePoint("same", 20, 2))
	c.Requeue(old)
	got := c.Get("same")
	want := []points.Point{{Value: 10, Timestamp: 1}, {Value: 20, Timestamp: 2}, {Value: 30, Timestamp: 3}}
	if !slices.Equal(got, want) {
		t.Fatalf("read order %v, want %v", got, want)
	}
	old, ok = c.PopNotConfirmed("same")
	if !ok || !slices.Equal(old.Data, want[:2]) {
		t.Fatalf("retry order %v", old)
	}
	c.Confirm(old)
	if !history.IsEmpty() || c.WriteoutReady("same") {
		t.Fatal("live frontier passed before the reader finished")
	}
	go func() { next <- c.WriteoutQueue().Get(abort) }()
	select {
	case name := <-next:
		t.Fatalf("live metric %q escaped the frontier", name)
	case <-time.After(20 * time.Millisecond):
	}
	c.FinishInputReplay()
	select {
	case name := <-next:
		if name != "same" {
			t.Fatal(name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live queue did not resume")
	}
	live, ok := c.PopNotConfirmed("same")
	if !ok || !slices.Equal(live.Data, want[2:]) {
		t.Fatalf("live %v", live)
	}
	c.Confirm(live)
}
