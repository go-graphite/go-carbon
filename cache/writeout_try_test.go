package cache

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestWriteoutTryGetDoesNotRebuild(t *testing.T) {
	c := New()
	q := c.WriteoutQueue()
	c.Add(points.OnePoint("queued.metric", 1, 1790000000))
	if q.TryGet() != "" {
		t.Fatal("nonblocking read rebuilt the queue")
	}
	q.update()
	if got := q.TryGet(); got != "queued.metric" {
		t.Fatalf("queued name %q", got)
	}
	if q.TryGet() != "" {
		t.Fatal("drained queue returned work")
	}
	if _, ok := c.PopNotConfirmed("queued.metric"); !ok {
		t.Fatal("queue read popped the points")
	}
}
