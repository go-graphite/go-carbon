package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestFiniteFenceExcludesConcurrentIntakeThroughPublication(t *testing.T) {
	c := New()
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- c.FenceEmptyMetric(context.Background(), "finite", func() error { close(entered); <-release; return nil })
	}()
	<-entered
	added := make(chan struct{})
	go func() { c.Add(points.OnePoint("finite", 1, 2)); close(added) }()
	select {
	case <-added:
		t.Fatal("intake passed publication fence")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	<-added
	if !c.Has("finite") {
		t.Fatal("fence lost concurrent intake")
	}
	called := false
	if err := c.FenceEmptyMetric(context.Background(), "finite", func() error { called = true; return nil }); err == nil || called {
		t.Fatal("published over queued intake")
	}
}

func TestFiniteFenceRefusesRecoveryAndUnboundedInflightCensus(t *testing.T) {
	c := New()
	recovery := &pendingRecovery{}
	recovery.remaining.Store(1)
	c.pending.Store(recovery)
	called := false
	if err := c.FenceEmptyMetric(context.Background(), "finite", func() error { called = true; return nil }); err == nil || called {
		t.Fatal("active recovery was treated as empty")
	}
	c.pending.Store(nil)
	shard := c.GetShard("finite")
	shard.notConfirmedUsed = 257
	if err := c.FenceEmptyMetric(context.Background(), "finite", func() error { called = true; return nil }); err == nil || called {
		t.Fatal("unbounded in-flight scan admitted")
	}
	shard.notConfirmedUsed = 0
}
