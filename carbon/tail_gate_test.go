package carbon

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestTailGateHoldsUntilReplay(t *testing.T) {
	g := &tailGate{ready: make(chan struct{})}
	passed := make(chan bool, 1)
	go func() { passed <- g.pass() }()
	select {
	case <-passed:
		t.Fatal("live point passed before the tail replay finished")
	case <-time.After(20 * time.Millisecond):
	}
	g.ok.Store(true)
	close(g.ready)
	select {
	case ok := <-passed:
		if !ok {
			t.Fatal("live point dropped after a successful replay")
		}
	case <-time.After(time.Second):
		t.Fatal("live point still held after the replay finished")
	}
	if !g.pass() {
		t.Fatal("fast path rejected a point after a successful replay")
	}
}

func TestTailGateDropsAfterFailedReplay(t *testing.T) {
	g := &tailGate{ready: make(chan struct{})}
	close(g.ready) // failed startup closes without marking the replay done
	if g.pass() {
		t.Fatal("live point admitted after a failed tail replay")
	}
}

// BenchmarkTailGate compares the per-point gate after a successful replay with
// the blocking receive on the closed channel that it replaced.
func BenchmarkTailGate(b *testing.B) {
	b.Run("closed-channel", func(b *testing.B) {
		ready := make(chan struct{})
		close(ready)
		var ok atomic.Bool
		ok.Store(true)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				<-ready
				if !ok.Load() {
					b.Fatal("dropped")
				}
			}
		})
	})
	b.Run("gate", func(b *testing.B) {
		g := &tailGate{ready: make(chan struct{})}
		g.ok.Store(true)
		close(g.ready)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if !g.pass() {
					b.Fatal("dropped")
				}
			}
		})
	})
}
