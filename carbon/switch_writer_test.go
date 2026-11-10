package carbon

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestSwitchWriterJoinsEveryPreviousWriter(t *testing.T) {
	const writers = 64
	entered := make(chan struct{}, writers)
	release := make(chan struct{})
	var active, oldCalls, newCalls atomic.Int64
	s := &switchWriter{write: func(*points.Points) error {
		active.Add(1)
		entered <- struct{}{}
		<-release
		oldCalls.Add(1)
		active.Add(-1)
		return nil
	}}
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() { _ = s.Write(points.OnePoint(fmt.Sprintf("lane.%d", i), 1, 1)) })
	}
	for range writers {
		<-entered
	}
	done := make(chan struct{})
	go func() {
		s.Switch(func(*points.Points) error { newCalls.Add(1); return nil })
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("switch returned with previous writes in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-done
	if active.Load() != 0 {
		t.Fatal("previous writer is still active")
	}
	wg.Wait()
	for i := range writers {
		if err := s.Write(points.OnePoint(fmt.Sprintf("lane.%d", i), 2, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if oldCalls.Load() != writers || newCalls.Load() != writers {
		t.Fatalf("old=%d new=%d", oldCalls.Load(), newCalls.Load())
	}
}

func TestSwitchWriterConcurrentSwitchesKeepAllPoints(t *testing.T) {
	var calls atomic.Int64
	write := func(*points.Points) error { calls.Add(1); return nil }
	s := &switchWriter{write: write}
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			p := points.OnePoint(fmt.Sprintf("metric.%d", i), 1, 1)
			for range 1000 {
				if err := s.Write(p); err != nil {
					t.Error(err)
				}
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 20 {
				s.Switch(write)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 32_000 {
		t.Fatalf("points=%d", calls.Load())
	}
}

func BenchmarkSwitchWriter(b *testing.B) {
	for _, legacy := range []bool{true, false} {
		b.Run(fmt.Sprintf("global=%v", legacy), func(b *testing.B) {
			var global sync.RWMutex
			s := &switchWriter{write: func(*points.Points) error { return nil }}
			var next atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				worker := next.Add(1)
				var names [256]*points.Points
				for i := range names {
					names[i] = points.OnePoint(fmt.Sprintf("metric.%d.%d", worker, i), 1, 1)
				}
				i := 0
				for pb.Next() {
					if legacy {
						global.RLock()
						global.RUnlock()
					} else {
						_ = s.Write(names[i&255])
					}
					i++
				}
			})
		})
	}
}
