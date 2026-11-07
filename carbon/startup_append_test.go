//go:build unix

package carbon

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestStartupCaptureQueuedRecords(t *testing.T) {
	s, _ := captureFixture(t)
	const writers, records = 32, 80
	values := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x3ff0000000000001}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Go(func() {
			<-start
			for seq := range records {
				name := fmt.Sprintf("tiny.writer.%02d", writer)
				raw := points.OnePoint(name, math.Float64frombits(values[seq%len(values)]), int64(seq)-40).AppendBinary(nil)
				if err := s.shards[0].Write(raw, 1); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	s.receiver.Stop()
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	if err := visitCapture(s, func(p *points.Points) {
		for _, d := range p.Data {
			seq := seen[p.Metric]
			if seq >= records || math.Float64bits(d.Value) != values[seq%len(values)] || d.Timestamp != int64(seq)-40 {
				t.Errorf("record changed: %q sequence %d, %+v", p.Metric, seq, d)
			}
			seen[p.Metric]++
		}
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != writers {
		t.Fatal("lost metric", len(seen))
	}
	for name, count := range seen {
		if count != records {
			t.Fatal("lost or duplicated point", name, count)
		}
	}
}

// A burst keeps queued requests pending so an escaping held request cannot be
// hidden by the sequential, uncontended path through appendLoop.
func TestStartupCaptureSubmissionAllocations(t *testing.T) {
	s, _ := captureFixture(t)
	raw := points.OnePoint("allocation.metric", 123.456, 1794015000).AppendBinary(nil)
	for _, shard := range s.shards {
		if err := shard.Write(raw, 1); err != nil {
			t.Fatal(err)
		}
	}
	const burst = 64
	done := make(chan error, burst)
	var failure error
	allocs := testing.AllocsPerRun(100, func() {
		for range burst {
			s.shards[0].SubmitCapture(raw, 1, done)
		}
		for range burst {
			failure = <-done
		}
	}) / burst
	if failure != nil {
		t.Fatal(failure)
	}
	t.Logf("allocations per captured submission: %.3f", allocs)
	if allocs != 0 {
		t.Fatalf("hot capture allocates per submission: %.3f", allocs)
	}
}

// A caught-up stream must retain capture's isolation from cache/GC stalls.
// Both the blocked frame and the following correction must be acknowledged
// while the cache is blocked, then apply in their original same-timestamp FIFO.
func TestStartupCaptureLiveApplyCannotBlockAppend(t *testing.T) {
	s, _ := captureFixture(t)
	core := cache.New()
	admission := &testAdmission{}
	admission.allowed.Store(true)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	add := func(p *points.Points, source *cache.RecoverySource) {
		if p.Data[0].Value == 2 {
			close(entered)
			<-release
		}
		core.AddRecoveredFrom(p, source)
	}
	s.startReplay(&startupSink{admission: admission, add: add, liveAdd: add, canRetire: func() bool { return false }}, func() error { return nil })
	sh := s.shards[0]
	if err := sh.Write(points.OnePoint("ordered.live", 1, 1).AppendBinary(nil), 1); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return sh.live.Load() })
	done := make(chan error, 2)
	finishedWrite := make(chan struct{})
	go func() {
		defer close(finishedWrite)
		done <- sh.Write(points.OnePoint("ordered.live", 2, 1).AppendBinary(nil), 1)
		done <- sh.Write(points.OnePoint("ordered.live", 3, 1).AppendBinary(nil), 1)
	}()
	defer func() { unblock(); <-finishedWrite }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cache apply did not start")
	}
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("cache application blocked durable input capture")
		}
	}
	unblock()
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == 3 })
	got := core.Get("ordered.live")
	if len(got) != 3 {
		t.Fatal(got)
	}
	for i, point := range got {
		if point.Value != float64(i+1) || point.Timestamp != 1 {
			t.Fatal("correction FIFO changed", got)
		}
	}
}
