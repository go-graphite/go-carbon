//go:build unix

package carbon

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// A stalled append owner must not park unrelated replay. Capture still drains
// every accepted request after the owner resumes.
func TestReplayProgressesDuringQueuedCapture(t *testing.T) {
	s, _ := captureFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	encoded, releaseEncoded := make(chan struct{}), make(chan struct{})
	unblockEncoded := sync.OnceFunc(func() { close(releaseEncoded) })
	defer unblockEncoded()
	var calls atomic.Int64
	s.observe = func(*points.Points) {
		switch calls.Add(1) {
		case 1:
			close(entered)
			<-release
		case 2:
			close(encoded)
			<-releaseEncoded
		}
	}
	done := make(chan error, 17)
	raw := points.OnePoint("capture.priority", 42, 7).AppendBinary(nil)
	s.shards[0].SubmitCapture(raw, 1, done)
	<-entered
	for range 16 {
		s.shards[0].SubmitCapture(raw, 1, done)
	}
	resumed := make(chan error, 1)
	go func() { resumed <- s.waitCapture() }()
	select {
	case err := <-resumed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued append stalled unrelated replay")
	}
	unblock()
	<-encoded
	if len(s.shards[0].requests) != 0 || s.shards[0].pending.Load() != 16 {
		t.Fatal("fixture did not hide pending writes outside the channel")
	}
	if err := s.waitCapture(); err != nil {
		t.Fatal(err)
	}
	unblockEncoded()
	for range 17 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	if s.points.Load() != 17 || s.shards[0].pending.Load() != 0 {
		t.Fatal("capture lost accepted points", s.points.Load())
	}
}

func TestReplayCaptureChecksCancellation(t *testing.T) {
	for _, replayOnly := range []bool{false, true} {
		s := &startupInput{stop: make(chan struct{}), replayStop: make(chan struct{}), shards: []*startupShard{{requests: make(chan startupAppend, 8)}}}
		for range 8 {
			s.shards[0].requests <- startupAppend{}
		}
		s.shards[0].pending.Store(8)
		if replayOnly {
			close(s.replayStop)
		} else {
			close(s.stop)
		}
		if err := s.waitCapture(); !errors.Is(err, errStartupReplayStopped) {
			t.Fatal("shutdown ignored", err)
		}
	}
}

func TestReplayCaptureYieldAllowsDiskDrain(t *testing.T) {
	s := &startupInput{stop: make(chan struct{}), replayStop: make(chan struct{}), shards: []*startupShard{{}}}
	s.shards[0].pending.Store(100)
	s.paused.Store(true)
	if err := s.waitCapture(); err != nil {
		t.Fatal("disk reserve must allow replay to retire sources", err)
	}
}
