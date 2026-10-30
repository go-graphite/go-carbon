package chunkstore

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestExpirationPressureSharesConcurrentSamples(t *testing.T) {
	c := expirationPressureCache{period: 2 * time.Second}
	var samples atomic.Int64
	sample := func() (int64, uint64) {
		samples.Add(1)
		return 4, 99
	}
	start := time.Now()
	for i, offset := range []time.Duration{0, time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second} {
		var workers sync.WaitGroup
		for range 64 {
			workers.Go(func() {
				l0, debt := c.load(start.Add(offset), sample)
				if l0 != 4 || debt != 99 {
					t.Errorf("pressure = %d, %d", l0, debt)
				}
			})
		}
		workers.Wait()
		if got, want := samples.Load(), int64(i/2+1); got != want {
			t.Fatalf("at %s: samples = %d, want %d", offset, got, want)
		}
	}
}

func TestExpirationPressureStallBypassesSampleLock(t *testing.T) {
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	// Populate a non-stalled sample, then model another worker stuck sampling.
	if _, _, stalled := s.ExpirationPressure(); stalled {
		t.Fatal("unexpected initial stall")
	}
	s.expirationPressure.mu.Lock()
	s.writeStallBegin(pebble.WriteStallBeginInfo{Reason: "test"})
	checked := make(chan bool, 1)
	go func() {
		_, _, stalled := s.ExpirationPressure()
		checked <- stalled
	}()
	select {
	case stalled := <-checked:
		s.expirationPressure.mu.Unlock()
		if !stalled {
			t.Fatal("cached sample hid an active stall")
		}
	case <-time.After(time.Second):
		s.expirationPressure.mu.Unlock()
		<-checked
		t.Fatal("active stall waited for metrics sampling")
	}
	s.writeStallEnd()
	if _, _, stalled := s.ExpirationPressure(); stalled {
		t.Fatal("stall end was cached")
	}
}
