package chunkstore

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestWriteStallsRemainLiveWhileOptionalTelemetryIsBusy(t *testing.T) {
	s, err := Open("store", Options{fs: vfs.NewMem(), CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.StopBackground()
	s.writeStallBegin(pebble.WriteStallBeginInfo{Reason: "test"})
	s.stallMu.Lock()
	done := make(chan uint64, 1)
	go func() { done <- s.WriteStalls() }()
	select {
	case count := <-done:
		s.stallMu.Unlock()
		if count != 1 {
			t.Fatalf("live stall count %d, want1", count)
		}
	case <-time.After(time.Second):
		s.stallMu.Unlock()
		<-done
		t.Fatal("essential stall count waited for optional telemetry")
	}
	s.writeStallEnd()
	s.writeStallBegin(pebble.WriteStallBeginInfo{Reason: "second"})
	s.writeStallEnd()
	if s.WriteStalls() != 2 || s.Stats().WriteStalls != 2 {
		t.Fatal("live and detailed counters disagree")
	}
}

func TestTelemetryNeverWaitsForControlLocks(t *testing.T) {
	s, err := Open("store", Options{fs: vfs.NewMem(), CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.StopBackground()
	c := s.compactionControl
	for _, mu := range []*sync.Mutex{&c.mu, &c.pacer.mu, &c.flushPacer.mu, &s.stallMu, &s.coldPack.mu, &s.relayout.guard.mu, &s.relayout.reclaim.mu, &s.sidecars.mu} {
		mu.Lock()
		done := make(chan bool, 1)
		go func() { _, _, ok := s.TelemetryStats(); done <- ok }()
		select {
		case ok := <-done:
			mu.Unlock()
			if ok {
				t.Fatal("busy control snapshot accepted")
			}
		case <-time.After(time.Second):
			mu.Unlock()
			<-done
			t.Fatal("telemetry waited for control mutex")
		}
	}
	if _, _, ok := s.TelemetryStats(); !ok {
		t.Fatal("partial try-lock acquisition leaked locks")
	}
}

func TestTelemetryDoesNotEnterPebble(t *testing.T) {
	s, err := Open("store", Options{fs: vfs.NewMem(), CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// A direct Pebble call after Close panics. The published background sample
	// must remain independently readable; no database/cache access is needed.
	if _, _, ok := s.TelemetryStats(); !ok {
		t.Fatal("missing cached backend sample")
	}
}
