package chunkstore

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

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
