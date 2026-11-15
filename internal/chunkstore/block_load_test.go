package chunkstore

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func TestBlockLoadConcurrency(t *testing.T) {
	for _, limit := range []int{0, 1, 64} {
		s, err := Open(t.TempDir(), Options{BlockLoadConcurrency: limit})
		if err != nil {
			t.Fatal(err)
		}
		gate := s.blockLoadSlots
		want := blockLoadConcurrency(limit)
		if got := s.Stats().BlockLoadCapacity; got != want {
			t.Fatalf("capacity %d, want %d", got, want)
		}
		if !gate.TryAcquire(want) || gate.TryAcquire(1) {
			t.Fatal("initial budget not enforced")
		}
		done := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		go func() { done <- gate.Acquire(ctx, 1) }()
		deadline := time.Now().Add(5 * time.Second)
		for gate.Stats().NumHadToWait == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if gate.Stats().NumHadToWait == 0 {
			cancel()
			t.Fatal("waiter did not queue")
		}
		if err := s.SetBlockLoadConcurrency(int(want) + 1); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("capacity increase did not wake queued load")
		}
		cancel()
		if s.blockLoadSlots != gate {
			t.Fatal("reload replaced admission gate")
		}
		if err := s.SetBlockLoadConcurrency(1); err != nil {
			t.Fatal(err)
		}
		if gate.TryAcquire(1) {
			t.Fatal("shrink admitted above budget")
		}
		gate.Release(want + 1)
		if !gate.TryAcquire(1) || gate.TryAcquire(1) {
			t.Fatal("shrunken budget not enforced")
		}
		gate.Release(1)
		if err := s.SetBlockLoadConcurrency(0); err != nil {
			t.Fatal(err)
		}
		if got := gate.Stats().Capacity; got != blockLoadConcurrency(0) {
			t.Fatalf("default capacity %d", got)
		}
		for _, bad := range []int{-1, 1025} {
			if s.SetBlockLoadConcurrency(bad) == nil {
				t.Fatal("invalid capacity accepted")
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []int{-1, 1025} {
		if _, err := Open(t.TempDir(), Options{BlockLoadConcurrency: bad}); err == nil {
			t.Fatal("invalid initial capacity accepted")
		}
	}
}

// GOMAXPROCS is process-wide, so these cases must remain sequential.
func TestBlockLoadAutomaticConcurrency(t *testing.T) {
	previous := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(previous)
	for _, tc := range []struct{ cpus, want int }{{1, 16}, {16, 16}, {32, 32}, {64, 64}, {192, 192}, {256, 256}, {512, 256}} {
		t.Run(fmt.Sprint(tc.cpus), func(t *testing.T) {
			runtime.GOMAXPROCS(tc.cpus)
			s, err := Open(t.TempDir(), Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if got := s.Stats().BlockLoadCapacity; got != int64(tc.want) {
				t.Fatalf("automatic capacity %d, want %d", got, tc.want)
			}
			gate := s.blockLoadSlots
			if err := s.SetBlockLoadConcurrency(7); err != nil {
				t.Fatal(err)
			}
			if got := s.BlockLoadConcurrency(); got != 7 {
				t.Fatalf("override capacity %d", got)
			}
			runtime.GOMAXPROCS(48)
			if got := s.BlockLoadConcurrency(); got != 7 {
				t.Fatalf("override changed with CPUs: %d", got)
			}
			if err := s.SetBlockLoadConcurrency(0); err != nil {
				t.Fatal(err)
			}
			if s.blockLoadSlots != gate || s.BlockLoadConcurrency() != 48 {
				t.Fatalf("automatic reload did not resize the existing gate: %d", s.BlockLoadConcurrency())
			}
		})
	}
}
