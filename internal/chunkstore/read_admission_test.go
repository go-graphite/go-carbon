package chunkstore

import (
	"context"
	"testing"
	"time"
)

func TestReadConcurrency(t *testing.T) {
	for _, limit := range []int{0, 1, 64} {
		s, err := Open(t.TempDir(), Options{ReadConcurrency: limit})
		if err != nil {
			t.Fatal(err)
		}
		gate := s.readSlots
		want := readConcurrency(limit)
		if got := s.Stats().ReadCapacity; got != want {
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
		if err := s.SetReadConcurrency(int(want) + 1); err != nil {
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
		if s.readSlots != gate {
			t.Fatal("reload replaced admission gate")
		}
		if err := s.SetReadConcurrency(1); err != nil {
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
		if err := s.SetReadConcurrency(0); err != nil {
			t.Fatal(err)
		}
		if got := gate.Stats().Capacity; got != 32 {
			t.Fatalf("default capacity %d", got)
		}
		for _, bad := range []int{-1, 1025} {
			if s.SetReadConcurrency(bad) == nil {
				t.Fatal("invalid capacity accepted")
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []int{-1, 1025} {
		if _, err := Open(t.TempDir(), Options{ReadConcurrency: bad}); err == nil {
			t.Fatal("invalid initial capacity accepted")
		}
	}
}

// Exercise the same admission paths as renders and digest/fetch requests,
// including reuse, cancellation and shrink while readers still own their slots.
func TestReadAdmissionResizeAndQueue(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{ReadConcurrency: 1})
	defer s.Close()
	m := createTestMetric(t, s, "admission.metric")
	ctx, release, err := s.AdmitRead(context.Background(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := s.Metadata(ctx, m.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Fetch(ctx, m.Name, now-60, now); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Metadata(waitCtx, m.Name); done <- err }()
	deadline := time.Now().Add(5 * time.Second)
	for s.Stats().ReadsQueued != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if st := s.Stats(); st.ReadsQueued != 1 || st.ReadsInProgress != 1 {
		cancel()
		t.Fatalf("queue stats %+v", st)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled read succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled reader stuck")
	}
	if s.Stats().ReadsQueued != 0 {
		t.Fatal("canceled queue leaked")
	}
	if err := s.SetReadConcurrency(2); err != nil {
		t.Fatal(err)
	}
	_, release2, err := s.AdmitRead(context.Background(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	if s.Stats().ReadsInProgress != 2 {
		t.Fatal("active gauge")
	}
	if err := s.SetReadConcurrency(1); err != nil {
		t.Fatal(err)
	}
	release()
	release()
	release2()
	if st := s.Stats(); st.ReadsInProgress != 0 || st.ReadsQueued != 0 {
		t.Fatalf("leaked %+v", st)
	}
}
