package chunkstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"sync"
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
		if got := gate.Stats().Capacity; got != readConcurrency(0) {
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

func TestAutomaticReadConcurrency(t *testing.T) {
	old := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(old)
	for _, tc := range []struct{ procs, want int }{{1, 32}, {8, 32}, {16, 64}, {32, 128}, {192, 768}, {512, 1024}} {
		runtime.GOMAXPROCS(tc.procs)
		if got := readConcurrency(0); got != int64(tc.want) {
			t.Fatalf("procs=%d: capacity=%d, want %d", tc.procs, got, tc.want)
		}
		if got := readConcurrency(7); got != 7 {
			t.Fatal("automatic sizing overrode explicit limit")
		}
	}
}

// Changing the shared gate must leave digest wire bytes and every fetched float
// bit unchanged, including sparse histories, signed zero and non-finite values.
func TestReadConcurrencyDifferential(t *testing.T) {
	s, now := openTestStore(t, t.TempDir(), Options{ReadConcurrency: 32})
	defer closeTestStore(t, s)
	ctx := context.Background()
	const names = 128
	for i := range names {
		name := fmt.Sprintf("differential.%03d", i)
		createTestMetric(t, s, name)
		values := []float64{math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, math.MaxFloat64, math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000000042), float64(i)}
		var points []Point
		for j, value := range values {
			points = append(points, Point{Timestamp: int64((now/60 - 2*j - 1) * 60), Value: value})
		}
		if err := s.UpdateManyForArchive(ctx, name, points, 60*256); err != nil {
			t.Fatal(err)
		}
	}
	read := func() [][]byte {
		out := make([][]byte, names)
		var wg sync.WaitGroup
		for i := range names {
			wg.Go(func() {
				name := fmt.Sprintf("differential.%03d", i)
				digest, err := s.SumDigest(ctx, name, 0, 0)
				if err != nil {
					t.Error(err)
					return
				}
				data, err := json.Marshal(digest)
				if err != nil {
					t.Error(err)
					return
				}
				series, err := s.Fetch(ctx, name, now-3600, now)
				if err != nil {
					t.Error(err)
					return
				}
				for _, value := range series.Values {
					data = binary.BigEndian.AppendUint64(data, math.Float64bits(value))
				}
				out[i] = data
			})
		}
		wg.Wait()
		return out
	}
	want := read()
	if err := s.SetReadConcurrency(0); err != nil {
		t.Fatal(err)
	}
	got := read()
	for i := range names {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("name %d changed digest or point bits", i)
		}
	}
}
