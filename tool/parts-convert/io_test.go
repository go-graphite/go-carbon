//go:build linux || darwin

package main

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestPacedFSSharedBudgetAndExactBytes(t *testing.T) {
	fs := newPacedFS(vfs.NewMem(), 1_000_000)
	clock := time.Unix(1, 0)
	fs.pace.now = func() time.Time { return clock }
	fs.pace.sleep = func(d time.Duration) { clock = clock.Add(d) }
	input := bytes.Repeat([]byte{0, 255, 7, 19}, 100_000)
	expected := bytes.Clone(input)
	f, err := fs.Create("data")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write(input); n != len(input) || err != nil {
		t.Fatal(n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f, err = fs.Open("data")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := make([]byte, len(input))
	if n, err := f.ReadAt(out, 0); n != len(out) || err != nil {
		t.Fatal(n, err)
	}
	if !bytes.Equal(expected, out) {
		t.Fatal("data bits changed")
	}
	if got := clock.Sub(time.Unix(1, 0)); got != 800*time.Millisecond {
		t.Fatal("shared read/write budget", got)
	}
	if _, err := f.ReadAt(make([]byte, 4), int64(len(input)-2)); err != io.EOF {
		t.Fatal("lost EOF", err)
	}
	if f.Fd() != vfs.InvalidFd {
		t.Fatal("exposed unmetered descriptor")
	}
}

func TestPacerConcurrentReservationsAndNoIdleBurst(t *testing.T) {
	p := newPacedFS(vfs.NewMem(), 1_000_000).pace
	clock := time.Unix(1, 0)
	p.now = func() time.Time { return clock }
	p.sleep = func(d time.Duration) { clock = clock.Add(d) }
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); p.charge(100_000) }()
	}
	wg.Wait()
	if clock.Sub(time.Unix(1, 0)) != 800*time.Millisecond {
		t.Fatal(clock)
	}
	clock = clock.Add(time.Hour)
	before := clock
	p.charge(100_000)
	if clock.Sub(before) != 100*time.Millisecond {
		t.Fatal("idle time accumulated a burst")
	}
}

func TestPacerRetainsPaidTimerOversleep(t *testing.T) {
	const rate = 24_000_000
	p := newPacedFS(vfs.NewMem(), rate).pace
	start := time.Unix(1, 0)
	clock := start
	sleeps := 0
	p.now = func() time.Time { return clock }
	p.sleep = func(d time.Duration) {
		sleeps++
		clock = clock.Add((d + time.Millisecond - 1) / time.Millisecond * time.Millisecond)
	}
	for i := 1; i <= 10_000; i++ {
		p.charge(256)
		minimum := time.Duration((int64(i)*256*int64(time.Second) + rate - 1) / rate)
		if clock.Sub(start) < minimum {
			t.Fatalf("byte cap exceeded at read %d: %v < %v", i, clock.Sub(start), minimum)
		}
	}
	if clock.Sub(start) > 110*time.Millisecond || sleeps > 110 {
		t.Fatalf("timer floor charged per read: elapsed %v, sleeps %d", clock.Sub(start), sleeps)
	}
	clock = clock.Add(time.Hour)
	before := clock
	p.charge(24_000)
	if clock.Sub(before) != time.Millisecond {
		t.Fatal("idle time retained pacing credit", clock.Sub(before))
	}
}
