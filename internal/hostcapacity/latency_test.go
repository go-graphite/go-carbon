package hostcapacity

import (
	"testing"
	"time"
)

func TestLatencyUsesIntervalNotLifetimeP99(t *testing.T) {
	var h Latency
	for range 10000 {
		h.Observe(time.Microsecond)
	}
	previous := h.Snapshot()
	for range 100 {
		h.Observe(20 * time.Millisecond)
	}
	p99, count := h.Snapshot().P99Since(previous)
	if count != 100 || p99 < 20*time.Millisecond || p99 > 40*time.Millisecond {
		t.Fatal(p99, count)
	}
	if p99, count := previous.P99Since(h.Snapshot()); p99 != 0 || count != 0 {
		t.Fatal("reset underflow", p99, count)
	}
}

func TestMemoryReserveBounds(t *testing.T) {
	if remaining(2<<30, 1<<30) != (2<<30)-(1<<30)-(2<<30)/20 {
		t.Fatal("reserve")
	}
	if remaining(128<<20, 100<<20) != 0 {
		t.Fatal("small limit omitted minimum reserve")
	}
}
