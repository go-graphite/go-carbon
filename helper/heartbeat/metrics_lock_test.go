package heartbeat

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRecordedDoesNotWaitForHeartbeatIO(t *testing.T) {
	c := &clock{t: time.Unix(1700000000, 0)}
	path := filepath.Join(t.TempDir(), "heartbeat.json")
	first := start(t, path, c)
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}
	h := start(t, path, c)
	defer h.Stop()
	h.mu.Lock() // held by beat across write, fsync and rename
	done := make(chan *Window, 1)
	go func() { done <- h.Recorded() }()
	select {
	case got := <-done:
		h.mu.Unlock()
		if got == nil || !got.LastAlive.Equal(c.now()) {
			t.Fatal(got)
		}
		got.LastAlive = time.Time{}
		if h.Recorded().LastAlive.IsZero() {
			t.Fatal("snapshot is mutable through caller")
		}
	case <-time.After(time.Second):
		h.mu.Unlock()
		<-done
		t.Fatal("metrics waited for heartbeat filesystem I/O")
	}
}
