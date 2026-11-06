package persister

import (
	"testing"
	"time"
)

func TestMetricsDoNotWaitForReloadOrConversionIO(t *testing.T) {
	p := &Whisper{}
	p.autoUpdate.mu.Lock()
	p.ioMu.Lock()
	done := make(chan bool, 1)
	go func() {
		p.autoUpdateObservation()
		_, ok := p.MigrationMetrics()
		done <- ok
	}()
	select {
	case ok := <-done:
		p.ioMu.Unlock()
		p.autoUpdate.mu.Unlock()
		if ok {
			t.Fatal("busy conversion I/O snapshot accepted")
		}
	case <-time.After(time.Second):
		p.ioMu.Unlock()
		p.autoUpdate.mu.Unlock()
		<-done
		t.Fatal("metrics waited for lifecycle locks")
	}
}
