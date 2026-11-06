package restartcpu

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsDoNotWaitForBudgetOrSampleDrain(t *testing.T) {
	b := NewWithCPU(1, func() float64 { return 0 })
	b.until = time.Now().Add(-time.Hour)
	b.warmed.Store(true)
	drain := func() bool { t.Error("metrics ran drain callback"); return false }
	b.drained.Store(&drain)
	r := prometheus.NewPedanticRegistry()
	b.InitPrometheus(r)
	b.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := r.Gather(); done <- err }()
	select {
	case err := <-done:
		b.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		b.mu.Unlock()
		<-done
		t.Fatal("metrics waited for admission mutex")
	}
	if _, err := r.Gather(); err != nil {
		t.Fatal(err)
	}
}
