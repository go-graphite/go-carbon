package carbonserver

import (
	"runtime"
	"runtime/metrics"
	"testing"
)

func otherPauses() uint64 {
	sample := []metrics.Sample{{Name: "/sched/pauses/total/other:seconds"}}
	metrics.Read(sample)
	var n uint64
	for _, c := range sample[0].Value.Float64Histogram().Counts {
		n += c
	}
	return n
}

// Stat runs every metric interval, so it must not stop the world the way
// runtime.ReadMemStats does, yet report the same figures.
func TestStatDoesNotStopTheWorld(t *testing.T) {
	listener := NewCarbonserverListener(nil)
	listener.initializeCaches()
	got := map[string]float64{}
	before := otherPauses()
	for range 20 {
		listener.Stat(func(name string, value float64) { got[name] = value })
	}
	if pauses := otherPauses() - before; pauses >= 20 {
		t.Fatalf("20 Stat calls stopped the world %d times", pauses)
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	alloc, totalAlloc, numGC, pauseNS := runtimeStats()
	if numGC < uint64(m.NumGC) || pauseNS < m.PauseTotalNs || totalAlloc < m.TotalAlloc || alloc == 0 {
		t.Fatalf("runtime stats alloc=%d total=%d gc=%d pause=%d; MemStats alloc=%d total=%d gc=%d pause=%d",
			alloc, totalAlloc, numGC, pauseNS, m.Alloc, m.TotalAlloc, m.NumGC, m.PauseTotalNs)
	}
	for _, name := range []string{"alloc", "total_alloc", "num_gc", "pause_ns"} {
		if _, ok := got[name]; !ok {
			t.Errorf("Stat did not send %s", name)
		}
	}
}
