package malloc

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestTrimmerTrimsUntilStopped(t *testing.T) {
	synctest.Test(t, testTrimmerTrimsUntilStopped)
}

func testTrimmerTrimsUntilStopped(t *testing.T) {
	var calls atomic.Int64
	tr := start(time.Millisecond, func() { calls.Add(1) }, nil)
	synctest.Wait()
	time.Sleep(3 * time.Millisecond)
	synctest.Wait()
	tr.Stop()
	got := calls.Load()
	if got < 3 || tr.trims.Load() != uint64(got) {
		t.Fatalf("calls %d, trims %d", got, tr.trims.Load())
	}
	time.Sleep(5 * time.Millisecond)
	if calls.Load() != got {
		t.Fatal("trimmer ran after Stop")
	}
}

func TestStartOffOrUnsupported(t *testing.T) {
	if Start(0, true) != nil {
		t.Fatal("a zero interval started a trimmer")
	}
	if !Supported() && Start(time.Second, true) != nil {
		t.Fatal("an unsupported build started a trimmer")
	}
	var nilTrimmer *Trimmer
	nilTrimmer.Stop()
}

func gather(t *testing.T, c *Collector) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			if g := m.GetGauge(); g != nil {
				out[f.GetName()] = g.GetValue()
			} else {
				out[f.GetName()] = m.GetCounter().GetValue()
			}
		}
	}
	return out
}

func TestCollectorReportsTrimsAndLastStats(t *testing.T) {
	c := NewCollector()
	if got := gather(t, c); len(got) != 0 {
		t.Fatalf("without a trimmer: %v", got)
	}
	tr := start(time.Hour, func() {}, nil)
	defer tr.Stop()
	tr.trims.Store(2)
	tr.nanos.Store(uint64(1500 * time.Millisecond))
	c.SetTrimmer(tr)
	got := gather(t, c)
	if len(got) != 2 || got["malloc_trims_total"] != 2 || got["malloc_trim_seconds_total"] != 1.5 {
		t.Fatalf("trimmer without stats: %v", got)
	}
	tr.last.Store(&Stats{System: 300, InUse: 100, Free: 200})
	got = gather(t, c)
	if len(got) != 5 || got["malloc_system_bytes"] != 300 || got["malloc_inuse_bytes"] != 100 || got["malloc_free_bytes"] != 200 {
		t.Fatalf("trimmer with stats: %v", got)
	}
}

func TestStatsReadAfterTrimNotOnScrape(t *testing.T) {
	synctest.Test(t, testStatsReadAfterTrimNotOnScrape)
}

func testStatsReadAfterTrimNotOnScrape(t *testing.T) {
	var reads atomic.Int64
	tr := start(time.Millisecond, func() {}, func() (Stats, bool) { reads.Add(1); return Stats{InUse: 7}, true })
	synctest.Wait()
	time.Sleep(time.Millisecond)
	synctest.Wait()
	tr.Stop()
	before := reads.Load()
	c := NewCollector()
	c.SetTrimmer(tr)
	for range 3 {
		gather(t, c)
	}
	if reads.Load() != before || before == 0 {
		t.Fatalf("stats reads: %d before scrapes, %d after", before, reads.Load())
	}
}
