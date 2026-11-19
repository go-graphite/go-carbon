package v2core

import (
	"testing"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestRollupMetricTerminal(t *testing.T) {
	if got := unsafe.Sizeof(rollupMetric{}); got != 24 {
		t.Fatalf("metric bytes=%d, want 24", got)
	}
	for _, ladder := range []string{"1s:1d", "1s:1d,1m:30d,1h:1y"} {
		_, raw := rollupTestPolicy(t, "terminal.metric", ladder, sealrollup.Last, 0)
		r := newCoreRollups()
		m, err := r.metric(1, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := int(m.terminal), len(m.schema().Archives)-1; got != want {
			t.Fatalf("terminal=%d, want %d", got, want)
		}
		m.keepRecent(3601)
		m.installRecent(sealrollup.Delta{Archive: 0, Slot: 3601, Sample: sealrollup.Sample{Time: 3601, Bits: 42}})
		if m.terminal == 0 {
			if m.slots != nil {
				t.Fatal("terminal archive retained residency")
			}
		} else if _, ok := m.slots.View(0, 3601); !ok {
			t.Fatal("source sample missing")
		}
	}
}
