package cache

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestRecoveryMetricBytesAvoidsBorrowedNameAllocation(t *testing.T) {
	c := New()
	name := "recovery.admission.borrowed.long.metric.name.existing"
	c.AddRecovered(points.OnePoint(name, 1, 1))
	for _, raw := range [][]byte{[]byte(name), []byte(name + ".missing")} {
		want := int64(len(raw)) + 1024
		if string(raw) == name {
			want = 0
		}
		var got int64
		allocs := testing.AllocsPerRun(100, func() { got = c.RecoveryMetricBytes(raw) })
		if got != want || allocs != 0 {
			t.Fatalf("name %q: cost %d want %d, allocations %g", raw, got, want, allocs)
		}
	}
}
