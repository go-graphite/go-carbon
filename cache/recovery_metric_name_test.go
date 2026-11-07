package cache

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestRecoveryMetricNameOwnsBorrowedBytes(t *testing.T) {
	c := New()
	raw := []byte("recovery.owned.name")
	name := c.RecoveryMetricName(raw)
	clear(raw)
	if name != "recovery.owned.name" {
		t.Fatal(name)
	}
	c.AddRecovered(points.OnePoint(name, 1, 2))
	raw = []byte(name)
	var got string
	if allocs := testing.AllocsPerRun(100, func() { got = c.RecoveryMetricName(raw) }); allocs != 0 {
		t.Fatal("existing name allocated", allocs)
	}
	p, ok := c.PopNotConfirmed(name)
	if !ok {
		t.Fatal("missing metric")
	}
	c.Confirm(p)
	clear(raw)
	if got != name {
		t.Fatal("popped name no longer owned", got)
	}
}
