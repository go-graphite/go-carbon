package tags

import (
	"testing"
	"time"
)

func TestStatUsesWorkerLagSnapshot(t *testing.T) {
	q := &Queue{} // No database: Stat must only read the worker's published sample.
	q.lagNanos.Store(int64(3 * time.Second))
	got := -1.0
	(&Tags{q: q}).Stat(func(k string, v float64) {
		if k == "queueLag" {
			got = v
		}
	})
	if got != 3 {
		t.Fatal(got)
	}
}
