package cache

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestLexicalQueueRetainsEligibilityAndPerMetricPointOrder(t *testing.T) {
	c := batchingCache(t, 3, time.Hour)
	if err := c.SetWriteStrategy("lexical"); err != nil {
		t.Fatal(err)
	}
	want := make([]string, 1024)
	values := []float64{math.Copysign(0, -1), 1.25, math.Float64frombits(0x7ff8000000000042)}
	for i := len(want) - 1; i >= 0; i-- {
		name := fmt.Sprintf("locality.%04d", i)
		want[i] = name
		for _, v := range values {
			c.Add(points.OnePoint(name, v, 1234))
		}
	}
	c.Add(points.OnePoint("deferred", 7, 1234))
	q, deadline := c.makeQueueAt(time.Now())
	if len(q) != len(want) || deadline.IsZero() {
		t.Fatal("lexical ordering changed eligibility")
	}
	sort.Strings(want)
	for _, name := range want {
		requireMetric(t, q, name)
		p, ok := c.PopNotConfirmed(name)
		if !ok || len(p.Data) != len(values) {
			t.Fatal("eligible metric disappeared")
		}
		for i, v := range values {
			if p.Data[i].Timestamp != 1234 || math.Float64bits(p.Data[i].Value) != math.Float64bits(v) {
				t.Fatal("point order or float bits changed")
			}
		}
		if got := c.Get(name); len(got) != len(values) {
			t.Fatal("in-flight points became unreadable")
		}
		c.Confirm(p)
	}
	if got := c.Get("deferred"); len(got) != 1 || got[0].Value != 7 {
		t.Fatal("deferred point changed")
	}
	q, _ = c.makeQueueAt(deadline)
	requireMetric(t, q, "deferred")
}
