package cache

import (
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// Names over points.MaxMetricNameBytes are refused at intake on every add
// path and counted; a name at the limit is cached.
func TestCacheRefusesNamesOverLimit(t *testing.T) {
	c := New()
	fits := strings.Repeat("a", points.MaxMetricNameBytes)
	over := fits + "a"
	c.Add(points.OnePoint(over, 1, 10))
	c.AddPoint([]byte(over), 2, 11)
	c.AddPoints([]byte(over), []points.Point{{Value: 3, Timestamp: 12}, {Value: 4, Timestamp: 13}})
	c.AddPointBatch([][]byte{[]byte(over)}, []points.Point{{Value: 5, Timestamp: 14}})
	if c.Size() != 0 || c.Len() != 0 {
		t.Fatalf("over-limit name cached: size=%d len=%d", c.Size(), c.Len())
	}
	if got := c.NameTooLongRejectedPoints(); got != 5 {
		t.Fatalf("rejected points = %d, want 5", got)
	}
	c.AddPoint([]byte(fits), 6, 15)
	if c.Size() != 1 || c.NameTooLongRejectedPoints() != 5 {
		t.Fatalf("name at the limit: size=%d rejected=%d", c.Size(), c.NameTooLongRejectedPoints())
	}
}

// Names that fit discovery must survive both live intake and dump restore.
func TestCacheKeepsIndexableLongNames(t *testing.T) {
	for _, n := range []int{4097, 5038, 5042, 8187} {
		for _, restored := range []bool{false, true} {
			c := New()
			name := strings.Repeat("a", n)
			p := points.OnePoint(name, 42, 100)
			if restored {
				c.AddRestored(p)
			} else {
				c.Add(p)
			}
			got := c.Get(name)
			if len(got) != 1 || got[0] != p.Data[0] || c.NameTooLongRejectedPoints() != 0 {
				t.Fatalf("bytes=%d restored=%v: got=%v rejected=%d", n, restored, got, c.NameTooLongRejectedPoints())
			}
		}
	}
}
