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
