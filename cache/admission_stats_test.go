package cache

import (
	"errors"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestAdmissionStatsThrottleAcrossInputPaths(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	c.AddPoint([]byte("cached"), 1, 1)
	var ref PointRef
	var slot any = c.ResolveStreamName([]byte("cached"))
	c.SetThrottle(func(*points.Points, bool) bool { return true })
	c.Add(&points.Points{Metric: "new", Data: make([]points.Point, 3)})
	c.AddPoint([]byte("cached"), 2, 2)
	c.AddPoints([]byte("cached"), make([]points.Point, 4))
	c.AddPointRef(&ref, []byte("cached"), 3, 3)
	c.AddStreamPoint([]byte("cached"), 4, 4, &slot)
	if got := c.AdmissionStats(); got != (AdmissionStats{Throttled: 10}) {
		t.Fatalf("stats %+v, want 10 throttled points", got)
	}
	// Graphite collection must not reset the cumulative accounting.
	c.Stat(func(string, float64) {})
	c.Stat(func(string, float64) {})
	c.AddRecovered(points.OnePoint("recovered", 1, 1))
	if got := c.AdmissionStats(); got.Throttled != 10 || c.Size() != 2 {
		t.Fatalf("stats %+v, cache %d; recovery must bypass live rejection", got, c.Size())
	}
}

func TestAdmissionStatsCountsPointsInRejectedBatches(t *testing.T) {
	t.Run("tags", func(t *testing.T) {
		c := New()
		c.SetTagsEnabled(true)
		c.Add(&points.Points{Metric: "metric;broken", Data: make([]points.Point, 7)})
		if got := c.AdmissionStats(); got != (AdmissionStats{InvalidTags: 7}) || c.Size() != 0 {
			t.Fatalf("stats %+v, cache %d", got, c.Size())
		}
	})
	t.Run("overflow", func(t *testing.T) {
		c := New()
		c.SetMaxSize(1)
		c.Add(&points.Points{Metric: "cached", Data: make([]points.Point, 2)})
		c.Add(&points.Points{Metric: "new", Data: make([]points.Point, 7)})
		c.AddPoint([]byte("cached"), 3, 3)
		c.Stat(func(string, float64) {})
		if got := c.AdmissionStats(); got != (AdmissionStats{Overflow: 8}) || c.Size() != 2 {
			t.Fatalf("stats %+v, cache %d", got, c.Size())
		}
	})
	t.Run("diversion", func(t *testing.T) {
		c := New()
		s := *c.settings.Load().(*cacheSettings)
		s.xlog = func(*points.Points) error { return errors.New("write failed") }
		c.settings.Store(&s)
		c.Add(&points.Points{Metric: "metric", Data: make([]points.Point, 7)})
		s.xlog = func(*points.Points) error { return nil }
		c.settings.Store(&s)
		c.Add(points.OnePoint("metric", 1, 1))
		if got := c.AdmissionStats(); got != (AdmissionStats{XlogWriteFailed: 7}) || c.Size() != 0 {
			t.Fatalf("stats %+v, cache %d", got, c.Size())
		}
	})
}
