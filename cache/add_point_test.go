package cache

import (
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestAddPointMatchesAdd(t *testing.T) {
	viaAdd, viaPoint := New(), New()
	var throttled []string
	viaPoint.SetThrottle(func(ps *points.Points, inCache bool) bool {
		if len(ps.Data) != 1 {
			t.Fatalf("throttle saw %d points, want 1", len(ps.Data))
		}
		throttled = append(throttled, ps.Metric)
		return ps.Data[0].Value < 0
	})

	name := []byte("metric.one")
	for i, value := range []float64{1, 2, -1, 3} {
		viaAdd.Add(points.OnePoint("metric.one", value, int64(i)))
		viaPoint.AddPoint(name, value, int64(i))
		copy(name, "XXXXXXXXXX") // the caller reuses its buffer
		copy(name, "metric.one")
	}

	want := []points.Point{{Value: 1, Timestamp: 0}, {Value: 2, Timestamp: 1}, {Value: 3, Timestamp: 3}}
	if got := viaPoint.Get("metric.one"); !reflect.DeepEqual(got, want) {
		t.Fatalf("AddPoint stored %v, want %v", got, want)
	}
	if got := viaAdd.Get("metric.one"); len(got) != 4 {
		t.Fatalf("Add stored %v", got)
	}
	if viaPoint.Size() != 3 || viaPoint.Len() != 1 {
		t.Fatalf("size %d, metrics %d", viaPoint.Size(), viaPoint.Len())
	}
	if !reflect.DeepEqual(throttled, []string{"metric.one", "metric.one", "metric.one", "metric.one"}) {
		t.Fatalf("throttled %q", throttled)
	}

	// The stored name must not alias the caller's buffer.
	copy(name, "XXXXXXXXXX")
	if p, ok := viaPoint.Pop("metric.one"); !ok || p.Metric != "metric.one" {
		t.Fatalf("popped %+v", p)
	}
}

func TestAddPointHonorsDiversion(t *testing.T) {
	c := New()
	c.AddPoint([]byte("cached"), 1, 1)
	var diverted []*points.Points
	c.DivertToPointWriter(func(p *points.Points) error {
		diverted = append(diverted, p)
		return nil
	})
	name := []byte("cached")
	c.AddPoint(name, 2, 2)
	copy(name, "XXXXXX")
	if len(diverted) != 1 || diverted[0].Metric != "cached" || diverted[0].Data[0].Value != 2 {
		t.Fatalf("diverted %+v", diverted)
	}
	if got := c.Get("cached"); len(got) != 1 {
		t.Fatalf("diverted point reached the cache: %v", got)
	}
}

func BenchmarkAddPointCached(b *testing.B) {
	c := New()
	c.SetMaxSize(0)
	name := []byte("benchmark.cached.metric")
	c.AddPoint(name, 0, 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.AddPoint(name, float64(i), int64(i))
		if i%1024 == 1023 {
			c.Pop(string(name))
			c.AddPoint(name, 0, 0)
		}
	}
}

func TestAddPointRetainsThrottleState(t *testing.T) {
	c := New()
	calls := 0
	c.SetThrottleWithState(func(ps *points.Points, inCache bool, state *any) bool {
		calls++
		if inCache && *state != ps.Metric {
			t.Fatalf("state for %s was not retained: %v", ps.Metric, *state)
		}
		*state = ps.Metric
		return ps.Data[0].Value < 0
	})
	for _, name := range []string{"a.one", "b.two"} {
		c.AddPoint([]byte(name), 1, 1)
		c.AddPoint([]byte(name), -1, 2)
		c.Add(points.OnePoint(name, 2, 3))
		c.AddPoint([]byte(name), 3, 4)
		if len(c.Get(name)) != 3 {
			t.Fatal("stateful hook changed rejected points")
		}
		c.Pop(name)
		c.AddPoint([]byte(name), 4, 5)
	}
	if calls != 10 {
		t.Fatalf("got %d hook calls", calls)
	}
}
