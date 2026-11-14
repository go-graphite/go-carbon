package cache

import (
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestWriteoutLivePointsExcludeRestorationAndRequeue(t *testing.T) {
	c := New()
	c.AddRestored(points.OnePoint("restored", 1, 1))
	c.AddRecovered(points.OnePoint("recovered", 2, 2))
	if got := c.WriteoutState().LivePoints; got != 0 {
		t.Fatal("saved points counted as live", got)
	}
	c.Add(points.OnePoint("live", 3, 3))
	c.AddPoint([]byte("live"), 4, 4)
	c.AddPoints([]byte("live"), []points.Point{{Value: 5, Timestamp: 5}, {Value: 6, Timestamp: 6}})
	var ref PointRef
	c.AddPointRef(&ref, []byte("live"), 7, 7)
	c.AddPointRef(&ref, []byte("live"), 8, 8)
	c.AddPointBatch([][]byte{[]byte("live")}, []points.Point{{Value: 9, Timestamp: 9}})
	if got := c.WriteoutState().LivePoints; got != 7 {
		t.Fatalf("accepted points=%d want7", got)
	}
	p, ok := c.Pop("live")
	if !ok {
		t.Fatal("missing batch")
	}
	c.Requeue(p)
	if got := c.WriteoutState().LivePoints; got != 7 {
		t.Fatalf("requeue counted twice: %d", got)
	}
}

// The added arrival accounting must leave values (including NaN payloads and
// signed zero), timestamps, and arrival order identical across ingestion paths.
func TestLivePointAccountingKeepsPointBits(t *testing.T) {
	data := []points.Point{{Value: math.Float64frombits(0x8000000000000000), Timestamp: 9},
		{Value: math.Float64frombits(0x7ff8000000000042), Timestamp: 2},
		{Value: math.Inf(1), Timestamp: 2}, {Value: 1.25, Timestamp: -7}}
	for _, mode := range []string{"add", "point", "batch", "ref", "stream"} {
		t.Run(mode, func(t *testing.T) {
			c := New()
			name := []byte("bits.metric")
			var ref PointRef
			var slot any
			if mode == "batch" {
				c.AddPoints(name, data)
			} else {
				for _, point := range data {
					switch mode {
					case "add":
						c.Add(points.OnePoint(string(name), point.Value, point.Timestamp))
					case "point":
						c.AddPoint(name, point.Value, point.Timestamp)
					case "ref":
						c.AddPointRef(&ref, name, point.Value, point.Timestamp)
					case "stream":
						c.AddStreamPoint(name, point.Value, point.Timestamp, &slot)
					}
				}
			}
			got := c.Get(string(name))
			if len(got) != len(data) || c.WriteoutState().LivePoints != uint64(len(data)) {
				t.Fatalf("points/accounting: %v", got)
			}
			for i, p := range got {
				if p.Timestamp != data[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(data[i].Value) {
					t.Fatalf("%d changed: value bits=%x timestamp=%d", i, math.Float64bits(p.Value), p.Timestamp)
				}
			}
		})
	}
}
