package cache

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestAddPointsMatchesIndividualAdmissionAndLogging(t *testing.T) {
	for _, limit := range []uint64{0, 1, 3, 20} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			old, batch := New(), New()
			var calls [2][]bool
			for i, c := range []*Cache{old, batch} {
				c.SetMaxSize(limit)
				c.EnableInputLog()
				c.SetThrottle(func(p *points.Points, inCache bool) bool {
					calls[i] = append(calls[i], inCache)
					return p.Data[0].Value < 0
				})
			}
			data := []points.Point{{Value: -1, Timestamp: 1}, {Value: 1, Timestamp: 2}, {Value: 2, Timestamp: 3}, {Value: -1, Timestamp: 4}, {Value: 3, Timestamp: 5}, {Value: 4, Timestamp: 6}}
			name := []byte("batch.metric")
			for _, p := range data {
				old.AddPoint(name, p.Value, p.Timestamp)
			}
			batch.AddPoints(name, data)
			copy(name, "XXXXXXXXXXXX")
			if !reflect.DeepEqual(old.Get("batch.metric"), batch.Get("batch.metric")) || old.Size() != batch.Size() || old.stat.overflowCnt != batch.stat.overflowCnt || !reflect.DeepEqual(calls[0], calls[1]) {
				t.Fatal("batch changed point admission or cache contents")
			}
			for shard := range shardCount {
				a, b := old.TakeLogEntries(shard, 1, nil), batch.TakeLogEntries(shard, 1, nil)
				if len(a) != len(b) {
					t.Fatal("input-log counts differ")
				}
				for i := range a {
					if a[i].Metric() != b[i].Metric() || a[i].Value != b[i].Value || a[i].Timestamp != b[i].Timestamp {
						t.Fatal("input-log entries differ")
					}
				}
			}
		})
	}
}

func TestAddPointsRechecksDiversionBetweenPoints(t *testing.T) {
	c := New()
	c.AddPoint([]byte("metric"), 0, 0)
	var out bytes.Buffer
	c.SetThrottle(func(*points.Points, bool) bool {
		c.DivertToXlog(&out)
		return false
	})
	c.AddPoints([]byte("metric"), []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 3}})
	if len(c.Get("metric")) != 2 || out.String() != "metric 2 2\nmetric 3 3\n" {
		t.Fatalf("diversion cached=%v output=%q", c.Get("metric"), out.String())
	}
}
