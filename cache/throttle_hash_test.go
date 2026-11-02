package cache

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

func TestThrottleHashUsesCanonicalNameAcrossIngestPaths(t *testing.T) {
	for _, mode := range []string{"add", "point", "points", "ref", "series", "stream-ref"} {
		for _, tagged := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/tags=%t", mode, tagged), func(t *testing.T) {
				c := New()
				c.SetTagsEnabled(tagged)
				name := "metric.one"
				canonical := name
				if tagged {
					name, canonical = "metric.one;z=last;a=first", "metric.one;a=first;z=last"
				}
				calls := 0
				c.SetThrottleWithHash(func(ps *points.Points, inCache bool, hash uint64, state *any) bool {
					calls++
					if ps.Metric != canonical || hash != xxh3.HashString(canonical) {
						t.Fatalf("wrong identity: %q/%x", ps.Metric, hash)
					}
					if inCache && *state != canonical {
						t.Fatal("hash callback lost cached state")
					}
					*state = canonical
					return ps.Data[0].Value < 0
				})
				var ref PointRef
				var slot any
				if mode == "series" {
					slot = c.ResolveStreamName([]byte(name))
				}
				add := func(value float64, ts int64) {
					switch mode {
					case "add":
						c.Add(points.OnePoint(name, value, ts))
					case "point":
						c.AddPoint([]byte(name), value, ts)
					case "points":
						c.AddPoints([]byte(name), []points.Point{{Value: value, Timestamp: ts}, {Value: value, Timestamp: ts + 1}})
					case "ref":
						c.AddPointRef(&ref, []byte(name), value, ts)
					default:
						c.AddStreamPoint([]byte(name), value, ts, &slot)
					}
				}
				for i, value := range []float64{1, 2, -1, 3} {
					add(value, int64(i*2))
				}
				want := []points.Point{{Value: 1, Timestamp: 0}, {Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 6}}
				wantCalls := 4
				if mode == "points" {
					want = []points.Point{{Value: 1, Timestamp: 0}, {Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}, {Value: 2, Timestamp: 3}, {Value: 3, Timestamp: 6}, {Value: 3, Timestamp: 7}}
					wantCalls = 8
				}
				if got := c.Get(canonical); !reflect.DeepEqual(got, want) {
					t.Fatalf("admitted points: %v, want %v", got, want)
				}
				if calls != wantCalls {
					t.Fatalf("unexpected admission calls: %d", calls)
				}
				c.Pop(canonical)
				add(4, 8) // A stale identity must refresh after item replacement.
				if got := c.Get(canonical); len(got) == 0 || got[0].Value != 4 {
					t.Fatal("stale identity lost the replacement point")
				}
			})
		}
	}
}

func TestThrottleSettersReplaceHashCallback(t *testing.T) {
	c := New()
	unexpected := func(*points.Points, bool, uint64, *any) bool { t.Fatal("replaced hash callback ran"); return false }
	c.SetThrottleWithHash(unexpected)
	c.SetThrottle(func(*points.Points, bool) bool { return true })
	c.AddPoint([]byte("a"), 1, 1)
	c.SetThrottleWithHash(unexpected)
	c.SetThrottleWithState(func(*points.Points, bool, *any) bool { return true })
	c.AddPoint([]byte("b"), 1, 1)
	if c.Size() != 0 {
		t.Fatal("legacy setter stopped enforcing admission")
	}
}
