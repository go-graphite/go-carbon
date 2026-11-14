package cache

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// An endlessly replenished warm prefix must not replace the cold tail on every
// refresh. Point ownership stays in the cache; scheduling may only change which
// metric is popped first, including across the nonblocking batch-fill path.
func TestWriteoutRefreshDrainsColdTail(t *testing.T) {
	testWriteoutRefreshDrainsTail(t, false)
}

func TestWriteoutRefreshDrainsRecoveryTail(t *testing.T) {
	testWriteoutRefreshDrainsTail(t, true)
}

// Retained plans can repeat a name after the current plan has written it.
// Recovery and pressure disable batching, but must still reject absent names
// before the persister spends a mandatory writeout admission.
func TestWriteoutRefreshSkipsConsumedNames(t *testing.T) {
	for _, draining := range []bool{false, true} {
		for _, nonblocking := range []bool{false, true} {
			t.Run(fmt.Sprintf("draining=%v/nonblocking=%v", draining, nonblocking), func(t *testing.T) {
				c := New()
				defer c.Stop()
				c.SetRecoveryDrain(draining)
				if draining {
					if err := c.SetWriteoutBatching(960, time.Hour); err != nil {
						t.Fatal(err)
					}
				}
				c.SetRestartOrder(func() bool { return true })
				c.Add(points.OnePoint("a.consumed", 1, 1))
				c.Add(points.OnePoint("z.remaining", 2, 2))
				q := c.WriteoutQueue()
				q.update()
				q.update() // Both snapshots contain the two names.
				if _, ok := c.Pop("a.consumed"); !ok {
					t.Fatal("missing consumed fixture")
				}
				if c.WriteoutReady("a.consumed") {
					t.Fatal("absent name is writeout ready")
				}
				abort := make(chan bool)
				defer close(abort)
				var got string
				if nonblocking {
					got = q.TryGet()
				} else {
					got = q.Get(abort)
				}
				if got != "z.remaining" {
					t.Fatalf("spent admission on %q, want remaining name", got)
				}
			})
		}
	}
}

func testWriteoutRefreshDrainsTail(t *testing.T, draining bool) {
	t.Helper()
	for _, nonblocking := range []bool{false, true} {
		t.Run(fmt.Sprint(nonblocking), func(t *testing.T) {
			c := New()
			defer c.Stop()
			c.SetRecoveryDrain(draining)
			c.SetRestartOrder(func() bool { return true })
			c.SetRestartWarmFirst(func(n string) bool { return n == "a.warm" }, nil, nil)
			stream := &RecoveryStream{}
			source := stream.Next()
			originals := map[string][]points.Point{}
			for i := range 8 {
				name := fmt.Sprintf("z.cold.%d", i)
				data := []points.Point{{Value: math.Float64frombits(0x7ff8000000000001 + uint64(i)), Timestamp: int64(i + 1)}, {Value: math.Copysign(0, -1), Timestamp: int64(i + 1)}}
				originals[name] = data
				if draining {
					c.AddRecoveredFrom(&points.Points{Metric: name, Data: data}, source)
				} else {
					c.Add(&points.Points{Metric: name, Data: data})
				}
			}
			c.Add(points.OnePoint("a.warm", 1, 0))
			q := c.WriteoutQueue()
			q.update()
			abort := make(chan bool)
			defer close(abort)
			seen := map[string]bool{}
			for round := 0; round < 40 && len(seen) < len(originals); round++ {
				var name string
				if nonblocking {
					name = q.TryGet()
				} else {
					name = q.Get(abort)
				}
				p, ok := c.PopNotConfirmed(name)
				if !ok {
					continue
				}
				if want, ok := originals[name]; ok {
					if seen[name] {
						t.Fatalf("duplicate cold write %s", name)
					}
					seen[name] = true
					if len(p.Data) != len(want) {
						t.Fatalf("point count %s: %d", name, len(p.Data))
					}
					for i, v := range p.Data {
						if v.Timestamp != want[i].Timestamp || math.Float64bits(v.Value) != math.Float64bits(want[i].Value) {
							t.Fatalf("point bits changed for %s", name)
						}
					}
				}
				c.Confirm(p)
				if name == "a.warm" {
					c.Add(points.OnePoint("a.warm", 1, 0))
				}
				q.update()
			}
			if source.Pending() != 0 {
				t.Fatalf("recovery source still pins %d batches", source.Pending())
			}
			if len(seen) != len(originals) {
				t.Fatalf("refresh starved cold points: wrote %d of %d cold metrics", len(seen), len(originals))
			}
		})
	}
}
