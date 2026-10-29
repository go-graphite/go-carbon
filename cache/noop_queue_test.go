package cache

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestNoopQueueAlternatesEligibleShardsWithoutStarvation(t *testing.T) {
	for _, sizes := range [][]int{{3, 3, 3, 3}, {1, 2, 5, 17}, {128}} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			c := New()
			groups := make(map[*Shard]int)
			wanted := make(map[string]bool)
			remaining := make(map[*Shard]int)
			filled := 0
			for i := 0; filled < len(sizes); i++ {
				name := fmt.Sprintf("production.metric_%d", i)
				shard := c.GetShard(name)
				group, found := groups[shard]
				if !found {
					if len(groups) == len(sizes) {
						continue
					}
					group = len(groups)
					groups[shard] = group
				}
				if remaining[shard] == sizes[group] {
					continue
				}
				c.Add(points.OnePoint(name, 1, 10))
				wanted[name] = true
				remaining[shard]++
				if remaining[shard] == sizes[group] {
					filled++
				}
			}
			q, _ := c.makeQueueAt(time.Now())
			if len(q) != len(wanted) {
				t.Fatalf("queue length %d want %d", len(q), len(wanted))
			}
			round := make(map[*Shard]bool)
			for shard := range remaining {
				round[shard] = true
			}
			for len(q) > 0 {
				metric := <-q
				shard := c.GetShard(metric)
				if !wanted[metric] || !round[shard] {
					t.Fatalf("duplicate, lost, or starved metric %q", metric)
				}
				delete(wanted, metric)
				delete(round, shard)
				remaining[shard]--
				if remaining[shard] == 0 {
					delete(remaining, shard)
				}
				if len(round) == 0 {
					for shard := range remaining {
						round[shard] = true
					}
				}
			}
			if len(wanted) != 0 || len(remaining) != 0 {
				t.Fatal("queue did not drain every shard")
			}
		})
	}
}

func TestNoopConcurrentPersistenceRetainsPerMetricPointOrder(t *testing.T) {
	c := New()
	c.SetMaxSize(1000000)
	const count = 10000
	values := []float64{.1, math.Float64frombits(1 << 63), 1e100, -17}
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("production.metric_%d", i)
		for j, value := range values {
			c.Add(points.OnePoint(name, value, int64(10+j%2)))
		}
	}
	q, _ := c.makeQueueAt(time.Now())
	close(q)
	var wg sync.WaitGroup
	var seen sync.Map
	for i := 0; i < 96; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for metric := range q {
				p, ok := c.PopNotConfirmed(metric)
				if !ok {
					t.Errorf("queued metric disappeared: %s", metric)
					continue
				}
				if _, duplicate := seen.LoadOrStore(metric, true); duplicate {
					t.Error("metric queued twice")
				}
				if len(p.Data) != len(values) {
					t.Error("point count changed")
				} else {
					for j, point := range p.Data {
						if point.Timestamp != int64(10+j%2) || math.Float64bits(point.Value) != math.Float64bits(values[j]) {
							t.Error("point order or bits changed")
						}
					}
				}
				c.Confirm(p)
			}
		}()
	}
	wg.Wait()
	total := 0
	seen.Range(func(_, _ any) bool { total++; return true })
	if total != count || c.Len() != 0 || c.Size() != 0 || c.NotConfirmedLength() != 0 {
		t.Fatal("persistence left missing, queued, or unconfirmed points", total, c.Len(), c.Size(), c.NotConfirmedLength())
	}
}
