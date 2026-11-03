package cache

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

// Capacity experiment, not a production trace. The captured point/cycle rate
// supplies the production window size; these two burst shapes bracket reference
// amortization on the same mixed-value generator as the gorilla workload.
func TestStagingWindowCapacity(t *testing.T) {
	if os.Getenv("CACHE_SLIM_STAGING") == "" {
		t.Skip("set CACHE_SLIM_STAGING=1")
	}
	const metrics = 100000
	data := compactWorkload("mix", 100)
	for _, staged := range []int{1, 10} {
		c := New()
		c.SetMaxSize(0)
		c.EnableInputLog()
		for i := 0; i < metrics; i++ {
			name := fmt.Sprintf("namespace.component.metric.%08d", i)
			c.AddPoints([]byte(name), data[i%len(data)][100-staged:])
		}
		if c.Size() != int64(metrics*staged) || c.LogDropped() != 0 {
			t.Fatal("staging window lost points")
		}
		var capacity int64
		for _, sh := range c.data {
			capacity += sh.log.stagingBytes()
		}
		t.Logf("metrics=%d staged-per-active-metric=%d capacity=%d capacity-B/staged-point=%.6f", metrics, staged, capacity, float64(capacity)/float64(metrics*staged))
		runtime.KeepAlive(c)
	}
}

// A rejected bulk call discards its replacement state, while a rejected warm
// single point keeps the state update. Preserve the original API distinction.
func TestBulkThrottleStateCommitOnAcceptance(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	var seen []int
	c.SetThrottleWithState(func(p *points.Points, _ bool, state *any) bool {
		n := 0
		if *state != nil {
			n = (*state).(int)
		}
		seen = append(seen, n)
		*state = n + len(p.Data)
		return p.Data[0].Value < 0
	})
	name := []byte("bulk.state.contract")
	positive := []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}}
	negative := []points.Point{{Value: -1, Timestamp: 3}, {Value: -2, Timestamp: 4}}
	c.Add(&points.Points{Metric: string(name), Data: positive})
	c.Add(&points.Points{Metric: string(name), Data: negative})
	c.Add(&points.Points{Metric: string(name), Data: positive})
	c.AddPoint(name, -1, 5)
	c.AddPoint(name, 3, 6)
	if !reflect.DeepEqual(seen, []int{0, 2, 2, 4, 5}) || len(c.Get(string(name))) != 5 {
		t.Fatalf("callback state/data changed: %v", seen)
	}
}

// The ingest fast path only refreshes byte accounting on this signal. Exercise
// new reference runs, every scalar field width, and reuse after an empty drain.
func TestStagingGrowthMemoryAccounting(t *testing.T) {
	var batch inputlog.Batch
	refs := []*inputlog.StreamRef{inputlog.NewStreamRef("a"), inputlog.NewStreamRef("b")}
	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < 10000; i++ {
			before := batch.Bytes()
			grew := batch.Append(refs[(i/10)%2], float64(i), int64(uint64(i)*0xd6e8feb86659fd93))
			if grew != (batch.Bytes() != before) {
				t.Fatalf("growth accounting cycle=%d point=%d before=%d after=%d signal=%v", cycle, i, before, batch.Bytes(), grew)
			}
		}
		batch.Recycle()
	}
}
