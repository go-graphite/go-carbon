package v2read

import (
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Request stages timed by FetchMany and Metadata. Per-owner stages are
// observed once per owner call, so their counts exceed the request count.
const (
	StagePin         = iota // pinning every owner publication
	StageViews              // wall time to build all owner views (owner selection and wait)
	StageSnapshot           // per owner: sealed part lookup for the routed names
	StagePending            // per owner: pending head lookup and exact-name filter
	StageLoadPending        // per owner: request-local pending metadata
	StageResolve            // per name: sealed read and merge into resolved samples
	StageRender             // per name: archive selection and slot merge
	StageConsume            // per name: callback, including serialization
	numStages
)

var stageNames = [numStages]string{"pin", "views", "snapshot", "pending", "load_pending", "resolve", "render", "consume"}

type stageCounter struct {
	count, nanoseconds atomic.Uint64
	buckets            [10]atomic.Uint64 // last is overflow
}

type stageCounters [numStages]stageCounter

func (s *stageCounters) observe(stage int, started time.Time) {
	d := time.Since(started)
	c := &s[stage]
	sec := d.Seconds()
	i := 0
	for ; i < 9; i++ {
		if sec <= stageBounds[i] {
			break
		}
	}
	c.buckets[i].Add(1)
	c.nanoseconds.Add(uint64(d))
	c.count.Add(1)
}

var stageBounds = [9]float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1}

// StageObservation is one stage's cumulative histogram, in seconds.
type StageObservation struct {
	Name    string
	Count   uint64
	Sum     float64
	Buckets map[float64]uint64
}

// StageObservations reports every request stage; an approximate concurrent view.
func (r *Reader) StageObservations() []StageObservation {
	out := make([]StageObservation, numStages)
	for i := range out {
		c := &r.stages[i]
		count := c.count.Load()
		b := make(map[float64]uint64, 9)
		var cum uint64
		for j, bound := range stageBounds {
			cum += c.buckets[j].Load()
			b[bound] = min(cum, count)
		}
		out[i] = StageObservation{Name: stageNames[i], Count: count, Sum: float64(c.nanoseconds.Load()) / 1e9, Buckets: b}
	}
	return out
}

// OwnerFor is the owner that intake, adoption and sealed routing give a name:
// the same function as the core's rollup routing. Names live on no other owner.
func OwnerFor(name string, owners int) int {
	return int(v2part.Route(v2part.ID(name))) % owners
}

// splitByOwner groups names by owning publication, preserving request order.
func splitByOwner(names []string, owners int) [][]string {
	out := make([][]string, owners)
	for _, n := range names {
		o := OwnerFor(n, owners)
		out[o] = append(out[o], n)
	}
	return out
}
