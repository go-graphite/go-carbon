package whisper

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
)

// TestOverlapShapeMix measures which overlap rule realistic emitters trigger.
// It is a measurement, not a regression test: run it with OVERLAP_SIM=1 -v.
func TestOverlapShapeMix(t *testing.T) {
	if os.Getenv("OVERLAP_SIM") == "" {
		t.Skip("set OVERLAP_SIM=1 to run the overlap shape simulation")
	}
	days := 1
	schemas := []string{"1m:14d,30m:2y", "1s:2d,1m:28d,1h:2y", "10s:2d,1m:14d,30m:2y", "5m:1y", "1h:10y"}
	emitters := []struct {
		name string
		e    emitterSim
	}{
		{"1s", emitterSim{cadence: 1}},
		{"10s", emitterSim{cadence: 10}},
		{"10s-jitter2", emitterSim{cadence: 10, jitter: 2}},
		{"60s", emitterSim{cadence: 60}},
		{"60s-jitter5", emitterSim{cadence: 60, jitter: 5}},
		{"60s-dup1%", emitterSim{cadence: 60, dupRate: .01}},
		{"60s-late1%", emitterSim{cadence: 60, lateRate: .01}},
		{"10s-dup1%-late0.1%", emitterSim{cadence: 10, dupRate: .01, lateRate: .001}},
		{"300s", emitterSim{cadence: 300}},
		{"10s-skew+2", emitterSim{cadence: 10, skew: 2}},
		{"60s-skew+2", emitterSim{cadence: 60, skew: 2}},
		{"60s-skew+30", emitterSim{cadence: 60, skew: 30}},
		{"60s-skew-30", emitterSim{cadence: 60, skew: -30}},
	}
	allow := []struct {
		name string
		p    float64
	}{{"compact=always", 1}}
	for _, schema := range schemas {
		for _, em := range emitters {
			for _, al := range allow {
				name := fmt.Sprintf("%s/%s/%s", schema, em.name, al.name)
				rng := rand.New(rand.NewSource(1))
				e := em.e
				e.minPoints, e.maxDelay = 960, 960
				f := newShapeFile(t, schema, Average, 0)
				p := al.p
				f.compact = persisterCompaction(t, func() bool { return rng.Float64() < p })
				start := f.now
				f.prefill(schema, start)
				for k := range overlapUpdates {
					overlapUpdates[k].Store(0)
				}
				flushAt, batches := e.batches(e.arrivals(rng, start, days*86400))
				var kinds [overlapKinds]int
				divertBatches := 0
				for i, b := range batches {
					r := f.write(flushAt[i], b)
					kinds[r.kind]++
					if r.diverted > 0 {
						divertBatches++
					}
				}
				var parts []string
				for k := OverlapKind(1); k < overlapKinds; k++ {
					if kinds[k] > 0 {
						parts = append(parts, fmt.Sprintf("%s=%d", k, kinds[k]))
					}
				}
				t.Logf("%-55s batches=%5d diverting=%5d overlap: %s", name, len(batches), divertBatches, strings.Join(parts, " "))
			}
		}
	}
}
