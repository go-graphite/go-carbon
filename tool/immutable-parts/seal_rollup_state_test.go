package main

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestDensePortableMetricStates(t *testing.T) {
	var states rollupMetricStates
	ids := []uint64{math.MaxUint64, 0, 1 << 63, 7, 42}
	rng := rand.New(rand.NewPCG(73, 91))
	for range 1000 {
		ids = append(ids, rng.Uint64())
	}
	for _, id := range ids {
		s := &atomicRollupState{}
		s.Frontier.Order = id
		states.set(id, s)
	}
	if len(states.ids) != len(ids) || len(states.values) != len(ids) {
		t.Fatal("directory is not dense")
	}
	for _, id := range ids {
		if s := states.get(id); s == nil || s.Frontier.Order != id {
			t.Fatalf("portable ID %x changed", id)
		}
	}
	if states.get(9) != nil {
		t.Fatal("missing metric was present")
	}
	if n := testing.AllocsPerRun(100, func() {
		for _, id := range ids {
			states.set(id, states.get(id))
		}
	}); n != 0 {
		t.Fatalf("existing metric access allocates: %g", n)
	}
}
