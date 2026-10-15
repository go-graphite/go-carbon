package cache

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// The old exact-size admission rule is the oracle while cache contents and
// limits change. Approximate-size publication must not change the decision.
func TestRecoveryCapacityMatchesExactAdmission(t *testing.T) {
	c := New()
	rng := rand.New(rand.NewSource(1))
	for step := range 128 {
		metric := fmt.Sprintf("capacity.%d", rng.Intn(8))
		c.Pop(metric)
		c.AddRecovered(&points.Points{Metric: metric, Data: make([]points.Point, rng.Intn(5000)+1)})
		size := c.Size()
		for _, limit := range []int64{0, 16384, 65536, 800000000} {
			c.SetMaxSize(uint64(limit))
			for _, count := range []int{1, 1024, 1000000} {
				want := limit <= 0 || size == 0 || size+int64(count) <= limit/2
				if got := c.HasRecoveryCapacity(count); got != want {
					t.Fatalf("step %d, size %d, limit %d, count %d: got %v, want %v", step, size, limit, count, got, want)
				}
			}
		}
	}
}

func TestRecoveryCapacityDoesNotBlockOnUnrelatedShard(t *testing.T) {
	c := New()
	c.SetMaxSize(800000000)
	c.data[0].mu.Lock()
	done := make(chan bool, 1)
	go func() { done <- c.HasRecoveryCapacity(1) }()
	select {
	case room := <-done:
		c.data[0].mu.Unlock()
		if !room {
			t.Fatal("ample recovery space rejected")
		}
	case <-time.After(time.Second):
		c.data[0].mu.Unlock()
		<-done
		t.Fatal("per-record recovery admission waited for an unrelated shard")
	}
}

func TestRecoveryCapacityPreservesPressureBoundary(t *testing.T) {
	c := New()
	c.SetMaxSize(262144)
	p := &points.Points{Metric: "capacity", Data: make([]points.Point, 131071)}
	c.AddRecovered(p)
	if !c.HasRecoveryCapacity(1) {
		t.Fatal("last point below half capacity rejected")
	}
	if c.HasRecoveryCapacity(2) {
		t.Fatal("admitted points above half capacity")
	}
	c.Pop("capacity")
	if !c.HasRecoveryCapacity(1000000) {
		t.Fatal("empty cache cannot recover oversized record")
	}
	c.SetMaxSize(0)
	if !c.HasRecoveryCapacity(1000000) {
		t.Fatal("unlimited cache rejected recovery")
	}
}
