package cache

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// The oracle visits the actual allocations, independently of the counters.
// Each shard is checked under its lock, so it also works during live writes.
func checkPointBytes(t *testing.T, c *Cache, quiescent bool) {
	t.Helper()
	var total int64
	for i, sh := range c.data {
		sh.mu.RLock()
		var want int64
		for _, it := range sh.items.all {
			want += int64(cap(it.compact.buf))
		}
		got := sh.items.pointBytes
		sh.mu.RUnlock()
		if got != want {
			t.Fatalf("shard %d point bytes %d, want %d", i, got, want)
		}
		total += want
	}
	if got := c.PointBytes(); quiescent && got != total || got < 0 {
		t.Fatalf("point bytes %d, recomputed %d (quiescent %v)", got, total, quiescent)
	}
}

// Exercise both numeric and raw/XOR encoding, single and batched appends,
// removals, decode at pop and re-encoding/merging after failed writes.
func pointBytesOperations(c *Cache, seed uint64, steps int, check func()) {
	r := rand.New(rand.NewPCG(seed, 19))
	for step := range steps {
		name := fmt.Sprintf("point.bytes.%d", r.IntN(128))
		data := make([]points.Point, 1+r.IntN(48))
		for i := range data {
			value := float64(step + i)
			if step%2 != 0 {
				value = math.Float64frombits(r.Uint64())
			}
			data[i] = points.Point{Value: value, Timestamp: int64(step*100 + i)}
		}
		switch r.IntN(8) {
		case 0, 1:
			c.AddPoint([]byte(name), data[0].Value, data[0].Timestamp)
		case 2:
			c.Add(&points.Points{Metric: name, Data: data})
		case 3:
			c.AddPoints([]byte(name), data)
		case 4:
			c.AddRecovered(&points.Points{Metric: name, Data: data})
		case 5:
			c.Pop(name)
		case 6, 7:
			if p, ok := c.PopNotConfirmed(name); ok {
				check() // popped storage must already be excluded
				if step%2 == 0 {
					c.AddPoints([]byte(name), data)
					c.Requeue(p) // replaces/compacts the queued item
				} else {
					c.Confirm(p)
				}
			}
		}
		check()
	}
}

func TestPointBytesRandomized(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	check := func() { checkPointBytes(t, c, true) }
	check()
	pointBytesOperations(c, 7, 2000, check)
	for i := range 128 {
		c.Pop(fmt.Sprintf("point.bytes.%d", i))
		check()
	}
	if got := c.PointBytes(); got != 0 {
		t.Fatalf("drained point bytes %d, want 0", got)
	}
}

func TestPointBytesConcurrent(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	var writers sync.WaitGroup
	for worker := range 4 {
		writers.Go(func() {
			pointBytesOperations(c, uint64(worker+1), 500, func() {})
		})
	}
	done := make(chan struct{})
	go func() { writers.Wait(); close(done) }()
	for {
		checkPointBytes(t, c, false)
		select {
		case <-done:
			checkPointBytes(t, c, true)
			return
		default:
		}
	}
}

func TestItemMapPointBytes(t *testing.T) {
	var m itemMap
	r := rand.New(rand.NewPCG(31, 41))
	check := func() {
		t.Helper()
		var want int64
		for _, it := range m.all {
			want += int64(cap(it.compact.buf))
		}
		if m.pointBytes != want {
			t.Fatalf("map point bytes %d, want %d", m.pointBytes, want)
		}
	}
	// Force collisions, growth, tombstones and same-capacity rehashes.
	for step := range 2000 {
		name := fmt.Sprintf("collision.%d", r.IntN(128))
		if step%3 == 0 {
			m.removeHashed(0, name)
		} else {
			it := newItem(name, r.IntN(4096))
			if old := m.getHashed(0, name); old != nil {
				// Re-encode into a fresh buffer, releasing excess capacity.
				it = newItem(name, 0)
				it.compact.appendPoints(old.compact.appendDecoded(nil))
			}
			it.compact.append(float64(step), int64(step))
			m.setHashed(0, it)
		}
		check()
	}
	for i := range 128 {
		m.removeHashed(0, fmt.Sprintf("collision.%d", i))
		check()
	}
}
