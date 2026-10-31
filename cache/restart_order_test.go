package cache

import (
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestRestartOrderUsesPointAgeAndResumesStrategy(t *testing.T) {
	c := New()
	defer c.Stop()
	if err := c.SetWriteStrategy("lexical"); err != nil {
		t.Fatal(err)
	}
	c.Add(points.OnePoint("z-old", 1, 100))
	c.Add(points.OnePoint("a-new", 1, 200))
	for name, arrival := range map[string]time.Time{"z-old": time.Unix(2, 0), "a-new": time.Unix(1, 0)} {
		shard := c.GetShard(name)
		shard.mu.Lock()
		shard.items.get(name).firstArrival = arrival
		shard.mu.Unlock()
	}
	active := true
	c.SetRestartOrder(func() bool { return active })
	q := c.makeQueue()
	if first := <-q; first != "z-old" {
		t.Fatal(first)
	}
	active = false
	q = c.makeQueue()
	if first := <-q; first != "a-new" {
		t.Fatal(first)
	}
}
