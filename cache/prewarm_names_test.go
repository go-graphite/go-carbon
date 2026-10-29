package cache

import (
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestPrewarmNameSequences(t *testing.T) {
	c := New()
	if names := slices.Collect(c.PendingNames()); len(names) != 0 {
		t.Fatal("pending names without a pending recovery", names)
	}
	if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
		t.Fatal(err)
	}
	// Claimed or not, every saved metric is named once.
	c.Add(points.OnePoint("metric", 9, 10))
	c.Add(points.OnePoint("live.only", 1, 10))
	pending := slices.Collect(c.PendingNames())
	slices.Sort(pending)
	if !slices.Equal(pending, []string{"metric", "other"}) {
		t.Fatal("pending names", pending)
	}
	// Consumers may stop early.
	for range c.PendingNames() {
		break
	}
	cached := slices.Collect(c.MetricNames())
	slices.Sort(cached)
	if !slices.Equal(cached, []string{"live.only", "metric"}) {
		t.Fatal("cached names", cached)
	}
	// The consumer holds no shard lock: it may write to the cache.
	for name := range c.MetricNames() {
		c.Add(points.OnePoint(name+".more", 1, 10))
		break
	}
}
