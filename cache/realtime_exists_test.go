package cache

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestRealtimeIndexSkipsIndexedMetricsAndRetriesUnknown(t *testing.T) {
	c := New()
	c.SetBloomSize(1000)
	ch := make(chan string, 1)
	c.SetNewMetricsChan(ch)
	ready := false
	checks := 0
	c.SetMetricExists(func(metric string) bool { checks++; return ready && metric == "already.indexed" })
	ch <- "queue.full"
	c.Add(points.OnePoint("already.indexed", 1, 1))
	if c.stat.droppedRealtimeIndex != 1 {
		t.Fatal("an unavailable index must not suppress a retry")
	}
	ready = true
	c.Add(points.OnePoint("already.indexed", 2, 2))
	before := checks
	c.Add(points.OnePoint("already.indexed", 3, 3))
	if checks != before || len(ch) != 1 || c.stat.droppedRealtimeIndex != 1 {
		t.Fatal("indexed metrics should warm the bloom filter without filling the queue")
	}
	c.Add(points.OnePoint("new.metric", 1, 1))
	<-ch
	c.Add(points.OnePoint("new.metric", 2, 2))
	if got := <-ch; got != "new.metric" {
		t.Fatalf("unknown metric was not retried: %s", got)
	}
	if len(c.Get("already.indexed")) != 3 || len(c.Get("new.metric")) != 2 {
		t.Fatal("notification suppression lost datapoints")
	}
}
