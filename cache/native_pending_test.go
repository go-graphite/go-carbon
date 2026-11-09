package cache

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestNativePendingNamesQueuedInflightAndRecovery(t *testing.T) {
	c := New()
	if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
		t.Fatal(err)
	}
	c.Add(points.OnePoint("queued", 1, 1))
	c.Add(points.OnePoint("flight", 1, 1))
	flight, ok := c.PopNotConfirmed("flight")
	if !ok {
		t.Fatal("no in-flight points")
	}
	names, err := c.NativePendingNames()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"metric", "other", "queued", "flight"} {
		if _, ok := names[name]; !ok {
			t.Fatal("missed pending name", name, names)
		}
	}
	c.Confirm(flight)
	c.AddPoint([]byte("metric"), 4, 4) // claims immutable recovery into the shard before its next write
	metric, ok := c.PopNotConfirmed("metric")
	if !ok {
		t.Fatal("no recovery claim")
	}
	c.Confirm(metric)
	names, err = c.NativePendingNames()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := names["flight"]; ok {
		t.Fatal("confirmed name remains excluded")
	}
	if _, ok := names["metric"]; ok {
		t.Fatal("claimed confirmed recovery name remains excluded")
	}
}
