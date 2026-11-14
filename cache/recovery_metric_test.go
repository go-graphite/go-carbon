package cache

import (
	"github.com/go-graphite/go-carbon/points"
	"testing"
)

func TestRecoveryMetricSurvivesEvictionAndNormalization(t *testing.T) {
	c := New()
	c.SetMaxSize(0)
	raw := []byte("owned.metric")
	m := c.ResolveRecoveryMetric(raw)
	if m.OverheadBytes() != int64(len(raw)+1024) {
		t.Fatal(m)
	}
	raw[0] = 'X'
	var stream RecoveryStream
	source := stream.Next()
	c.AddRecoveryMetric(m, points.OnePoint(m.Name(), 1, 2), source, false)
	existing := c.ResolveRecoveryMetric([]byte(m.Name()))
	if existing.OverheadBytes() != 0 {
		t.Fatal("active metric charged as new")
	}
	p, ok := c.PopNotConfirmed(m.Name())
	if !ok {
		t.Fatal("missing")
	}
	c.Confirm(p)
	c.AddRecoveryMetric(existing, points.OnePoint(existing.Name(), 3, 2), source, false)
	if got := c.Get("owned.metric"); len(got) != 1 || got[0].Value != 3 || source.Pending() != 1 {
		t.Fatal(got, source.Pending())
	}
	c.SetTagsEnabled(true)
	tagged := c.ResolveRecoveryMetric([]byte("tagged;z=2;a=1"))
	c.AddRecoveryMetric(tagged, points.OnePoint(tagged.Name(), 4, 5), source, true)
	if got := c.Get("tagged;a=1;z=2"); len(got) != 1 || got[0].Value != 4 {
		t.Fatal(got)
	}
}
