package carbon

import (
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestCaptureMetricBytesUsesActiveMap(t *testing.T) {
	c := cache.New()
	c.AddRecovered(points.OnePoint("active.metric", 1, 1))
	var raw []byte
	for _, name := range []string{"active.metric", "new.metric", "active.metric", "new.metric"} {
		raw = points.OnePoint(name, 1, 1).AppendBinary(raw)
	}
	cost, err := captureMetricBytes(raw, c)
	if err != nil || cost != int64(len("new.metric")+1024) {
		t.Fatal(cost, err)
	}
	// In-flight points are deliberately treated as absent by named recovery;
	// capture must use the same conservative rule, without scanning in-flight.
	p, ok := c.PopNotConfirmed("active.metric")
	if !ok {
		t.Fatal("missing active batch")
	}
	cost, err = captureMetricBytes(raw, c)
	if err != nil || cost != int64(len("active.metric")+len("new.metric")+2048) {
		t.Fatal(cost, err)
	}
	c.Requeue(p)
	cost, err = captureMetricBytes(raw, c)
	if err != nil || cost != int64(len("new.metric")+1024) {
		t.Fatal(cost, err)
	}
	if _, err := captureMetricBytes(raw[:len(raw)-1], c); err == nil {
		t.Fatal("truncated frame accepted")
	}
}
