package carbon

import (
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestV2WriteTelemetryDeduplicatesPublication(t *testing.T) {
	s := newV2WriteTelemetry(2, 2)
	b := []v2core.Batch{{ID: 1, Order: 2, Clock: time.Now().Unix(), Points: make([]v2core.Row, 3)}, {ID: 1, Order: 1, Clock: time.Now().Unix(), Points: make([]v2core.Row, 2)}}
	s.visible(0, b)
	s.visible(0, b)
	s.visible(1, b)
	if n := s.visibleLatency.report()["count"]; n != uint64(10) {
		t.Fatal(n)
	}
	s.observeStage("seal_total", time.Second)
	if n := s.seal.report()["count"]; n != uint64(1) {
		t.Fatal(n)
	}
}

func TestV2WriteTelemetryReportsOverflow(t *testing.T) {
	var h v2WriteHistogram
	h.add(3*time.Minute, 100)
	r := h.report()
	if r["p99_seconds_upper"] != nil || r["overflow"] != uint64(100) {
		t.Fatalf("overflow has no finite upper bound: %v", r)
	}
}
