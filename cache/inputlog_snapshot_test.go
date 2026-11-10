package cache

import (
	"slices"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func TestInputLogSnapshotBypassesStagingBound(t *testing.T) {
	c := New()
	defer c.Stop()
	dir := t.TempDir()
	log, _ := startInputLog(t, dir, c, inputlog.Options{SegmentSize: 1 << 20, TruncateLag: -1})
	defer log.Close()
	want := make([]points.Point, 2*maxShardLogEntries+1)
	for i := range want {
		want[i] = points.Point{Value: float64(i) + .125, Timestamp: int64(i)}
	}
	c.Add(&points.Points{Metric: "large.metric", Data: want})
	c.Add(points.OnePoint("flight", 3, 4))
	if _, ok := c.PopNotConfirmed("flight"); !ok {
		t.Fatal("missing in-flight batch")
	}
	if c.LogDropped() == 0 {
		t.Fatal("fixture did not exceed ordinary staging")
	}
	if err := log.Relog(); err != nil {
		t.Fatal(err)
	}
	// Later live input must follow the snapshot and remain independently logged.
	c.Add(points.OnePoint("large.metric", 999, 1))
	// Repeated cycles with no truncate grace must retain the snapshot until
	// queued and in-flight batches are actually persisted.
	for range 3 {
		syncLog(t, log)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := inputlog.Open(dir, inputlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got := make(map[string][]points.Point)
	var mu sync.Mutex
	stats, err := reader.Replay(func(p *points.Points) {
		mu.Lock()
		defer mu.Unlock()
		got[p.Metric] = append(got[p.Metric], p.Data...)
	})
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, points.Point{Value: 999, Timestamp: 1})
	if !slices.Equal(got["large.metric"], want) || !slices.Equal(got["flight"], []points.Point{{Value: 3, Timestamp: 4}}) {
		t.Fatalf("snapshot contents changed: large=%d flight=%v", len(got["large.metric"]), got["flight"])
	}
	if stats.Points != int64(len(want)+1) || stats.Corrupt != 0 {
		t.Fatalf("snapshot replayed old duplicates or corrupt frames: %+v", stats)
	}
}

func TestInputLogSnapshotEmpty(t *testing.T) {
	c := New()
	defer c.Stop()
	dir := t.TempDir()
	log, _ := startInputLog(t, dir, c, inputlog.Options{SegmentSize: 1 << 20, TruncateLag: -1})
	if err := log.Relog(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := inputlog.Open(dir, inputlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	stats, err := reader.Replay(func(*points.Points) { t.Error("empty cache replayed a point") })
	if err != nil || stats.Points != 0 || stats.Corrupt != 0 {
		t.Fatalf("empty snapshot: %+v %v", stats, err)
	}
}
