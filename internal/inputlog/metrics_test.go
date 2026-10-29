package inputlog

import (
	"fmt"
	"testing"
	"time"
)

// Metrics count frames, writes and syncs, time each stage of a cycle, track
// disk use and the oldest unpersisted cycle, and describe the replay.
func TestMetrics(t *testing.T) {
	dir := t.TempDir()
	src := newFakeSource()
	var cycles []time.Duration
	opts := Options{SegmentSize: 1 << 20, ObserveCycle: func(d time.Duration) { cycles = append(cycles, d) }}
	l := openStarted(t, dir, opts, src)
	const n = 1000
	for i := range n {
		src.add(fmt.Sprintf("metric.%d", i%50), float64(i), int64(i))
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	// Cycles only run on Sync in this fixture. Age the recorded cycle while
	// its goroutine is idle, then let the next cycle refresh the gauges.
	l.ticks[0].at = l.ticks[0].at.Add(-5 * time.Millisecond)
	if err := l.Sync(); err != nil { // an empty cycle: counted, not observed
		t.Fatal(err)
	}
	m := l.Metrics()
	switch {
	case m.PointsWritten != n || m.BytesWritten <= segmentHeaderSize || m.Syncs < 1:
		t.Fatalf("written: %+v", m)
	case m.Cycles != 2 || len(cycles) != 1 || cycles[0] <= 0:
		t.Fatalf("cycles %d, observed %v", m.Cycles, cycles)
	case m.Segments != 1 || m.DiskBytes < 1<<20:
		t.Fatalf("segments %d, disk %d", m.Segments, m.DiskBytes)
	case m.OldestUnpersisted < 5*time.Millisecond:
		t.Fatalf("oldest unpersisted %v while nothing is persisted", m.OldestUnpersisted)
	case m.WriteErrors != 0 || m.Relogged != 0 || m.Unlogged != 0 || m.ReplayedPoints != 0:
		t.Fatalf("unexpected counts: %+v", m)
	}
	for _, s := range []int{StageCollect, StageEncode, StageCompress, StageWrite, StageSync, StageRoll} {
		if m.Stages[s] <= 0 {
			t.Fatalf("stage %s not timed: %v", StageNames[s], m.Stages)
		}
	}
	src.setLow(0, true)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if m = l.Metrics(); m.OldestUnpersisted != 0 {
		t.Fatalf("oldest unpersisted %v once all is persisted", m.OldestUnpersisted)
	}
	l.Kill()

	// Unpersisted points are replayed by the next instance.
	src = newFakeSource()
	l2, got, stats := reopen(t, dir, opts, src)
	defer l2.Close()
	m = l2.Metrics()
	if len(got) == 0 || m.ReplayedPoints != stats.Points || m.ReplayDuration != stats.Duration || stats.Points != n {
		t.Fatalf("replay metrics %+v, stats %+v", m, stats)
	}
}
