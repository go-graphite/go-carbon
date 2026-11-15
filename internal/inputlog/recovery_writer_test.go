package inputlog

import (
	"math"
	"os"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// Open's inventory is immutable even if the writer has advanced generations,
// rolled segments, and would normally recycle generation zero while replay is
// blocked. A historical point is not persisted merely because it was applied.
func TestRecoveryWriterKeepsHistoricalInventory(t *testing.T) {
	dir := t.TempDir()
	opts := withTestDefaults(Options{SegmentSize: 256})
	seed := newFakeSource()
	l := openStarted(t, dir, opts, seed)
	seed.add("same", math.Float64frombits(0x7ff8000000000042), 17)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	oldPath := l.pending[0].seg.path
	src := newFakeSource()
	src.setLow(0, true)
	l.StartRecovery(src, testShards)
	// Empty cycles can remove the original generation-zero tick. They must
	// not lose the original retention authority when replay later completes.
	for range 3 {
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		stats, err := l.ReplayStopped(func(p *points.Points) {
			if p.Metric != "same" || len(p.Data) != 1 || math.Float64bits(p.Data[0].Value) != 0x7ff8000000000042 {
				t.Errorf("historical replay changed: %+v", p)
			}
			close(entered)
			<-resume
		}, nil)
		if err != nil || stats.Points != 1 {
			t.Errorf("historical replay: %+v %v", stats, err)
		}
	})
	<-entered
	for i := range 5 {
		src.add("live", float64(i), int64(i))
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("unread history recycled: %v", err)
	}
	// Model the replayed batch still in flight at the current generation.
	src.setLow(8, false)
	close(resume)
	wg.Wait()
	if err := l.CompleteRecovery(); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		src.add("later", float64(i), int64(i))
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("applied unpersisted history recycled: %v", err)
	}
	// Reopen checks the durable replayFrom boundary, not just inode retention.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, _ := replayAll(t, l)
	if len(got["same"]) != 1 || math.Float64bits(got["same"][0].Value) != 0x7ff8000000000042 {
		t.Fatal("applied unpersisted point missing after restart", got)
	}
	// Completion is a transfer of authority, not an indefinite copy pin.
	// Once persistence passes that generation, ordinary recycling must resume.
	src = newFakeSource()
	l.StartRecovery(src, testShards)
	if _, err := l.Replay(func(*points.Points) {}); err != nil {
		t.Fatal(err)
	}
	if err := l.CompleteRecovery(); err != nil {
		t.Fatal(err)
	}
	src.setLow(1000, false)
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("persisted history retained after release: %v", err)
	}
}

func TestRecoveryWriterCancellationKeepsPrefix(t *testing.T) {
	dir := t.TempDir()
	opts := withTestDefaults(Options{SegmentSize: 256})
	src := newFakeSource()
	l := openStarted(t, dir, opts, src)
	src.add("history", 1, 1)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	src = newFakeSource()
	src.setLow(0, true)
	l.StartRecovery(src, testShards)
	stop := make(chan struct{})
	close(stop)
	if _, err := l.ReplayStopped(func(*points.Points) { t.Error("applied after cancellation") }, stop); err == nil {
		t.Fatal("replay ignored cancellation")
	}
	for range 4 {
		src.add("suffix", 2, 2)
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, _ := replayAll(t, l)
	if len(got["history"]) != 1 {
		t.Fatal("cancelled recovery lost original authority", got)
	}
}
