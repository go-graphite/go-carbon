package inputlog

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// A reader blocked in admission must not stop an unrelated partition behind
// the old producer's four-frame queue. Exercise more frames than that queue.
func TestReplayReadersProgressIndependently(t *testing.T) {
	previous := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(previous)
	dir := t.TempDir()
	src := newFakeSource()
	log := openStarted(t, dir, Options{SegmentSize: 1 << 20}, src)
	for cycle := range 12 {
		src.add(strings.Repeat("a", 16), float64(cycle), int64(cycle))
		src.add(strings.Repeat("b", 17), float64(cycle), int64(cycle))
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	log.Kill()
	log, err := Open(dir, withTestDefaults(Options{SegmentSize: 1 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	finished := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		count := 0 // only partition 1 writes this value
		_, err := log.Replay(func(p *points.Points) {
			if p.Metric[0] == 'a' {
				<-release
				return
			}
			count += len(p.Data)
			if count == 12 {
				close(finished)
			}
		})
		done <- err
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		releaseOnce.Do(func() { close(release) })
		<-done
		t.Fatal("unrelated partition stalled behind blocked reader")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
