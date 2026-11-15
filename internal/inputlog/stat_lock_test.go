package inputlog

import (
	"testing"
	"time"
)

type metricsSource struct{ fakeSource }

func (*metricsSource) LogDropped() uint64 { panic("statistics entered source locks") }

func TestStatDoesNotEnterSource(t *testing.T) {
	l := &Log{src: &metricsSource{}}
	for _, count := range []int64{5, 9} {
		l.total.unlogged.Store(count)
		want := float64(count - l.stat.lastDropped.Load())
		got := -1.0
		l.Stat(func(k string, v float64) {
			if k == "dropped" {
				got = v
			}
		})
		if got != want {
			t.Fatalf("dropped delta %g want %g", got, want)
		}
	}
}

func TestStatDuringStart(t *testing.T) {
	l, err := Open(t.TempDir(), Options{SyncInterval: time.Hour, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	l.total.unlogged.Store(7)
	started := make(chan struct{})
	go func() {
		l.Start(newFakeSource(), partitions)
		close(started)
	}()
	defer l.Close()
	var dropped float64
	for {
		l.Stat(func(name string, value float64) {
			if name == "dropped" {
				dropped += value
			}
		})
		select {
		case <-started:
			if dropped != 7 {
				t.Fatalf("dropped delta sum %g, want 7", dropped)
			}
			return
		default:
		}
	}
}
