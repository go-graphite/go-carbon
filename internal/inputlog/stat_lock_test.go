package inputlog

import "testing"

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
