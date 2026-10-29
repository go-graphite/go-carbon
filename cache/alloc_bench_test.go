package cache

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkMakeQueueRebuild(b *testing.B) {
	for _, strategy := range []string{"noop", "max"} {
		b.Run(strategy, func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			if err := c.SetWriteStrategy(strategy); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < 100000; i++ {
				c.AddPoint([]byte(fmt.Sprintf("bench.queue.metric.%06d", i)), 1, int64(i))
			}
			c.makeQueueAt(time.Now())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.makeQueueAt(time.Now())
			}
		})
	}
}

// One op: a metric misses, receives perWriteout points, and is popped.
// perWriteout 0 mixes metrics: seven in ten get one point, the rest eight.
func BenchmarkAddPointWriteoutCycle(b *testing.B) {
	for _, perWriteout := range []int{1, 2, 4, 8, 0} {
		b.Run(fmt.Sprintf("points=%d", perWriteout), func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			names := make([][]byte, 4096)
			for i := range names {
				names[i] = []byte(fmt.Sprintf("bench.writeout.cycle.metric.%04d", i))
			}
			cycle := func(i int) {
				name := names[i%len(names)]
				n := perWriteout
				if n == 0 {
					n = 1
					if i%10 >= 7 {
						n = 8
					}
				}
				for j := 0; j < n; j++ {
					c.AddPoint(name, float64(j), int64(i+j))
				}
				c.Pop(string(name))
			}
			for i := 0; i < 64*len(names); i++ {
				cycle(i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cycle(i)
			}
		})
	}
}
