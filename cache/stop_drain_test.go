package cache

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestWriteStrategyChangeDiscardsQueuedOrder(t *testing.T) {
	c := New()
	c.AddPoint([]byte("a.light"), 1, 1)
	for i := range 8 {
		c.AddPoint([]byte("z.heavy"), 1, int64(i+1))
	}
	if err := c.SetWriteStrategy("lexical"); err != nil {
		t.Fatal(err)
	}
	c.writeoutQueue.update()
	if err := c.SetWriteStrategy("max"); err != nil {
		t.Fatal(err)
	}
	abort := make(chan bool)
	defer close(abort)
	if got := c.WriteoutQueue().Get(abort); got != "z.heavy" {
		t.Fatal("old queue survived strategy change", got)
	}
}

func TestWriteStrategyChangeDiscardsInFlightBuild(t *testing.T) {
	c := New()
	c.AddPoint([]byte("a.light"), 1, 1)
	for i := range 8 {
		c.AddPoint([]byte("z.heavy"), 1, int64(i+1))
	}
	if err := c.SetWriteStrategy("lexical"); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.SetRestartOrder(func() bool { close(entered); <-release; return false })
	go func() { defer close(done); c.writeoutQueue.update() }()
	<-entered // The builder captured the preceding strategy.
	if err := c.SetWriteStrategy("max"); err != nil {
		t.Fatal(err)
	}
	c.SetRestartOrder(nil)
	close(release)
	<-done
	if c.WriteoutQueue().TryGet() != "" {
		t.Fatal("stale build was published")
	}
	abort := make(chan bool)
	defer close(abort)
	if got := c.WriteoutQueue().Get(abort); got != "z.heavy" {
		t.Fatal("new order not rebuilt", got)
	}
}

// Opt in: INGEST_BENCH_METRICS=28000000 go test ./cache -run '^$'
// -bench BenchmarkStopDrainQueue -benchtime=1x -benchmem. Fixture generation
// is outside the timer; varied batch lengths exercise the max sort.
func BenchmarkStopDrainQueue(b *testing.B) {
	n, _ := strconv.Atoi(os.Getenv("INGEST_BENCH_METRICS"))
	if n <= 0 {
		b.Skip("set INGEST_BENCH_METRICS")
	}
	c := New()
	c.SetMaxSize(0)
	for i := range n {
		name := fmt.Appendf(nil, "queue.metric.%08d", i)
		for j := range 1 + i%8 {
			c.AddPoint(name, 1, int64(j+1))
		}
	}
	for _, strategy := range []string{"max", "noop"} {
		b.Run(strategy, func(b *testing.B) {
			if err := c.SetWriteStrategy(strategy); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				q, _ := c.makeQueueAt(time.Now())
				if len(q) != n {
					b.Fatal("incomplete queue", len(q), n)
				}
			}
		})
	}
}
