package cache

import (
	"testing"
	"time"
)

func TestNameVisitDoesNotHoldIngestionLock(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		c := New()
		c.AddPoint([]byte("warmup.metric"), 1, 1)
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			visit := func(string) error { close(entered); <-release; return nil }
			if parallel {
				done <- c.VisitMetricNamesParallel(8, visit)
			} else {
				done <- c.VisitMetricNames(visit)
			}
		}()
		<-entered
		inserted := make(chan struct{})
		go func() { c.AddPoint([]byte("warmup.metric"), 2, 2); close(inserted) }()
		select {
		case <-inserted:
			close(release)
		case <-time.After(time.Second):
			close(release)
			<-done
			<-inserted
			t.Fatal("name visitor held ingestion lock while waiting on external I/O")
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if got := c.Get("warmup.metric"); len(got) != 2 || got[1].Value != 2 {
			t.Fatal(got)
		}
	}
}
