package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func batchingCache(t *testing.T, min int, delay time.Duration) *Cache {
	t.Helper()
	c := New()
	if err := c.SetWriteoutBatching(min, delay); err != nil {
		t.Fatal(err)
	}
	return c
}

func requireMetric(t *testing.T, queue chan string, want string) {
	t.Helper()
	select {
	case got := <-queue:
		if got != want {
			t.Fatalf("metric = %q, want %q", got, want)
		}
	default:
		t.Fatalf("metric %q is not eligible", want)
	}
}

func TestBatchingThresholdAndArrivalDeadline(t *testing.T) {
	for _, strategy := range []string{"max", "sorted", "noop"} {
		t.Run(strategy, func(t *testing.T) {
			c := batchingCache(t, 8, 2*time.Second)
			if err := c.SetWriteStrategy(strategy); err != nil {
				t.Fatal(err)
			}
			c.Add(points.OnePoint("sparse", 1, 1)) // Historical sample timestamp is not arrival time.
			shard := c.GetShard("sparse")
			first := shard.firstArrival[shard.items["sparse"]]
			q, deadline := c.makeQueueAt(first.Add(time.Second))
			if q != nil || !deadline.Equal(first.Add(2*time.Second)) {
				t.Fatalf("premature queue/deadline: %v %v", q, deadline)
			}
			if got := c.Get("sparse"); len(got) != 1 {
				t.Fatal("deferred point is not readable")
			}
			q, _ = c.makeQueueAt(deadline)
			requireMetric(t, q, "sparse")
			for i := 0; i < 7; i++ {
				c.Add(points.OnePoint("sparse", float64(i), 1))
			}
			q, _ = c.makeQueueAt(first)
			requireMetric(t, q, "sparse")
		})
	}
}

func TestBatchingRequeueRetainsDeadline(t *testing.T) {
	c := batchingCache(t, 8, 2*time.Second)
	c.Add(points.OnePoint("metric", 1, 1))
	shard := c.GetShard("metric")
	first := shard.firstArrival[shard.items["metric"]]
	p, _ := c.PopNotConfirmed("metric")
	c.Add(points.OnePoint("metric", 2, 1))
	c.Requeue(p)
	if got := shard.firstArrival[p]; !got.Equal(first) {
		t.Fatalf("retry changed arrival from %v to %v", first, got)
	}
	if got := c.Get("metric"); len(got) != 2 || got[0].Value != 1 || got[1].Value != 2 {
		t.Fatalf("retry order: %v", got)
	}
	q, _ := c.makeQueueAt(first.Add(2 * time.Second))
	requireMetric(t, q, "metric")
	p, _ = c.PopNotConfirmed("metric")
	c.Confirm(p)
	if len(shard.firstArrival) != 0 || !c.IsEmpty() {
		t.Fatal("confirmed batch retains data or arrival metadata")
	}
}

func TestBatchingBypasses(t *testing.T) {
	t.Run("pressure", func(t *testing.T) {
		c := batchingCache(t, 100, time.Hour)
		c.SetMaxSize(100)
		for i := 0; i < 75; i++ {
			c.Add(points.OnePoint("metric", 1, 1))
		}
		if q, _ := c.makeQueueAt(time.Now()); q != nil {
			t.Fatal("batch bypassed at 75 percent")
		}
		c.Add(points.OnePoint("metric", 1, 1))
		requireMetric(t, c.makeQueue(), "metric")
	})
	t.Run("reload and shutdown", func(t *testing.T) {
		c := New()
		c.Add(points.OnePoint("existing", 1, 1))
		if err := c.SetWriteoutBatching(8, time.Hour); err != nil {
			t.Fatal(err)
		}
		requireMetric(t, c.makeQueue(), "existing")
		c.Pop("existing")
		c.Add(points.OnePoint("new", 1, 1))
		if err := c.SetWriteoutBatching(0, 0); err != nil {
			t.Fatal(err)
		}
		requireMetric(t, c.makeQueue(), "new")
		c.Pop("new")
		if len(c.GetShard("new").firstArrival) != 0 {
			t.Fatal("Pop retains arrival metadata")
		}
	})
	t.Run("restored into live and retried batch", func(t *testing.T) {
		c := batchingCache(t, 100, time.Hour)
		c.Add(points.OnePoint("metric", 1, 1))
		failed, _ := c.PopNotConfirmed("metric")
		c.Add(points.OnePoint("metric", 2, 2))
		c.AddRestored(points.OnePoint("metric", 3, 3))
		c.Requeue(failed)
		requireMetric(t, c.makeQueue(), "metric")
	})
}

func waitBuild(t *testing.T, c *Cache) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for atomic.LoadUint32(&c.stat.queueBuildCnt) == 0 {
		if time.Now().After(until) {
			t.Fatal("queue did not build")
		}
		time.Sleep(time.Millisecond)
	}
}

func queuedMetric(t *testing.T, got <-chan string, want string) {
	t.Helper()
	select {
	case metric := <-got:
		if metric != want {
			t.Fatalf("Get = %q, want %q", metric, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not wake")
	}
}

func TestBatchingWaitDoesNotRescanSparseArrivals(t *testing.T) {
	c := batchingCache(t, 8, time.Hour)
	abort := make(chan bool)
	defer close(abort)
	got := make(chan string, 1)
	go func() { got <- c.WriteoutQueue().Get(abort) }()
	waitBuild(t, c)
	builds := atomic.LoadUint32(&c.stat.queueBuildCnt)
	for i := 0; i < 100; i++ {
		c.Add(points.OnePoint(fmt.Sprintf("sparse.%d", i), 1, 1))
	}
	time.Sleep(150 * time.Millisecond)
	if n := atomic.LoadUint32(&c.stat.queueBuildCnt); n != builds {
		t.Fatalf("rescanned deferred data: %d -> %d", builds, n)
	}
	for i := 0; i < 7; i++ {
		c.Add(points.OnePoint("sparse.0", 2, 2))
	}
	queuedMetric(t, got, "sparse.0")
}

func TestBatchingQueueWakeups(t *testing.T) {
	for _, event := range []string{"deadline", "pressure", "restore", "disable", "shorter delay", "retry"} {
		t.Run(event, func(t *testing.T) {
			delay := time.Hour
			if event == "deadline" {
				delay = 150 * time.Millisecond
			}
			c := batchingCache(t, 8, delay)
			c.SetMaxSize(100)
			abort := make(chan bool)
			defer close(abort)
			got := make(chan string, 1)
			go func() { got <- c.WriteoutQueue().Get(abort) }()
			waitBuild(t, c)
			c.Add(points.OnePoint("metric", 1, 1))
			switch event {
			case "pressure":
				c.SetMaxSize(1)
			case "restore":
				c.AddRestored(points.OnePoint("metric", 2, 2))
			case "disable":
				if err := c.SetWriteoutBatching(0, 0); err != nil {
					t.Fatal(err)
				}
			case "shorter delay":
				if err := c.SetWriteoutBatching(8, time.Nanosecond); err != nil {
					t.Fatal(err)
				}
			case "retry":
				c.AddRestored(points.OnePoint("metric", 2, 2))
				p, _ := c.PopNotConfirmed("metric")
				c.Requeue(p)
			}
			queuedMetric(t, got, "metric")
		})
	}
}

func TestBatchingAbortAndResume(t *testing.T) {
	c := batchingCache(t, 8, time.Hour)
	abort := make(chan bool)
	got := make(chan string, 1)
	go func() { got <- c.WriteoutQueue().Get(abort) }()
	waitBuild(t, c)
	close(abort)
	queuedMetric(t, got, "")
	c.AddRestored(points.OnePoint("metric", 1, 1))
	go func() { got <- c.WriteoutQueue().Get(nil) }()
	queuedMetric(t, got, "metric")
}

func TestBatchingIndependentAbort(t *testing.T) {
	c := batchingCache(t, 8, time.Hour)
	firstAbort := make(chan bool)
	defer close(firstAbort)
	first := make(chan string, 1)
	go func() { first <- c.WriteoutQueue().Get(firstAbort) }()
	waitBuild(t, c)
	secondAbort := make(chan bool)
	second := make(chan string, 1)
	go func() { second <- c.WriteoutQueue().Get(secondAbort) }()
	close(secondAbort)
	queuedMetric(t, second, "")
	c.AddRestored(points.OnePoint("metric", 1, 1))
	queuedMetric(t, first, "metric")
}

func TestBatchingDumpRestore(t *testing.T) {
	c := batchingCache(t, 8, time.Hour)
	c.Add(points.OnePoint("metric", 1, 1))
	c.PopNotConfirmed("metric")
	c.Add(points.OnePoint("metric", 2, 2))
	filename := filepath.Join(t.TempDir(), "cache.bin")
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DumpBinary(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	restored := batchingCache(t, 8, time.Hour)
	if err := points.ReadFromFile(filename, restored.AddRestored); err != nil {
		t.Fatal(err)
	}
	requireMetric(t, restored.makeQueue(), "metric")
	got := restored.Get("metric")
	if len(got) != 2 || got[0].Value != 1 || got[1].Value != 2 {
		t.Fatalf("restored: %v", got)
	}
}

func TestBatchingConcurrentIngestReadRetry(t *testing.T) {
	c := batchingCache(t, 8, 10*time.Millisecond)
	const count = 200
	abort := make(chan bool)
	var producers, consumers sync.WaitGroup
	var persisted atomic.Int64
	for worker := 0; worker < 4; worker++ {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			retried := false
			for {
				metric := c.WriteoutQueue().Get(abort)
				if metric == "" {
					return
				}
				p, ok := c.PopNotConfirmed(metric)
				if !ok {
					continue
				}
				if !retried {
					c.Requeue(p)
					retried = true
					continue
				}
				persisted.Add(int64(len(p.Data)))
				c.Confirm(p)
			}
		}()
		producers.Add(1)
		go func(worker int) {
			defer producers.Done()
			for i := 0; i < count; i++ {
				metric := fmt.Sprintf("metric.%d.%d", worker, i%10)
				c.Add(points.OnePoint(metric, float64(i), int64(i)))
				_ = len(c.Get(metric))
			}
		}(worker)
	}
	producers.Wait()
	until := time.Now().Add(3 * time.Second)
	for !c.IsEmpty() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	close(abort)
	consumers.Wait()
	if !c.IsEmpty() || persisted.Load() != 4*count {
		t.Fatalf("persisted %d/%d; cache empty=%v", persisted.Load(), 4*count, c.IsEmpty())
	}
}

func BenchmarkBatchingCacheCycle(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("enabled=%v", enabled), func(b *testing.B) {
			c := New()
			if enabled {
				_ = c.SetWriteoutBatching(8, 2*time.Second)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < 8; j++ {
					c.Add(points.OnePoint("metric", 1, 1))
				}
				p, _ := c.PopNotConfirmed("metric")
				c.Confirm(p)
			}
		})
	}
}
