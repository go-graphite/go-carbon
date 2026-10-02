package cache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestRealtimeIndexRetriesAfterOverflow(t *testing.T) {
	c := New()
	c.SetBloomSize(100)
	ch := make(chan string, 1)
	c.SetNewMetricsChan(ch)
	ch <- "queue.full"
	c.Add(points.OnePoint("retry.metric", 1, 1))
	if c.stat.droppedRealtimeIndex != 1 || c.Size() != 1 {
		t.Fatal("overflow must count the notification and retain its datapoint")
	}
	<-ch
	c.Add(points.OnePoint("retry.metric", 2, 2))
	select {
	case metric := <-ch:
		if metric != "retry.metric" {
			t.Fatalf("unexpected notification: %s", metric)
		}
	default:
		t.Fatal("a failed enqueue must be retried on the next sample")
	}
	c.Add(points.OnePoint("retry.metric", 3, 3))
	if len(ch) != 0 {
		t.Fatal("successful notifications must remain deduplicated")
	}
	if got := c.Get("retry.metric"); len(got) != 3 {
		t.Fatalf("lost datapoints: %v", got)
	}
}

func TestRealtimeIndexConcurrentBloom(t *testing.T) {
	c := New()
	c.SetBloomSize(1000)
	c.SetNewMetricsChan(make(chan string, 1000))
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for i := 0; i < 100; i++ {
				c.Add(points.OnePoint(fmt.Sprintf("metric.%d.%d", worker, i), 1, 1))
				c.newMetricCf.Cardinality()
			}
		}(worker)
	}
	workers.Wait()
	if c.Size() != 800 {
		t.Fatalf("lost datapoints: %d", c.Size())
	}
}

func BenchmarkRealtimeIndexAdd(b *testing.B) {
	for _, scenario := range []string{"known", "new", "overflow"} {
		b.Run(scenario, func(b *testing.B) {
			c := New()
			c.SetMaxSize(0)
			c.SetBloomSize(uint64(max(1000000, b.N)))
			ch := make(chan string, 1)
			c.SetNewMetricsChan(ch)
			nameCount := 1
			if scenario == "new" {
				nameCount = b.N
			}
			names := make([]string, nameCount)
			for i := range names {
				names[i] = fmt.Sprintf("benchmark.metric.%d", i)
			}
			if scenario == "known" {
				c.Add(points.OnePoint(names[0], 1, 1))
				<-ch
				c.Pop(names[0])
			} else if scenario == "overflow" {
				ch <- "full"
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				name := names[i%len(names)]
				if scenario == "known" {
					name = names[0]
				}
				c.Add(points.OnePoint(name, 1, 1))
				c.Pop(name)
				if scenario == "new" {
					select {
					case <-ch:
					default:
					}
				}
			}
		})
	}
}
