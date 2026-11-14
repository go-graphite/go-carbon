package helper

import (
	"fmt"
	"sync"
	"testing"
)

func TestMetricQueueCoalescesThroughAcknowledgement(t *testing.T) {
	q := NewMetricQueue()
	q.Add("a")
	q.Add("a")
	batch := q.Take(1)
	q.Add("a")
	if len(batch) != 1 || batch[0] != "a" || q.Len() != 1 || len(q.Take(1)) != 0 {
		t.Fatal("in-flight name was duplicated")
	}
	q.Ack(batch)
	if q.Len() != 0 {
		t.Fatal(q.Len())
	}
	q.Add("a")
	if q.Len() != 1 {
		t.Fatal("acknowledged name cannot be enqueued again")
	}
}

func TestMetricQueueConcurrentBurst(t *testing.T) {
	q := NewMetricQueue()
	const total = 400000 // Exceeds the old production-sized queue.
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for i := worker; i < total; i += 8 {
				name := fmt.Sprintf("burst.%d", i)
				q.Add(name)
				q.Add(name)
			}
		})
	}
	wg.Wait()
	if q.Len() != total {
		t.Fatalf("pending=%d", q.Len())
	}
	seen := make(map[string]bool, total)
	for q.Len() > 0 {
		batch := q.Take(4096)
		if len(batch) == 0 {
			t.Fatal("backlog not drainable")
		}
		for _, name := range batch {
			if seen[name] {
				t.Fatal("duplicate", name)
			}
			seen[name] = true
		}
		q.Ack(batch)
	}
	if len(seen) != total {
		t.Fatal(len(seen))
	}
	for i := range q.shards {
		if q.shards[i].names != nil || q.shards[i].waiting != nil {
			t.Fatal("drained storage retained")
		}
	}
}

func TestMetricQueueConcurrentConsumer(t *testing.T) {
	q := NewMetricQueue()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for i := range 10000 {
				q.Add(fmt.Sprintf("worker%d.metric%d", worker, i))
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	count := 0
	for {
		batch := q.Take(127)
		count += len(batch)
		q.Ack(batch)
		select {
		case <-done:
			if q.Len() == 0 {
				if count != 80000 {
					t.Fatal(count)
				}
				return
			}
		default:
		}
	}
}
