package carbon

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestCache(t *testing.T) {

	c := NewCache()

	c.Add("hello.world", 42, 10)

	if c.Size() != 1 {
		t.FailNow()
	}

	c.Add("hello.world", 15, 12)

	if c.Size() != 2 {
		t.FailNow()
	}

	key, values := c.Get()

	if key != "hello.world" {
		t.FailNow()
	}

	if len(values.Data) != 2 {
		t.FailNow()
	}

	c.Remove(key)

	if c.Size() != 0 {
		t.FailNow()
	}
}

func TestCacheCheckpoint(t *testing.T) {
	cache := NewCache()
	cache.Start()
	cache.SetOutputChanSize(0)

	defer cache.Stop()

	startTime := time.Now().Unix() - 60*60

	sizes := []int{1, 15, 42, 56, 22, 90, 1}

	for index, value := range sizes {
		metricName := fmt.Sprintf("metric%d", index)

		for i := value; i > 0; i-- {

			cache.In() <- &Message{
				Name:      metricName,
				Value:     float64(i),
				Timestamp: startTime + int64(i),
			}
		}

	}

	time.Sleep(100 * time.Millisecond)
	cache.doCheckpoint()

	d := <-cache.Out()
	if d.Metric != "metric0" {
		t.Fatal("wrong metric received")
	}

	result := sizes[1:]
	sort.Sort(sort.Reverse(sort.IntSlice(result)))

	for _, size := range result {
		d = <-cache.Out()
		if len(d.Data) != size {
			t.Fatalf("wrong metric received. Waiting metric with %d points, received with %d", size, len(d.Data))
		}
	}
}
