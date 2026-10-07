package cache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestBinaryXlogConcurrentRecords(t *testing.T) {
	c := New()
	var out bytes.Buffer
	c.DivertToBinaryXlog(&out)
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
				c.Add(points.OnePoint(fmt.Sprintf("worker.%d", worker), float64(n), int64(n)).Add(-float64(n), int64(n+1000)))
			}
		}(worker)
	}
	wg.Wait()
	seen := map[string]int{}
	if err := points.ReadBinary(&out, func(p *points.Points) {
		n := seen[p.Metric]
		if len(p.Data) != 2 || p.Data[0].Timestamp != int64(n) || p.Data[0].Value != float64(n) || p.Data[1].Value != -float64(n) {
			t.Fatalf("interleaved or reordered record: %+v", p)
		}
		seen[p.Metric]++
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 16 {
		t.Fatalf("lost writers: %v", seen)
	}
	for _, count := range seen {
		if count != 1000 {
			t.Fatal(count)
		}
	}
	if !c.IsEmpty() {
		t.Fatal("diverted points entered the cache")
	}
}
