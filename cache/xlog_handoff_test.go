package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestDumpAndDiversionPreserveConcurrentAdds(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		c := New()
		const workers, perWorker = 16, 200
		var mu sync.Mutex
		seen := make(map[int]int)
		save := func(p *points.Points) error {
			mu.Lock()
			defer mu.Unlock()
			for _, point := range p.Data {
				seen[int(point.Value)]++
			}
			return nil
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < perWorker; i++ {
					c.Add(points.OnePoint(fmt.Sprintf("metric.%d", worker), float64(worker*perWorker+i), 1))
					if i%7 == 0 {
						runtime.Gosched()
					}
				}
			}()
		}
		close(start)
		runtime.Gosched()
		c.DivertToPointWriter(save)
		if err := c.DumpPoints(save); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
		if len(seen) != workers*perWorker {
			t.Fatalf("dump/WAL lost accepted points: %d/%d", len(seen), workers*perWorker)
		}
		for id, count := range seen {
			if count != 1 {
				t.Fatalf("point %d saved %d times", id, count)
			}
		}
	}
}
