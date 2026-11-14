package persister

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestPointsBalanceAcrossPersistenceRoutes(t *testing.T) {
	for _, route := range []string{"single", "batch", "transfer"} {
		t.Run(route, func(t *testing.T) {
			c := cache.New()
			names := []string{"a", "b"}
			for _, name := range names {
				c.Add(points.OnePoint(name, 1, 10).Add(2, 10))
			}
			p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
			p.SetRequeue(c.Requeue)
			var calls []string
			run := func() {
				switch route {
				case "single":
					for _, name := range names {
						p.store(name)
					}
				case "batch":
					p.storeSharedBatch(names)
				case "transfer":
					_ = p.FlushForTransfer(context.Background(), names)
				}
			}
			p.SetMetricStore(transferBatchStore{failedSharedStore{&calls, errors.New("write failed")}})
			run()
			if p.CommittedPoints() != 0 || c.Size() != 4 || c.AdmissionStats().InFlight != 0 {
				t.Fatal("failed input lost", p.CommittedPoints(), c.Size(), c.AdmissionStats())
			}
			p.SetMetricStore(transferBatchStore{failedSharedStore{&calls, nil}})
			run()
			if p.CommittedPoints() != 4 || c.Size() != 0 || c.AdmissionStats().InFlight != 0 {
				t.Fatal("committed input miscounted", p.CommittedPoints(), c.Size(), c.AdmissionStats())
			}
			for range 2 {
				metrics := map[string]float64{}
				p.Stat(func(n string, v float64) { metrics[n] = v })
				if metrics["committed_points_total"] != 4 || metrics["write_failed_points_total"] != 4 {
					t.Fatal(metrics)
				}
			}
		})
	}
}

func TestCommittedPointsConcurrentCollectionOracle(t *testing.T) {
	p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, nil, nil, nil)
	var want atomic.Uint64
	var wg sync.WaitGroup
	done := make(chan struct{})
	var scrape sync.WaitGroup
	scrape.Add(1)
	go func() {
		defer scrape.Done()
		for {
			select {
			case <-done:
				return
			default:
				p.CommittedPoints()
			}
		}
	}()
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 512; i++ {
				name := fmt.Sprintf("metric.%d", (i+w)%11)
				batch := &points.Points{Metric: name, Data: make([]points.Point, i%17+1)}
				mu := &p.storeMutex[storeMutexIndex(name)]
				mu.Lock()
				p.committedPointsTotal.Add(uint64(len(batch.Data)))
				mu.Unlock()
				want.Add(uint64(len(batch.Data)))
			}
		}(w)
	}
	wg.Wait()
	close(done)
	scrape.Wait()
	for range 2 {
		if got := p.CommittedPoints(); got != want.Load() {
			t.Fatalf("got %d want %d", got, want.Load())
		}
		p.Stat(func(string, float64) {})
	}
}

func TestCommittedPointIntervalsPartitionTotal(t *testing.T) {
	p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, nil, nil, nil)
	var sum atomic.Uint64
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				p.committedPointsTotal.Add(uint64(i%17 + 1))
				sum.Add(p.committedPointsDelta())
			}
		}()
	}
	wg.Wait()
	sum.Add(p.committedPointsDelta())
	if sum.Load() != p.CommittedPoints() {
		t.Fatalf("interval sum %d total %d", sum.Load(), p.CommittedPoints())
	}
	if p.committedPointsDelta() != 0 {
		t.Fatal("reported points twice")
	}
}
