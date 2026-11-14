package persister

import (
	"context"
	"errors"
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
