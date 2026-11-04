package persister

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
)

type latencyFloorStore struct {
	autoReloadStore
	latency time.Duration
	started time.Time
	stall   time.Duration
}

func (s *latencyFloorStore) wait() {
	// Correlated storage/scheduler pauses leave enough productive capacity:
	// eight workers can do 80 updates in each remaining 20ms at 2ms/update.
	phase := time.Since(s.started) % (100 * time.Millisecond)
	if phase < s.stall {
		time.Sleep(s.stall - phase)
	}
	time.Sleep(s.latency)
}
func (*latencyFloorStore) MetadataCached(string) bool { return false }
func (*latencyFloorStore) PrewarmWriteout(context.Context, []string) {
	time.Sleep(48 * time.Millisecond)
}

func TestRestartFloorWithStoreLatency(t *testing.T) {
	for _, latency := range []time.Duration{time.Millisecond, 2 * time.Millisecond} {
		t.Run(latency.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const floor = 585.5
				core := cache.New()
				defer core.Stop()
				core.SetMaxSize(0)
				if err := core.SetWriteoutBatching(960, 960*time.Second); err != nil {
					t.Fatal(err)
				}
				for i := range 12000 {
					core.Add(points.OnePoint(fmt.Sprintf("latency.%d", i), 1, 1))
				}
				time.Sleep(960 * time.Second) // Make the restored-sized backlog eligible.
				started := time.Now()
				db := &latencyFloorStore{latency: latency, started: started, stall: 80 * time.Millisecond}
				budget := restartcpu.NewWithCPU(.238, func() float64 { return .28*time.Since(started).Seconds() + float64(db.written.Load())*.0001 })
				budget.SetSteadyRate(floor)
				core.SetRestartOrder(budget.Active)
				core.SetRestartWarmFirst(db.MetadataCached, budget.WaitPrefetch, budget.EndPrefetch)
				var skipped atomic.Uint64
				p := &Whisper{recv: core.WriteoutQueue().Get}
				p.SetWriteoutReady(core.WriteoutReady)
				p.SetMockStore(func() (StoreFunc, func()) {
					return func(name string) {
						db.wait()
						if _, ok := core.Pop(name); ok {
							db.written.Add(1)
							p.recordUpdate()
						} else {
							skipped.Add(1)
						}
					}, nil
				})
				p.SetWriteoutBatchReceiver(core.WriteoutQueue().TryGet)
				p.SetRequeue(core.Add)
				p.SetStoreUpdateBatchSize(8)
				p.SetMetricStore(db)
				p.SetWarmFirst(true)
				p.SetWorkers(8)
				p.SetMaxUpdatesPerSecond(50000)
				p.SetRestartBudget(budget, func() float64 { return floor })
				if err := p.Start(); err != nil {
					t.Fatal(err)
				}
				defer p.Stop()
				time.Sleep(time.Second)
				synctest.Wait()
				before := budget.Updates()
				time.Sleep(10 * time.Second)
				synctest.Wait()
				rate := float64(budget.Updates()-before) / 10
				t.Logf("latency=%s rate=%.2f/s floor=%.1f/s stale_writes=%d grants=%d", latency, rate, floor, skipped.Load(), budget.State().FloorGrants)
				if rate < .95*floor {
					t.Fatalf("writeout below mandatory floor: %.2f/s", rate)
				}
			})
		})
	}
}
