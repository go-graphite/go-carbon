package persister

import (
	"context"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
	"iter"
	"reflect"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

type controlledWarmer struct {
	entered chan []string
	release chan struct{}
}

func (*controlledWarmer) Metadata(context.Context, string) (store.Metadata, error) {
	return store.Metadata{}, nil
}
func (*controlledWarmer) Create(context.Context, store.MetricConfig) (store.Metadata, error) {
	return store.Metadata{}, nil
}
func (*controlledWarmer) UpdateMany(context.Context, string, []points.Point) error { return nil }
func (*controlledWarmer) MetadataCached(name string) bool                          { return name == "warm" }
func (w *controlledWarmer) PrewarmWriteout(ctx context.Context, names []string) {
	select {
	case w.entered <- append([]string(nil), names...):
	case <-ctx.Done():
		return
	}
	select {
	case <-w.release:
	case <-ctx.Done():
	}
}
func TestWarmFirstPassesWarmNamesAndJoinsPrefetch(t *testing.T) {
	source, writes := make(chan string, 3), make(chan string, 3)
	w := &controlledWarmer{entered: make(chan []string, 1), release: make(chan struct{})}
	p := &Whisper{recv: func(exit chan bool) string {
		select {
		case name := <-source:
			return name
		case <-exit:
			return ""
		}
	}}
	p.SetWriteoutBatchReceiver(func() string {
		select {
		case name := <-source:
			return name
		default:
			return ""
		}
	})
	p.SetMetricStore(w)
	p.SetWarmFirst(true)
	p.SetWorkers(1)
	p.SetRestartBudget(restartcpu.NewWithCPU(1, func() float64 { return 0 }), nil)
	p.SetMockStore(func() (StoreFunc, func()) { return func(name string) { writes <- name }, nil })
	for cycle := 0; cycle < 2; cycle++ {
		source <- "cold-old"
		source <- "warm"
		source <- "cold-new"
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		select {
		case names := <-w.entered:
			if !reflect.DeepEqual(names, []string{"cold-old", "cold-new"}) {
				t.Fatal(names)
			}
		case <-time.After(time.Second):
			t.Fatal("cold window did not prefetch")
		}
		select {
		case name := <-writes:
			if name != "warm" {
				t.Fatal(name)
			}
		case <-time.After(time.Second):
			t.Fatal("warm write waited behind cold window")
		}
		if cycle == 0 {
			close(w.release)
			for range 2 {
				select {
				case <-writes:
				case <-time.After(time.Second):
					t.Fatal("cold writes did not resume")
				}
			}
			p.Stop()
			w.release = make(chan struct{})
		} else {
			stopped := make(chan struct{})
			go func() { p.Stop(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("stop did not cancel and join prefetch")
			}
		}
	}
}

func TestRestartCatchupHonorsSteadyRateAndConfiguredCap(t *testing.T) {
	budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
	budget.SetSteadyRate(100)
	p := &Whisper{restartBudget: budget, maxUpdatesPerSecond: 1000}
	p.setRestartRate()
	if p.restartRate != 300 {
		t.Fatal("catch-up ceiling left CPU headroom unavailable", p.restartRate)
	}
	p.maxUpdatesPerSecond = 50
	p.setRestartRate()
	if p.restartRate != 50 {
		t.Fatal("explicit rate cap ignored", p.restartRate)
	}
	p.stopRestartTicker()
}

func TestWarmFirstReservationsDeduplicateRebuiltQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := make(chan string, 3)
		source <- "warm"
		source <- "warm"
		source <- "cold-new"
		calls := make(chan string, 4)
		allowWarm := make(chan struct{})
		warmer := &controlledWarmer{entered: make(chan []string, 1), release: make(chan struct{})}
		budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
		p := &Whisper{recv: func(exit chan bool) string {
			select {
			case name := <-source:
				return name
			case <-exit:
				return ""
			}
		}}
		p.SetWriteoutBatchReceiver(func() string {
			select {
			case name := <-source:
				return name
			default:
				return ""
			}
		})
		p.SetMetricStore(warmer)
		p.SetWarmFirst(true)
		p.SetWorkers(2)
		p.SetMaxUpdatesPerSecond(10)
		p.SetRestartBudget(budget, nil)
		p.SetMockStore(func() (StoreFunc, func()) {
			return func(name string) {
				calls <- name
				if name == "warm" {
					<-allowWarm
				}
				p.recordUpdate()
			}, nil
		})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		<-warmer.entered
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		if len(calls) != 1 {
			t.Fatal("rebuilt queue admitted a duplicate pending write", len(calls))
		}
		close(warmer.release)
		time.Sleep(time.Second)
		synctest.Wait()
		if len(calls) != 2 {
			t.Fatal("distinct cold write did not progress", len(calls))
		}
		close(allowWarm)
		synctest.Wait()
		p.Stop()
		if budget.Updates() != 2 {
			t.Fatal("duplicate consumed confirmed throughput", budget.Updates())
		}
	})
}

type validatingWarmer struct {
	*controlledWarmer
	validated chan []string
}

func (w *validatingWarmer) ValidateMetadataCheckpoint(_ context.Context, names iter.Seq[string], options store.PrewarmOptions) (store.MetadataCheckpointStats, error) {
	if options.Wait == nil || options.Batch != store.DefaultPrewarmBatch {
		panic("validation must use the shared allowance in writeout-sized batches")
	}
	w.validated <- slices.Collect(names)
	return store.MetadataCheckpointStats{}, nil
}

// Provisional checkpoint names are offered for validation before the cold
// window's writeout admission and prefetch.
func TestWarmFirstValidatesColdWindowBeforePrefetch(t *testing.T) {
	source, writes := make(chan string, 3), make(chan string, 3)
	source <- "cold-old"
	source <- "warm"
	source <- "cold-new"
	w := &validatingWarmer{controlledWarmer: &controlledWarmer{entered: make(chan []string, 1), release: make(chan struct{})}, validated: make(chan []string, 1)}
	p := &Whisper{recv: func(exit chan bool) string {
		select {
		case name := <-source:
			return name
		case <-exit:
			return ""
		}
	}}
	p.SetWriteoutBatchReceiver(func() string {
		select {
		case name := <-source:
			return name
		default:
			return ""
		}
	})
	p.SetMetricStore(w)
	p.SetWarmFirst(true)
	p.SetWorkers(1)
	p.SetRestartBudget(restartcpu.NewWithCPU(1, func() float64 { return 0 }), nil)
	p.SetMockStore(func() (StoreFunc, func()) { return func(name string) { writes <- name }, nil })
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	want := []string{"cold-old", "cold-new"}
	select {
	case names := <-w.validated:
		if !reflect.DeepEqual(names, want) {
			t.Fatal(names)
		}
	case <-time.After(time.Second):
		t.Fatal("cold window was not offered for validation")
	}
	select {
	case names := <-w.entered:
		if !reflect.DeepEqual(names, want) {
			t.Fatal(names)
		}
	case <-time.After(time.Second):
		t.Fatal("cold window did not prefetch after validation")
	}
	close(w.release)
	for range 3 {
		select {
		case <-writes:
		case <-time.After(time.Second):
			t.Fatal("writes did not complete")
		}
	}
}
