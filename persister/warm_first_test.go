package persister

import (
	"context"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
	"iter"
	"reflect"
	"slices"
	"strconv"
	"strings"
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
func (*controlledWarmer) MetadataCached(name string) bool {
	return strings.HasPrefix(name, "warm")
}
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
	p.workersCount = 0 // Start the prefetch window before its consumer deterministically.
	p.SetRestartBudget(restartcpu.NewWithCPU(1, func() float64 { return 0 }), nil)
	p.SetMockStore(func() (StoreFunc, func()) { return func(name string) { writes <- name }, nil })
	for cycle := 0; cycle < 2; cycle++ {
		w.entered = make(chan []string, 4)
		source <- "warm"
		source <- "cold-old"
		source <- "cold-new"
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		select {
		case names := <-w.entered:
			if len(names) == 0 || names[0] != "cold-old" {
				t.Fatal(names)
			}
		case <-time.After(time.Second):
			t.Fatal("cold window did not prefetch")
		}
		p.Go(p.worker)
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

func TestWarmFirstKeepsTheNameAfterAFullPrefetchWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const count = store.DefaultPrewarmBatch + 1
		source, writes := make(chan string, count), make(chan string, count)
		for i := range count {
			source <- "cold-" + strconv.Itoa(i)
		}
		warmer := &controlledWarmer{entered: make(chan []string, count), release: make(chan struct{})}
		close(warmer.release)
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
		p.SetMockStore(func() (StoreFunc, func()) { return func(name string) { writes <- name }, nil })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		seen := make(map[string]bool)
		for range count {
			select {
			case name := <-writes:
				if seen[name] {
					t.Fatal("duplicate write", name)
				}
				seen[name] = true
			case <-time.After(time.Second):
				t.Fatalf("prefetch window lost queued work: wrote %d/%d", len(seen), count)
			}
		}
	})
}

func TestWarmFirstReservationsDeduplicateRebuiltQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := make(chan string, 3)
		source <- "warm"
		source <- "warm"
		source <- "warm-other"
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
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		if len(calls) != 2 {
			t.Fatal("rebuilt queue admitted a duplicate pending write", len(calls))
		}
		close(warmer.release)
		time.Sleep(time.Second)
		synctest.Wait()
		if len(calls) != 2 {
			t.Fatal("distinct warm write did not progress", len(calls))
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
	p.workersCount = 0
	p.SetRestartBudget(restartcpu.NewWithCPU(1, func() float64 { return 0 }), nil)
	p.SetMockStore(func() (StoreFunc, func()) { return func(name string) { writes <- name }, nil })
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	var want []string
	select {
	case names := <-w.validated:
		want = names
		if len(names) == 0 || names[0] != "cold-old" || slices.Contains(names, "warm") {
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
	p.Go(p.worker)
	close(w.release)
	for range 3 {
		select {
		case <-writes:
		case <-time.After(time.Second):
			t.Fatal("writes did not complete")
		}
	}
}
