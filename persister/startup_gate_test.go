package persister

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type startupLogWriter chan string

func (w startupLogWriter) Write(data []byte) (int, error) {
	w <- string(data)
	return len(data), nil
}

func TestStartupGate(t *testing.T) {
	for _, how := range []string{"done", "deadline", "cache", "stop"} {
		t.Run(how, func(t *testing.T) {
			ready, done, exit := make(chan struct{}), make(chan struct{}), make(chan bool)
			p := &Whisper{}
			limit := time.Hour
			if how == "deadline" {
				limit = 10 * time.Millisecond
			}
			p.SetStartupGate(ready, done, limit, func() bool { return how == "cache" })
			result := make(chan bool, 1)
			go func() { result <- p.waitStartup(exit) }()
			// Intake can become ready with the persister still waiting.
			close(ready)
			if how == "done" {
				close(done)
			}
			if how == "stop" {
				close(exit)
			}
			select {
			case ok := <-result:
				if ok != (how != "stop") {
					t.Fatalf("released: %v", ok)
				}
			case <-time.After(time.Second):
				t.Fatal("gate did not release")
			}
			for _, reason := range []string{"done", "deadline", "cache"} {
				var metric dto.Metric
				if err := p.startupReleases.WithLabelValues(reason).Write(&metric); err != nil {
					t.Fatal(err)
				}
				want := float64(0)
				if how == reason {
					want = 1
				}
				if got := metric.GetCounter().GetValue(); got != want {
					t.Fatalf("%s releases: %g, want %g", reason, got, want)
				}
			}
		})
	}
}

func TestStartupGateWorker(t *testing.T) {
	ready, done := make(chan struct{}), make(chan struct{})
	received, writes := make(chan string), make(chan string, 3)
	p := &Whisper{recv: func(exit chan bool) string {
		select {
		case metric := <-received:
			return metric
		case <-exit:
			return ""
		}
	}}
	p.mockStore = func() (StoreFunc, func()) { return func(metric string) { writes <- metric }, nil }
	p.backlogged = func() bool { return true }
	p.SetStartupGate(ready, done, time.Hour, nil)
	p.SetWorkers(1)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	// Ordered restoration must drain while intake is still closed.
	received <- "ordered"
	select {
	case <-writes:
	case <-time.After(time.Second):
		t.Fatal("ordered restore blocked")
	}
	close(ready)
	received <- "pending"
	select {
	case <-writes:
		t.Fatal("wrote before prewarm")
	case <-time.After(10 * time.Millisecond):
	}

	close(done)
	select {
	case got := <-writes:
		if got != "pending" {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("pending write lost")
	}
}

func TestStartupGateStopWhileWaiting(t *testing.T) {
	ready, done := make(chan struct{}), make(chan struct{})
	close(ready)
	arrived := make(chan struct{})
	pending := make(chan string, 1)
	pending <- "unpersisted"
	p := &Whisper{recv: func(exit chan bool) string {
		select {
		case metric := <-pending:
			close(arrived)
			return metric
		case <-exit:
			return ""
		}
	}}
	p.backlogged = func() bool { return true }
	wrote := make(chan struct{}, 1)
	p.mockStore = func() (StoreFunc, func()) { return func(string) { wrote <- struct{}{} }, nil }
	p.SetWorkers(1)
	p.SetStartupGate(ready, done, time.Hour, nil)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(time.Second):
		p.Stop()
		t.Fatal("worker did not receive pending writeout")
	}
	stopped := make(chan struct{})
	go func() { p.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for prewarm deadline")
	}
	select {
	case <-wrote:
		t.Fatal("shutdown released the gate into storage")
	default:
	}
}

func TestStartupGateMetricsAcrossReplacement(t *testing.T) {
	for _, configuredFirst := range []bool{false, true} {
		reg := prometheus.NewPedanticRegistry()
		// Exercise the app's wrapped registerer as well as plain registration.
		var registerer prometheus.Registerer = reg
		prefix := ""
		if configuredFirst {
			prefix = "carbon_"
			registerer = prometheus.WrapRegistererWithPrefix(prefix, reg)
		}
		ready, done := make(chan struct{}), make(chan struct{})
		close(ready)
		close(done)
		for i := 1; i <= 3; i++ {
			p := &Whisper{}
			if configuredFirst {
				p.SetStartupGate(ready, done, time.Second, nil)
			}
			p.InitPrometheus(registerer)
			if !configuredFirst {
				p.SetStartupGate(ready, done, time.Second, nil)
			}
			if !p.waitStartup(make(chan bool)) {
				t.Fatal("completed gate blocked")
			}
			families, err := reg.Gather()
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, family := range families {
				if family.GetName() != prefix+"persister_metadata_prewarm_gate_releases_total" {
					continue
				}
				for _, metric := range family.Metric {
					if metric.Label[0].GetValue() == "done" {
						found = true
						if got := metric.GetCounter().GetValue(); got != float64(i) {
							t.Fatalf("release count %g, want %d", got, i)
						}
					}
				}
			}
			if !found {
				t.Fatal("missing release counter")
			}
		}
	}
}

func TestStartupGateAdaptiveProgress(t *testing.T) {
	for _, how := range []string{"finishes", "stalls", "projected_over_cap", "unknown", "cap"} {
		t.Run(how, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ready, done, exit := make(chan struct{}), make(chan struct{}), make(chan bool)
				close(ready)
				p := &Whisper{startupGrace: time.Second}
				p.SetStartupGate(ready, done, 6*time.Second, nil)
				var scanned atomic.Uint64
				p.SetStartupProgress(func() (uint64, uint64) {
					total := uint64(50)
					if how == "unknown" || how == "cap" || how == "stalls" {
						total = 0
					}
					if how == "projected_over_cap" {
						total = 1000
					}
					return scanned.Load(), total
				})
				started := time.Now()
				result := make(chan bool, 1)
				go func() { result <- p.waitStartup(exit) }()
				if how == "stalls" || how == "projected_over_cap" {
					scanned.Store(1)
				}
				producerDone := make(chan struct{})
				if how == "finishes" || how == "unknown" || how == "cap" || how == "projected_over_cap" {
					go func() {
						defer close(producerDone)
						for i := uint64(1); i <= 70; i++ {
							time.Sleep(100 * time.Millisecond)
							scanned.Store(i)
						}
					}()
				} else {
					close(producerDone)
				}
				if how == "finishes" || how == "unknown" {
					time.Sleep(3500 * time.Millisecond)
					synctest.Wait()
					select {
					case <-result:
						t.Fatal("released a progressing scan at old deadline")
					default:
					}
					close(done)
				}
				if !<-result {
					t.Fatal("gate stopped")
				}
				elapsed := time.Since(started)
				if how == "cap" && elapsed != 6*time.Second {
					t.Fatal("cap not enforced", elapsed)
				}
				if (how == "stalls" || how == "projected_over_cap") && elapsed >= 6*time.Second {
					t.Fatal("projection/stall did not release early", elapsed)
				}
				var metric dto.Metric
				reason := "deadline"
				if how == "finishes" || how == "unknown" {
					reason = "done"
				}
				if err := p.startupReleases.WithLabelValues(reason).Write(&metric); err != nil || metric.GetCounter().GetValue() != 1 {
					t.Fatal("wrong adaptive release reason", reason, &metric, err)
				}
				<-producerDone
			})
		})
	}
}

func TestStartupProjection(t *testing.T) {
	if got := startupProjection(300*time.Second, 80, 92); got != 345*time.Second {
		t.Fatal("projection", got)
	}
	if got := startupProjection(time.Hour, 1, ^uint64(0)); got <= 0 {
		t.Fatal("projection overflow", got)
	}
	if startupProjection(time.Second, 0, 10) != 0 || startupProjection(time.Second, 10, 0) != 0 {
		t.Fatal("unknown projection fabricated a census")
	}
}

func TestStartupGateReleaseBeforeWorkers(t *testing.T) {
	ready, done := make(chan struct{}), make(chan struct{})
	close(ready)
	close(done)
	var resumed atomic.Bool
	p := &Whisper{}
	p.SetStartupGate(ready, done, time.Second, nil)
	p.SetStartupRelease(func(time.Time) { resumed.Store(true) })
	p.SetWorkers(1)
	p.recv = func(exit chan bool) string { <-exit; return "" }
	p.mockStore = func() (StoreFunc, func()) { return func(string) {}, nil }
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	select {
	case <-p.startupGate:
	case <-time.After(time.Second):
		t.Fatal("gate did not open")
	}
	if !resumed.Load() {
		t.Fatal("workers released before cache clock")
	}
}

func TestStartupWriteoutPriorityAtCPUAllowance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		var optional atomic.Int64
		var reducedAt, writeCPU atomic.Int64
		budget := restartcpu.NewWithCPU(2, func() float64 {
			elapsed := time.Since(started).Seconds()
			background := 2 * elapsed
			if at := reducedAt.Load(); at > 0 {
				background = elapsed + float64(at)/1e9
			}
			return background + float64(optional.Load()+writeCPU.Load())/1e6
		})
		optional.Store(1000000)
		ctx, cancel := context.WithCancel(context.Background())
		warming := make(chan struct{})
		go func() {
			defer close(warming)
			for budget.Wait(ctx) == nil {
				optional.Add(1000)
				time.Sleep(time.Millisecond)
			}
		}()
		// Mandatory CPU consumes the whole allowance; add no real CPU load.
		time.Sleep(time.Second)
		ready, done := make(chan struct{}), make(chan struct{})
		close(ready)
		p := &Whisper{recv: func(exit chan bool) string {
			select {
			case <-exit:
				return ""
			default:
				return "metric"
			}
		}}
		p.mockStore = func() (StoreFunc, func()) { return func(string) { writeCPU.Add(5000); p.recordUpdate() }, nil }
		p.SetWorkers(4)
		p.SetMaxUpdatesPerSecond(100) // sufficient headroom for the measured floor
		p.SetRestartBudget(budget, func() float64 { return 100 })
		p.SetStartupGate(ready, done, time.Second, nil)
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		defer func() { cancel(); <-warming }()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if budget.Updates() != 0 {
			t.Fatal("cold writeout bypassed the shared CPU allowance")
		}
		if s := budget.State(); math.IsInf(s.Writeout, 1) || s.Warming+s.Writeout > 2 {
			t.Fatal(s)
		}
		// Input yields CPU; writeout has priority over the optional loader.
		reducedAt.Store(int64(time.Since(started)))
		before := budget.Updates()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if got := budget.Updates() - before; got < 2900 {
			t.Fatal("warming starved the writeout floor", got)
		}
		select {
		case <-warming:
			t.Fatal("warming was not still running at release")
		default:
		}
		cancel()
		<-warming
		budget.Warmed()
		close(done)
		before = budget.Updates()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if got := budget.Updates() - before; got < 2900 {
			t.Fatal("completed warming left writeout starved", got)
		}
		if s := budget.State(); s.Warming != 0 || math.IsInf(s.Writeout, 1) {
			t.Fatal("priority grants", s)
		}
	})
}

func TestStartupWriteoutBelowFloorWarning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := make(startupLogWriter, 4)
		logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(logs), zap.WarnLevel))
		budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
		p := &Whisper{logger: logger, recv: func(exit chan bool) string {
			select {
			case <-exit:
				return ""
			default:
				return "failed"
			}
		}}
		// Attempts do not count as confirmed writes.
		p.mockStore = func() (StoreFunc, func()) { return func(string) {}, nil }
		p.SetWorkers(1)
		p.SetMaxUpdatesPerSecond(100)
		p.SetRestartBudget(budget, func() float64 { return 100 })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		defer p.Stop()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		select {
		case line := <-logs:
			t.Fatal("warned before more than 10s below floor", line)
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case line := <-logs:
			if !strings.Contains(line, "restart writeout below floor") {
				t.Fatal(line)
			}
		default:
			t.Fatal("missing writeout floor warning")
		}

	})
}

func TestStartupWriteoutFloorCannotRaiseConfiguredCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var floor atomic.Int64
		floor.Store(20)
		budget := restartcpu.NewWithCPU(2, func() float64 { return 0 })
		p := &Whisper{recv: func(exit chan bool) string {
			select {
			case <-exit:
				return ""
			default:
				return "metric"
			}
		}}
		p.mockStore = func() (StoreFunc, func()) { return func(string) { p.recordUpdate() }, nil }
		p.SetWorkers(2)
		p.SetMaxUpdatesPerSecond(10)
		p.SetRestartBudget(budget, func() float64 { return float64(floor.Load()) })
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		defer p.Stop()
		if budget.Updates() < 8 || budget.Updates() > 12 {
			t.Fatal("configured cap was not preserved", budget.Updates())
		}
		floor.Store(100)
		before := budget.Updates()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if delta := budget.Updates() - before; delta < 28 || delta > 32 {
			t.Fatal("backlog increased its own cap", delta)
		}
		floor.Store(200)
		stopped := make(chan struct{})
		go func() { p.Stop(); close(stopped) }()
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case <-stopped:
		default:
			t.Fatal("rate replacement blocked shutdown")
		}
	})
}

func TestStartupWriteoutBeforeReadyWithoutGate(t *testing.T) {
	ready := make(chan struct{})
	received, writes := make(chan string, 1), make(chan string, 1)
	received <- "ordered"
	p := &Whisper{recv: func(exit chan bool) string {
		select {
		case metric := <-received:
			return metric
		case <-exit:
			return ""
		}
	}}
	p.mockStore = func() (StoreFunc, func()) { return func(name string) { writes <- name }, nil }
	p.backlogged = func() bool { return true }
	p.SetStartupGate(ready, make(chan struct{}), 0, nil)
	p.SetRestartBudget(restartcpu.New(1e-9), func() float64 { return 1 })
	p.SetWorkers(1)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	select {
	case <-writes:
	case <-time.After(time.Second):
		t.Fatal("urgent ordered restore blocked")
	}
}

// A checkpoint's byte projection can be hours while the cache is nearly empty.
// It must not release writeout at the catalog scan's grace deadline.
func TestStartupGateSlowCheckpoint(t *testing.T) {
	for _, finish := range []bool{false, true} {
		t.Run(fmt.Sprint(finish), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ready, done, exit := make(chan struct{}), make(chan struct{}), make(chan bool)
				close(ready)
				p := &Whisper{startupGrace: 30 * time.Second}
				p.SetStartupGate(ready, done, 480*time.Second, nil)
				p.SetStartupProgress(func() (uint64, uint64) { return 500000, 32700000000 })
				p.SetStartupCheckpointLoading(func() bool { return true })
				released := make(chan bool, 1)
				go func() { released <- p.waitStartup(exit) }()
				time.Sleep(31 * time.Second)
				synctest.Wait()
				select {
				case <-released:
					t.Fatal("slow checkpoint released a cold cache at grace")
				default:
				}
				if finish {
					close(done)
				} else {
					time.Sleep(449 * time.Second)
				}
				if !<-released {
					t.Fatal("checkpoint gate stopped")
				}
			})
		})
	}
}
