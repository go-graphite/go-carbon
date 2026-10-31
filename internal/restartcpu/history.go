package restartcpu

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"runtime"
	"sort"
	"time"
)

type sample struct {
	At    time.Time
	Cores float64
}
type history struct {
	Host    string
	Samples []sample // previous median format, retained for migration
	Steady  sample
	Updates float64
}

// SetSteadyRate seeds a known predecessor rate before consumers start.
func (b *Budget) SetSteadyRate(rate float64) {
	if finitePositive(rate) {
		b.steadyRate = rate
	}
}
func (b *Budget) SteadyRate() float64 { return b.steadyRate }
func (b *Budget) Source() string      { return b.source }

// Stop joins the sampler and its final atomic save before the next process can
// reuse the directory. It is safe to repeat. Failure only loses an accelerator.
func (b *Budget) Stop() error {
	if b == nil || b.samplerStop == nil {
		return nil
	}
	b.samplerStop()
	<-b.samplerDone
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.historyErr
}

// Start restores 1.05 times a recent host-local steady EWMA, independent of the
// executable build. Missing history selects a finite conservative allowance.
func Start(ctx context.Context, path string, cores float64) *Budget {
	return startHistory(ctx, path, cores, CPU)
}

func startHistory(ctx context.Context, path string, cores float64, read func() float64) *Budget {
	host, _ := os.Hostname()
	h := history{Host: host}
	if data, err := os.ReadFile(path); err == nil {
		var old history
		if json.Unmarshal(data, &old) == nil && old.Host == host {
			h = old
		}
	}
	now := time.Now()
	fresh := h.Steady.At.After(now.Add(-24*time.Hour)) && !h.Steady.At.After(now) && finitePositive(h.Steady.Cores)
	source := "explicit"
	if cores == 0 {
		source = "history"
		if fresh {
			cores = 1.05 * h.Steady.Cores
		} else {
			cores = 1.05 * median(h.Samples, now)
		}
		if cores == 0 {
			source = "fallback"
			cores = fallbackCores()
		}
	}
	b := NewWithCPU(cores, read)
	b.source = source
	if fresh {
		b.SetSteadyRate(h.Updates)
	}
	ctx, b.samplerStop = context.WithCancel(ctx)
	b.samplerDone = make(chan struct{})
	go func() {
		defer close(b.samplerDone)
		save := func() {
			data, err := json.Marshal(h)
			if err == nil {
				err = saveHistory(path, data)
			}
			b.mu.Lock()
			b.historyErr = err
			b.mu.Unlock()
		}
		defer save()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		last, used, written := time.Now(), read(), b.Updates()
		eligible := false
		for {
			select {
			case at := <-ticker.C:
				next, count := read(), b.Updates()
				drained := b.drained.Load()
				b.mu.Lock()
				meetingFloor := b.floor <= 0 || b.rate >= .95*b.floor
				b.mu.Unlock()
				steady := b.warmed.Load() && meetingFloor && (drained == nil || (*drained)())
				// Both endpoints exclude warming and held restoration. One complete
				// minute is sufficient; the previous implementation needed 31min.
				if eligible && steady {
					value := (next - used) / at.Sub(last).Seconds()
					rate := float64(count-written) / at.Sub(last).Seconds()
					if finitePositive(value) {
						if fresh {
							h.Steady.Cores += .2 * (value - h.Steady.Cores)
							h.Updates += .2 * (rate - h.Updates)
						} else {
							h.Steady.Cores, h.Updates = value, rate
							fresh = true
						}
						h.Steady.At = at
						save()
					}
				}
				eligible = steady
				last, used, written = at, next, count
			case <-ctx.Done():
				return
			}
		}
	}()
	return b
}

func saveHistory(path string, data []byte) error {
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(path+".tmp", path)
	}
	if err != nil {
		_ = os.Remove(path + ".tmp")
	}
	return err
}

func fallbackCores() float64                   { return derivedAllowance(runtime.NumCPU(), runtime.GOMAXPROCS(0)) }
func derivedAllowance(cpus, procs int) float64 { return float64(min(max(1, procs), max(1, cpus/12))) }

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func median(samples []sample, now time.Time) float64 {
	values := make([]float64, 0, 30)
	for _, s := range samples {
		if s.At.After(now.Add(-31*time.Minute)) && !s.At.After(now) && finitePositive(s.Cores) {
			values = append(values, s.Cores)
		}
	}
	if len(values) < 30 {
		return 0
	}
	sort.Float64s(values)
	return (values[14] + values[15]) / 2
}
