package carbon

import (
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

func TestPhaseCPUConfigAndLifecycle(t *testing.T) {
	cfg := NewConfig()
	if phaseCPUInterval(cfg) != 0 {
		t.Fatal("phase CPU profiling must default off")
	}
	for _, d := range []time.Duration{-time.Second, time.Second, 0, 10 * time.Second, time.Minute} {
		cfg.Common.PhaseCPUProfileInterval = &Duration{d}
		err := validatePhaseCPU(cfg)
		if (err == nil) != (d == 0 || d >= phasecpu.MinInterval) {
			t.Fatalf("interval %s: %v", d, err)
		}
	}
	cfg.Common.PhaseCPUProfileInterval = &Duration{30 * time.Second}
	cfg.Common.PhaseCPUProfileDuration = &Duration{500 * time.Millisecond}
	if err := validatePhaseCPU(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Common.PhaseCPUProfileDuration = &Duration{time.Second}
	if err := validatePhaseCPU(cfg); err == nil {
		t.Fatal("excessive duty allowed")
	}

	path, cfg := sharedAppConfig(t)
	cfg.Common.PhaseCPUProfileInterval = &Duration{time.Minute}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	if app.phaseProfiler == nil {
		t.Fatal("profiler did not start")
	}
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "go_carbon_phase_cpu_seconds_total" {
			found = true
			if len(f.Metric) != 28 {
				t.Fatalf("unexpected phase cardinality %d", len(f.Metric))
			}
		}
	}
	if !found {
		t.Fatal("phase CPU collector missing")
	}
	cfg.Common.PhaseCPUProfileInterval = &Duration{0}
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("reload changed profiler ownership")
	}
	app.Stop()
	if app.phaseProfiler != nil {
		t.Fatal("profiler survived stop")
	}
}
