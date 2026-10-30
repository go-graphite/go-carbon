package carbon

import (
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

func phaseCPUInterval(cfg *Config) time.Duration {
	if cfg.Common.PhaseCPUProfileInterval == nil {
		return 0
	}
	return cfg.Common.PhaseCPUProfileInterval.Value()
}

func phaseCPUWindow(cfg *Config) time.Duration {
	if cfg.Common.PhaseCPUProfileDuration == nil {
		return phasecpu.Window
	}
	return cfg.Common.PhaseCPUProfileDuration.Value()
}

func validatePhaseCPU(cfg *Config) error {
	interval := phaseCPUInterval(cfg)
	if interval == 0 {
		return nil
	}
	return phasecpu.Validate(interval, phaseCPUWindow(cfg))
}

func (app *App) startPhaseCPU() error {
	if err := validatePhaseCPU(app.Config); err != nil {
		return err
	}
	interval := phaseCPUInterval(app.Config)
	if interval == 0 {
		return nil
	}
	p, err := phasecpu.StartWithDuration(interval, phaseCPUWindow(app.Config))
	if err != nil {
		return err
	}
	if err = app.PromRegisterer.Register(p); err != nil {
		p.Stop()
		return err
	}
	app.phaseProfiler, app.phaseInterval, app.phaseWindow = p, interval, phaseCPUWindow(app.Config)
	return nil
}

// reloadPhaseCPU applies changed sampler settings on SIGHUP. Phase labels are
// always set, so a sampler started by a reload attributes CPU from its first
// window; its counters start again from zero.
func (app *App) reloadPhaseCPU() error {
	interval, window := phaseCPUInterval(app.Config), phaseCPUWindow(app.Config)
	if app.phaseProfiler == nil && interval == 0 ||
		app.phaseProfiler != nil && interval == app.phaseInterval && window == app.phaseWindow {
		return nil
	}
	app.stopPhaseCPU()
	return app.startPhaseCPU()
}

func (app *App) stopPhaseCPU() {
	if app.phaseProfiler == nil {
		return
	}
	app.phaseProfiler.Stop()
	app.PromRegisterer.Unregister(app.phaseProfiler)
	app.phaseProfiler = nil
}
