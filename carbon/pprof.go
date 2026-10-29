package carbon

import (
	"errors"
	"runtime"
)

func validateRuntimeProfiling(cfg *Config) error {
	if cfg.Pprof.MutexProfileFraction < 0 || cfg.Pprof.BlockProfileRate < 0 {
		return errors.New("pprof mutex-profile-fraction and block-profile-rate must not be negative")
	}
	return nil
}

// Profiling is process-wide. Reapply after buckyd starts, since its optional
// Pyroscope setup also changes these rates; [pprof] remains authoritative.
func (app *App) applyRuntimeProfiling() {
	runtime.SetMutexProfileFraction(app.Config.Pprof.MutexProfileFraction)
	runtime.SetBlockProfileRate(app.Config.Pprof.BlockProfileRate)
}
