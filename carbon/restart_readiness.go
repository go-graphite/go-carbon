package carbon

import "github.com/go-graphite/go-carbon/cache"

// This is a recovery boundary, not a traffic health certificate. A deployment
// must additionally observe a complete coalescing cycle of input/persistence.
// All reads are bounded scalar/channel observations; no lifecycle or shard
// walk can park an intake or metrics handler behind recovery.
func restartRecoveryReady(core *cache.Cache, restored, warmed <-chan struct{}, capture *startupInput) bool {
	if !restartDrainState(core, restored)() {
		return false
	}
	if warmed != nil {
		select {
		case <-warmed:
		default:
			return false
		}
	}
	if capture == nil {
		return true
	}
	return capture.direct.Load() && !capture.fallback.Load() && !capture.paused.Load() &&
		capture.points.Load() == capture.replayedPoints.Load() && capture.size.Load() == capture.retiredBytes.Load()
}
