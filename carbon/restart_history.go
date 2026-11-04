package carbon

import "github.com/go-graphite/go-carbon/cache"

// History follows the current workload, not the intake-ready point count or a
// one-second writeout rate. Normal buffering can exceed that initial count and
// have quiet minutes between writeout phases without being behind.
func restartHistoryState(core *cache.Cache, restored <-chan struct{}) func() (int64, bool) {
	drained := restartDrainState(core, restored)
	return func() (int64, bool) {
		points := core.Size()
		return points, drained() && !core.Backlogged()
	}
}

// Ordinary buffering and growth can exceed the intake-ready point count
// indefinitely. Restart drain tracks restoration and durable confirmations,
// not a snapshot of the live workload's size.
func restartDrainState(core *cache.Cache, restored <-chan struct{}) func() bool {
	return func() bool {
		if restored != nil {
			select {
			case <-restored:
			default:
				return false
			}
		}
		return core.PendingOutstanding() == 0
	}
}
