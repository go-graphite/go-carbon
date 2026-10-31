package carbon

import "github.com/go-graphite/go-carbon/cache"

// History follows the current workload, not the intake-ready point count or a
// one-second writeout rate. Normal buffering can exceed that initial count and
// have quiet minutes between writeout phases without being behind.
func restartHistoryState(core *cache.Cache, restored <-chan struct{}) func() (int64, bool) {
	return func() (int64, bool) {
		points := core.Size()
		if restored != nil {
			select {
			case <-restored:
			default:
				return points, false
			}
		}
		return points, core.PendingOutstanding() == 0 && !core.Backlogged()
	}
}
