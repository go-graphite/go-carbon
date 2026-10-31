package cache

import (
	"sync/atomic"
	"time"
)

// startupWriteout freezes batching time while metadata warms. Arrivals and
// deadlines use the same clock, so release needs no per-metric rescheduling.
type startupWriteout struct {
	at       time.Time
	released atomic.Pointer[time.Time]
}

// PauseStartupWriteout is set before starting persister workers. Held batching
// deadlines span the full max delay from release, independently of the ordinary
// spread settings. Urgent restoration is still eligible before intake.
func (c *Cache) PauseStartupWriteout() {
	s := *c.settings.Load().(*cacheSettings)
	if s.startupWriteout != nil {
		return
	}
	s.startupWriteout = &startupWriteout{at: time.Now()}
	c.settings.Store(&s)
}

// ResumeStartupWriteout starts the held window once, before opening the gate.
// Pending points retain their ordinary input-log/checkpoint ownership.
func (c *Cache) ResumeStartupWriteout(at time.Time) {
	s := c.settings.Load().(*cacheSettings)
	if clock := s.startupWriteout; clock != nil && clock.released.CompareAndSwap(nil, &at) {
		c.writeoutQueue.notifyAt(time.Now())
	}
}

// Capture admission before release together with its clock, even if release
// races with assigning the item's first window.
func (s *cacheSettings) writeoutTime(now time.Time) (time.Time, bool) {
	if clock := s.startupWriteout; clock != nil {
		if released := clock.released.Load(); released != nil {
			return now.Add(-released.Sub(clock.at)), false
		}
		return clock.at, true
	}
	return now, false
}

func (s *cacheSettings) writeoutWall(at time.Time) time.Time {
	if clock := s.startupWriteout; clock != nil && !at.IsZero() {
		if released := clock.released.Load(); released != nil {
			return at.Add(released.Sub(clock.at))
		}
		return clock.at.Add(time.Duration(1<<63 - 1))
	}
	return at
}
