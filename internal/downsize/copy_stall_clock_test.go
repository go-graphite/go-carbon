package downsize

import (
	"sync/atomic"
	"testing"
	"time"
)

// stallClock advances only at test events, never because a goroutine was
// descheduled or a catalog/journal operation took longer on a busy machine.
type stallClock struct{ elapsed atomic.Int64 }

func newStallClock() *stallClock              { return &stallClock{} }
func (c *stallClock) now() time.Time          { return time.Unix(1, c.elapsed.Load()) }
func (c *stallClock) advance(d time.Duration) { c.elapsed.Add(int64(d)) }

func TestCopyStallClock(t *testing.T) {
	t.Parallel()
	clock := newStallClock()
	p := newCopyProgress()
	p.now, p.lastSuccess = clock.now, clock.now()
	const budget = 200 * time.Millisecond
	check := func(want time.Duration) {
		t.Helper()
		if got := p.stuck(); got != want {
			t.Fatalf("stuck = %s, want %s", got, want)
		}
	}
	clock.advance(2 * budget)
	check(0) // discovery with no transfers is idle, not stalled
	p.inFlight.Store(1)
	clock.advance(budget - time.Nanosecond)
	check(budget - time.Nanosecond)
	clock.advance(time.Nanosecond)
	check(budget) // a genuinely stuck transfer reaches the exact limit
	p.success()
	check(0)
	p.down("source")
	clock.advance(2 * budget)
	check(0) // host-down time exceeds the budget but is excluded
	p.up("source")
	p.restartWaiting.Store(1)
	clock.advance(2 * budget)
	check(0) // readiness waits with status up are also excluded
	p.restartWaiting.Store(0)
	clock.advance(budget)
	check(budget) // normal stall detection resumes after readiness
}
