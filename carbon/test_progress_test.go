package carbon

import (
	"testing"
	"time"

	"github.com/lomik/zapwriter"
)

// Test timeouts detect stalled work, not a slow scheduler or disk. Observe
// progress before checking the idle bound, so a descheduled waiter does not
// mistake completed work for a timeout. The package timeout bounds deadlocks.
func awaitTestProgress(t testing.TB, label string, done func() bool, progress func() any) {
	t.Helper()
	last, changed := progress(), time.Now()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if done() {
			return
		}
		if current := progress(); current != last {
			last, changed = current, time.Now()
		} else if time.Since(changed) > 30*time.Second {
			t.Fatalf("%s: no progress for 30s (last %v)", label, last)
		}
		<-tick.C
	}
}

func testLogProgress() any { return len(zapwriter.TestString()) }

func awaitTestResult[T any](t testing.TB, label string, ch <-chan T, progress func() any) T {
	t.Helper()
	last, changed := progress(), time.Now()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case result := <-ch:
			return result
		case <-tick.C:
			// Prefer a concurrently completed result to the idle check.
			select {
			case result := <-ch:
				return result
			default:
			}
			if current := progress(); current != last {
				last, changed = current, time.Now()
			} else if time.Since(changed) > 30*time.Second {
				t.Fatalf("%s: no progress for 30s (last %v)", label, last)
			}
		}
	}
}
