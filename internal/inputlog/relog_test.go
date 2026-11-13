package inputlog

import (
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// relogSource pins every generation (low-water zero, like an item that never
// persists) and records when relog was asked for which generations.
type relogSource struct {
	*fakeSource
	mu   sync.Mutex
	cuts []relogCall
}

type relogCall struct {
	at  time.Time
	gen uint64
}

func (r *relogSource) RelogBefore(gen uint64, _ int) int {
	r.mu.Lock()
	r.cuts = append(r.cuts, relogCall{time.Now(), gen})
	r.mu.Unlock()
	return 0
}

func (r *relogSource) calls() []relogCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.cuts)
}

// Restore pressure runs cycles far faster than SyncInterval. MaxAge is a
// time: generations that are only seconds old must not be relogged, however
// many cycles ran, and must be once MaxAge has passed.
func TestRelogAgeIsWallClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &relogSource{fakeSource: newFakeSource()}
		opts := Options{SyncInterval: time.Second, MaxAge: time.Hour, SegmentSize: 64 << 10}
		l, _, _ := reopen(t, t.TempDir(), opts, src)
		defer l.Close()
		start := time.Now()
		const pressureCycles = 10000 // 2.8 MaxAge of generations at SyncInterval
		for range pressureCycles {
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if calls := src.calls(); len(calls) != 0 {
			t.Fatalf("%d pressure cycles within MaxAge relogged before generation %d", pressureCycles, calls[0].gen)
		}
		// Fake time: the ticker now runs one cycle per SyncInterval. Starts
		// are sampled MaxAge/maxAgeSteps apart, on the next cycle after.
		time.Sleep(opts.MaxAge + opts.MaxAge/maxAgeSteps + opts.SyncInterval)
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
		calls := src.calls()
		if len(calls) == 0 {
			t.Fatal("no relog after MaxAge")
		}
		for _, c := range calls {
			// Points of generations before c.gen were recorded until c.gen
			// started: at start for the pressure cycles, then one ticker
			// cycle per SyncInterval.
			ended := start
			if c.gen > pressureCycles {
				ended = start.Add(time.Duration(c.gen-pressureCycles) * opts.SyncInterval)
			}
			if age := c.at.Sub(ended); age < opts.MaxAge {
				t.Fatalf("relogged generations before %d when their newest points were %v old", c.gen, age)
			}
		}
		if last := calls[len(calls)-1].gen; last <= pressureCycles {
			t.Fatalf("pressure generations not relogged within MaxAge+MaxAge/%d+SyncInterval: cut %d", maxAgeSteps, last)
		}
	})
}
