package cache

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestAutoWriteoutKeepsPhasesAfterEarlyWrite(t *testing.T) {
	for _, restored := range []bool{false, true} {
		t.Run(fmt.Sprint(restored), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const delay = 960 * time.Second
				c := batchingCache(t, 960, delay)
				c.SetMaxSize(0)
				c.SetWriteoutPacing(true)
				c.SetWriteoutRestoreSpread(true)
				c.SetWriteoutSparseSpread(true)
				c.EnableInputLog()
				if err := c.SetWriteoutSpread(.25); err != nil {
					t.Fatal(err)
				}
				names := make([]string, 10000)
				deadlines := make([]time.Time, len(names))
				var bins [16]int
				for i := range names {
					names[i] = fmt.Sprintf("phase.metric.%08d", i)
					p := points.OnePoint(names[i], 1, time.Now().Unix())
					if restored {
						p.FirstArrival = time.Now().UnixNano()
						c.AddRestored(p)
					} else {
						c.Add(p)
					}
					deadlines[i] = deadlineOf(c, names[i])
					left := deadlines[i].Sub(time.Now())
					if left <= 0 || left > delay {
						t.Fatalf("deadline outside first window: %s", left)
					}
					bins[min(int(left/time.Minute), len(bins)-1)]++
				}
				for _, n := range bins {
					if n < 500 || n > 750 {
						t.Fatalf("eligibility not uniform: %v", bins)
					}
				}
				time.Sleep(120 * time.Second)
				for _, name := range names {
					if _, ok := c.Pop(name); !ok {
						t.Fatal("missing early batch")
					}
				}
				time.Sleep(time.Minute)
				for i, name := range names {
					c.AddPoint([]byte(name), 1, time.Now().Unix())
					next := deadlineOf(c, name)
					if next.Sub(deadlines[i])%delay != 0 {
						t.Fatal("early write moved the phase")
					}
					if !next.After(time.Now()) || next.After(time.Now().Add(delay)) {
						t.Fatal("next phase exceeded the age window")
					}
				}
				// Sampling uses real residence, not a synthesized eligibility arrival.
				if state := c.WriteoutState(); state.OldestAge != 0 || state.MeanDelay != delay {
					t.Fatalf("state: %+v", state)
				}
			})
		})
	}
}

func TestAutoWriteoutStartupKeepsClockAndThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := batchingCache(t, 2, time.Minute)
		c.SetWriteoutPacing(true)
		c.PauseStartupWriteout()
		c.AddPoint([]byte("held.phase"), 1, time.Now().Unix())
		at := deadlineOf(c, "held.phase")
		time.Sleep(time.Hour)
		c.ResumeStartupWriteout(time.Now())
		if got := deadlineOf(c, "held.phase"); !got.Equal(at) {
			t.Fatalf("release moved deadline from %v to %v", at, got)
		}
		if !c.WriteoutReady("held.phase") {
			t.Fatal("startup reset an expired deadline")
		}
		c.AddPoint([]byte("full.batch"), 1, 1)
		c.AddPoint([]byte("full.batch"), 2, 2)
		if !c.WriteoutReady("full.batch") {
			t.Fatal("startup suppressed full-batch threshold")
		}
	})
}

func TestAutoWriteoutFixedModeDeadline(t *testing.T) {
	c := batchingCache(t, 960, 960*time.Second)
	if err := c.SetWriteoutSpread(.25); err != nil {
		t.Fatal(err)
	}
	c.AddPoint([]byte("fixed.phase"), 1, 1)
	before := deadlineOf(c, "fixed.phase")
	c.SetWriteoutPacing(false)
	if after := deadlineOf(c, "fixed.phase"); !before.Equal(after) {
		t.Fatal("fixed mode changed relative deadline")
	}
}

// With relative spread, restore phase and period are affine functions of the
// same hash. At spread=.25 four periods cancel the initial phase exactly.
func TestRelativeRestoreSpreadRefocuses(t *testing.T) {
	s := &cacheSettings{writeoutMinPoints: 960, writeoutMaxDelay: 960 * time.Second, writeoutSpread: .25}
	start := time.Now()
	firstMin, firstMax := start.Add(time.Hour), start
	fourthMin, fourthMax := start.Add(2*time.Hour), start
	for i := range 10000 {
		it := newItem(fmt.Sprintf("restore.refocus.%d", i), 0)
		it.firstArrival = s.restoreArrival(it, start)
		first := writeoutDeadline(it, s)
		fourth := first.Add(4 * s.delayFor(it))
		if first.Before(firstMin) {
			firstMin = first
		}
		if first.After(firstMax) {
			firstMax = first
		}
		if fourth.Before(fourthMin) {
			fourthMin = fourth
		}
		if fourth.After(fourthMax) {
			fourthMax = fourth
		}
	}
	if firstMax.Sub(firstMin) < 15*time.Minute || fourthMax.Sub(fourthMin) > time.Microsecond {
		t.Fatalf("initial spread %s, after four periods %s", firstMax.Sub(firstMin), fourthMax.Sub(fourthMin))
	}
	t.Logf("initial spread %s refocuses to %s after four periods", firstMax.Sub(firstMin), fourthMax.Sub(fourthMin))
}
