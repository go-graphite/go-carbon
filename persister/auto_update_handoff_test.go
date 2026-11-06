package persister

import (
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
)

func TestAutoUpdateGrowthDuringDeferredHandoff(t *testing.T) {
	// A saved checkpoint is still outstanding after the startup clock releases.
	// Restored residence age starts over, but this is not an empty cache filling.
	for _, initiallyEmpty := range []bool{false, true} {
		t.Run(map[bool]string{false: "attached_before_start", true: "attached_after_start"}[initiallyEmpty], func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			c := autoUpdateController{}
			if initiallyEmpty {
				c.step(now.Add(-autoUpdateInterval), cache.WriteoutState{MaxDelay: 30 * time.Minute}, 1, 0)
			}
			s := cache.WriteoutState{
				Metrics: 21_600_000, Points: 6_600_000_000, MaxPoints: 40_000_000_000,
				MaxDelay: 30 * time.Minute, MeanDelay: 30 * time.Minute, Restored: true,
			}
			before := c.step(now, s, 1, 0)
			// Poll at the real five-second cadence: smooth growth must not
			// become a new equilibrium at each sample. Stay below pressure
			// and before the predecessor backlog's first reset deadline.
			var rate int
			for second := 5; second <= 600; second += 5 {
				s.Points += 2_500_000 * 5
				s.OldestAge = time.Duration(second) * time.Second
				rate = c.step(now.Add(s.OldestAge), s, 1, 0)
			}
			t.Logf("reset oldest_age=%s initial=%d/s after_growth=%d/s reason=%s", s.OldestAge, before, rate, c.reason)
			if rate <= before || c.reason != "growth" {
				t.Fatal("handoff suppressed backlog growth for max delay", before, rate, c.reason)
			}
			// Retiring the last saved source must not reinstate the cold-fill guard.
			s.Restored = false
			s.Points *= 2
			c.step(now.Add(11*time.Minute), s, 1, 0)
			if c.reason != "growth" {
				t.Fatal("source retirement restarted the fill grace period", c.reason)
			}
		})
	}
}

func TestAutoUpdateRestoredBaselineStartsAtRelease(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	c := autoUpdateController{}
	s := cache.WriteoutState{Metrics: 21_600_000, Points: 6_600_000_000,
		MaxDelay: 30 * time.Minute, MeanDelay: 30 * time.Minute, Restored: true, Recovering: true}
	c.step(now, s, 1, 0)
	if !c.restoredAt.IsZero() {
		t.Fatal("held restoration started the growth window")
	}
	// Warming can outlast the controller's original empty-fill grace period.
	released := now.Add(40 * time.Minute)
	s.Recovering = false
	before := c.step(released, s, 1, 0)
	for second := 5; second <= 600; second += 5 {
		s.Points += 2_500_000 * 5
		s.OldestAge = time.Duration(second) * time.Second
		c.step(released.Add(s.OldestAge), s, 1, 0)
	}
	if c.rate <= float64(before) || c.reason != "growth" {
		t.Fatal("long warmup consumed the restored growth window", c.rate, c.reason)
	}
	// The special baseline is bounded; ordinary recent-peak tracking resumes.
	c.step(released.Add(s.MaxDelay), s, 1, 0)
	if c.equilibrium != float64(s.Points) {
		t.Fatal("restored baseline persisted beyond its first window", c.equilibrium)
	}
}

func TestAutoUpdateEmptyStartupStillAllowsNormalFill(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	c := autoUpdateController{}
	s := cache.WriteoutState{MaxDelay: 30 * time.Minute, MeanDelay: 30 * time.Minute}
	c.step(now, s, 1, 0)
	for minute := 1; minute <= 29; minute++ {
		s.Points = int64(minute) * 200_000_000
		s.Metrics = 21_600_000
		c.step(now.Add(time.Duration(minute)*time.Minute), s, 1, 0)
		if c.reason != "steady" {
			t.Fatal("normal empty-start filling was treated as overload", minute, c.reason)
		}
	}
}
