package persister

import (
	"testing"

	"github.com/go-graphite/go-carbon/helper"
)

func TestStopDrainUsesAutomaticCeiling(t *testing.T) {
	for _, ceiling := range []int{-1, 4000, 0} {
		p := &Whisper{}
		p.liveSettings.Store(&whisperSettings{maxUpdatesPerSecond: -1, updateTicker: helper.NewThrottleTicker(0)})
		p.autoUpdate.ordinary, p.autoUpdate.ceiling = 100, ceiling
		p.autoUpdate.capacity.hardCeiling = 3000
		p.autoUpdate.controller.target = 100
		p.autoUpdate.controller.reason = "steady"
		p.SetStopDrain(true)
		want := ceiling
		if ceiling == -1 {
			want = 3000
		} else if ceiling == 0 {
			want = 100
		}
		if rate, _, _ := p.autoUpdateObservation(); rate != float64(want) {
			t.Fatal(ceiling, rate, want)
		}
		// A later sampler must retain the override without resetting the
		// ordinary controller; ending the drain restores ordinary pacing.
		p.autoUpdate.mu.Lock()
		p.applyAutoUpdateRateLocked()
		p.autoUpdate.mu.Unlock()
		p.SetStopDrain(false)
		if rate, _, _ := p.autoUpdateObservation(); rate != 100 {
			t.Fatal("ordinary rate not restored", rate)
		}
		p.stopAutoUpdateRate()
	}
}

func TestStopDrainPreservesFixedLimit(t *testing.T) {
	p := &Whisper{}
	ticker := helper.NewThrottleTicker(25)
	defer ticker.Stop()
	p.liveSettings.Store(&whisperSettings{maxUpdatesPerSecond: 25, updateTicker: ticker})
	p.SetStopDrain(true)
	if p.settings().maxUpdatesPerSecond != 25 || p.settings().updateTicker != ticker || p.autoUpdate.ticker.Load() != nil {
		t.Fatal("fixed admission changed")
	}
	p.SetStopDrain(false)
}
