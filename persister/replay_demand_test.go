package persister

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/cache"
)

func TestAutomaticRateFundsCaptureReplay(t *testing.T) {
	for _, ceiling := range []int{-1, 0, 100000, 200000} {
		t.Run(fmt.Sprint(ceiling), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &Whisper{}
				p.SetMaxUpdatesPerSecond(-1)
				defer p.stopAutoUpdateRate()
				s := cache.WriteoutState{Metrics: 19000000, Points: 20000000000, Eligible: 19000000, MeanDelay: 30 * time.Minute, MaxDelay: 30 * time.Minute}
				p.autoUpdate.floor, p.autoUpdate.ceiling = 75000, ceiling
				p.sampleAutoUpdateRateLocked(s)
				time.Sleep(5 * time.Second)
				s.ReplayPoints = 80000000 // 16M points/s; no direct receiver input.
				p.writeStats.shared.observe(10000000, time.Second, nil)
				// 75k updates/s at about 133 points per update.
				p.writeStats.shared.updates.Store(75000)
				p.sampleAutoUpdateRateLocked(s)
				want := 144000
				if ceiling > 0 {
					want = min(want, ceiling)
				}
				if got := p.autoUpdate.ordinary; got < want || ceiling > 0 && got > ceiling {
					t.Fatalf("replay demand: rate=%d want>=%d ceiling=%d", got, want, ceiling)
				}
			})
		})
	}
}
