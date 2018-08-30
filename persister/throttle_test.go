package persister

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func doTestThrottleTicker(perSecond int, wait time.Duration, hard bool) (bw int) {
	// start := time.Now()
	w := time.After(wait)

	ticker := newThrottleTicker(perSecond, hard)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-w:
			break LOOP
		case keep := <-ticker.C:
			if keep {
				bw++
			}
		}
	}
	// stop := time.Now()

	return bw
}

func TestThrottleChan(t *testing.T) {
	// perSecondTable := []int{100, 1000, 10000, 100000, 200000, 400000}
	perSecondTable := []int{100, 1000, 10000, 100000, 200000}

	for _, perSecond := range perSecondTable {
		bw := doTestThrottleTicker(perSecond, time.Second, false)
		max := float64(perSecond) * 1.05
		min := float64(perSecond) * 0.95
		assert.True(t, float64(bw) >= min, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
		assert.True(t, float64(bw) <= max, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
	}
}

func TestHardThrottleChan(t *testing.T) {
	perSecondTable := []int{100, 1000, 10000, 100000, 200000}

	for _, perSecond := range perSecondTable {
		// A little less than a second, as the hard throttle front-loads
		// its payloads
		kept := doTestThrottleTicker(perSecond, 900*time.Millisecond, true)
		max := float64(perSecond) * 1.05
		min := float64(perSecond) * 0.95
		assert.True(t, float64(kept) >= min, fmt.Sprintf("perSecond: %d, kept: %d", perSecond, kept))
		assert.True(t, float64(kept) <= max, fmt.Sprintf("perSecond: %d, kept: %d", perSecond, kept))
	}
}
