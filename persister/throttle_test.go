package persister

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func doTestThrottleTicker(perSecond int) (bw int) {
	// start := time.Now()
	wait := time.After(time.Second)

	ticker := NewThrottleTicker(perSecond)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-wait:
			break LOOP
		case <-ticker.C:
			bw++
		}
	}
	// stop := time.Now()

	return bw
}

func TestThrottleChan(t *testing.T) {
	// perSecondTable := []int{100, 1000, 10000, 100000, 200000, 400000}
	perSecondTable := []int{100, 1000, 10000, 100000, 200000}

	for _, perSecond := range perSecondTable {
		bw := doTestThrottleTicker(perSecond)
		max := float64(perSecond) * 1.05
		min := float64(perSecond) * 0.95
		assert.True(t, float64(bw) >= min, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
		assert.True(t, float64(bw) <= max, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
	}
}
