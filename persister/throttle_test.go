package persister

import (
	"fmt"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func doTestThrottleChan(perSecond int) (bw int) {
	timestamp := time.Now().Unix()

	chIn := make(chan *points.Points)
	chOut := ThrottleChan(chIn, perSecond, nil)
	wait := time.After(time.Second)

LOOP:
	for {
		select {
		case <-wait:
			break LOOP
		default:
		}
		chIn <- points.OnePoint("metric", 1, timestamp)
		<-chOut
		bw++
	}
	close(chIn)

	return bw
}

func TestThrottleChan(t *testing.T) {
	perSecondTable := []int{1, 10, 100, 1000, 10000, 100000, 200000, 400000, 800000, 1600000, 3200000}

	bwMax := doTestThrottleChan(100000000000)

	for _, perSecond := range perSecondTable {
		if float64(perSecond) > float64(bwMax)*float64(0.9) {
			fmt.Printf("Maximum bandwidth is %d. Skipping %d and higher.\n", bwMax, perSecond)
			break
		}
		bw := doTestThrottleChan(perSecond)
		max := float64(perSecond) * 1.05
		min := float64(perSecond) * 0.95
		assert.True(t, float64(bw) >= min, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
		assert.True(t, float64(bw) <= max, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
	}
}
