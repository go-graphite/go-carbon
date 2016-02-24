package persister

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func doTestThrottleChan(t *testing.T, perSecond int) {
	timestamp := time.Now().Unix()

	chIn := make(chan *points.Points)
	chOut := throttleChan(chIn, perSecond, nil)
	wait := time.After(time.Second)

	bw := 0

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

	max := float64(perSecond) * 1.05
	min := float64(perSecond) * 0.95

	assert.True(t, float64(bw) >= min, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
	assert.True(t, float64(bw) <= max, fmt.Sprintf("perSecond: %d, bw: %d", perSecond, bw))
}

func TestThrottleChan(t *testing.T) {
	perSecondTable := []int{1, 10, 100, 1000, 10000, 100000, 213000}

	if os.Getenv("TRAVIS") != "true" {
		perSecondTable = append(perSecondTable, 531234)
	}

	for _, perSecond := range perSecondTable {
		doTestThrottleChan(t, perSecond)
	}
}
