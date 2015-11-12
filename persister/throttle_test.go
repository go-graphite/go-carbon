package persister

import (
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func TestThrottleChan(t *testing.T) {
	perSecond := 100
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

	assert.True(t, float64(bw) >= min)
	assert.True(t, float64(bw) <= max)
}
