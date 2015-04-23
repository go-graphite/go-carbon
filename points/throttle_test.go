package points

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThrottleChan(t *testing.T) {
	perSecond := 1000
	timestamp := time.Now().Unix()

	chIn := make(chan *Points)
	chOut := ThrottleChan(chIn, perSecond)
	wait := time.After(time.Second)

	bw := 0

loop:
	for {
		select {
		case <-wait:
			break loop
		default:
		}
		chIn <- OnePoint("metric", 1, timestamp)
		<-chOut
		bw++
	}
	close(chIn)

	max := float64(perSecond) * 1.05
	min := float64(perSecond) * 0.95

	assert.True(t, float64(bw) >= min)
	assert.True(t, float64(bw) <= max)
}
