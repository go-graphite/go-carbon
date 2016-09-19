package helper

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func doRatelimitTest(t *testing.T, perSecond int) {
	lim := NewRatelimiter()
	wait := time.After(time.Second)
	bw := 0

LOOP:
	for {
		select {
		case <-wait:
			break LOOP
		default:
			lim.TickSleep(perSecond)
			bw++
		}
	}

	max := float64(perSecond)*1.05 + 1
	min := float64(perSecond)*0.95 - 1

	assert.True(t, float64(bw) >= min, "perSecond: %d, bw: %d", perSecond, bw)
	assert.True(t, float64(bw) <= max, "perSecond: %d, bw: %d", perSecond, bw)
}

func TestRatelimit(t *testing.T) {
	perSecondTable := []int{1, 10, 100, 1000, 10000, 100000, 213000}

	if os.Getenv("TRAVIS") != "true" {
		perSecondTable = append(perSecondTable, 531234)
	}

	for _, perSecond := range perSecondTable {
		doRatelimitTest(t, perSecond)
	}
}
