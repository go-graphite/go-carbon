package cache

import (
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

func TestCarbonlinkNotConfirmed(t *testing.T) {
	assert := assert.New(t)

	cache := New()
	cache.Start()
	defer cache.Stop()

	outChan := cache.Out()

	msg1 := points.OnePoint(
		"hello.world",
		42.17,
		1422797285,
	)

	cache.Add(msg1)

	inFlightMessage := <-outChan
	assert.True(inFlightMessage.Eq(msg1))

	r := NewQuery(msg1.Metric)
	cache.Query() <- r
	<-r.Wait

	if assert.NotNil(r.InFlightData) {
		assert.True(r.InFlightData[0].Eq(msg1))
	}

	// confirm message and try again
	cache.Confirm() <- msg1

	r = NewQuery(msg1.Metric)
	cache.Query() <- r
	<-r.Wait

	assert.Nil(r.InFlightData)
}
