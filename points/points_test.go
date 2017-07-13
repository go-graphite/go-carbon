package points

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCopyAndEq(t *testing.T) {
	assert := assert.New(t)

	timestamp := time.Now().Unix()

	points := OnePoint("metric.name", 42.15, timestamp)

	assert.True(
		points.Eq(
			OnePoint("metric.name", 42.15, timestamp),
		),
	)

	assert.True(
		points.Eq(
			points.Copy(),
		),
	)

	other := []*Points{
		nil,
		OnePoint("other.metric", 42.15, timestamp),
		OnePoint("metric.name", 42.16, timestamp),
		OnePoint("metric.name", 42.15, timestamp+1),
		New(),
		&Points{
			Metric: "metric.name",
		},
		points.Copy().Append(Point{
			Value:     42.15,
			Timestamp: timestamp,
		}),
	}

	for i := 0; i < len(other); i++ {
		assert.False(
			points.Eq(
				other[i],
			),
		)
	}

	// Eq of empty points
	assert.True((&Points{
		Metric: "metric.name",
	}).Eq(
		&Points{
			Metric: "metric.name",
		},
	),
	)

	assert.False((&Points{
		Metric: "metric.name",
	}).Eq(
		&Points{
			Metric: "other.metric",
		},
	),
	)
}
