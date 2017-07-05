package points

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseText(t *testing.T) {

	assertError := func(line string) {
		m, err := ParseText(line)
		if err == nil {
			t.Fatalf("Bad message parsed without error: %#v", line)
			return
		}
		if m != nil {
			t.Fatalf("Wrong message %#v != nil", m)
			return
		}
	}

	assertOk := func(line string, points *Points) {
		p, err := ParseText(line)

		if err != nil {
			t.Fatalf("Normal message not parsed: %#v", line)
			return
		}

		if !points.Eq(p) {
			t.Fatalf("%#v != %#v", p, points)
			return
		}
	}

	assertError("42")
	assertError("")
	assertError("\n")
	assertError("metric..name 42 \n")

	assertError("metric.name 42 a1422642189\n")

	// assertError("metric..name 42 1422642189\n")
	// assertError("metric.name.. 42 1422642189\n")
	// assertError("metric..name 42 1422642189")
	assertError("metric.name 42a 1422642189\n")
	// assertError("metric.name 42 10\n")

	// assertError("metric.name 42 4102545300\n")

	assertError("metric.name NaN 1422642189\n")
	assertError("metric.name 42 NaN\n")

	assertOk("metric.name -42.76 1422642189\n",
		OnePoint("metric.name", -42.76, 1422642189))

	assertOk("metric.name 42.15 1422642189\n",
		OnePoint("metric.name", 42.15, 1422642189))

	assertOk("metric.name 1.003392e+06 1422642189\n",
		OnePoint("metric.name", 1003392.0, 1422642189))

}

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
