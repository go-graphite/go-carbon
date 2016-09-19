package points

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/lomik/go-whisper"
	"github.com/stretchr/testify/assert"
)

func TestParseText(t *testing.T) {

	assertError := func(line string) {
		m, err := ParseText(line)
		if err == nil {
			t.Fatalf("Bad message parsed without error: %#v", line)
			return
		}
		if (m != SinglePoint{}) {
			t.Fatalf("Wrong message %#v != nil", m)
			return
		}
	}

	assertOk := func(line string, expected SinglePoint) {
		p, err := ParseText(line)

		if err != nil {
			t.Fatalf("Normal message not parsed: %#v", line)
			return
		}

		if expected != p {
			t.Fatalf("%#v != %#v", expected, p)
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
		SinglePoint{"metric.name", Point{Value: -42.76, Timestamp: 1422642189}})

	assertOk("metric.name 42.15 1422642189\n",
		SinglePoint{"metric.name", Point{Value: 42.15, Timestamp: 1422642189}})

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

	other := []*Points{
		nil,
		OnePoint("other.metric", 42.15, timestamp),
		OnePoint("metric.name", 42.16, timestamp),
		OnePoint("metric.name", 42.15, timestamp+1),
		New(),
		&Points{
			Metric: "metric.name",
		},
		points.copy().Append(Point{
			Value:     42.15,
			Timestamp: int(timestamp),
		}),
	}

	for i := 0; i < len(other); i++ {
		assert.False(
			points.Eq(
				other[i],
			), "%v.Eq(%v) == True, but should be false",
			points,
			other[i],
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

type testcase struct {
	description string
	input       []byte
	output      []*Points
}

var (
	goodPickles = []testcase{
		// [("param1", (1423931224, 60.2))]
		testcase{"One metric with one datapoint",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na."),
			[]*Points{OnePoint("param1", 60.2, 1423931224)},
		},
		// [("param1", (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))]
		testcase{"One metric with multiple datapoints",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I14239" +
				"31225\nF50.2\ntp3\n(I1423931226\nF40.2\ntp4\ntp5\na."),
			[]*Points{OnePoint("param1", 60.2, 1423931224).Add(50.2, 1423931225).Add(40.2, 1423931226)}},
		// [("param1", (1423931224, 60.2)), ("param2", (1423931224, -15))]
		testcase{"Multiple metrics with single datapoints",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na(S'param2" +
				"'\np4\n(I1423931224\nI-15\ntp5\ntp6\na."),
			[]*Points{
				OnePoint("param1", 60.2, 1423931224),
				OnePoint("param2", -15, 1423931224),
			}},
		// [("param1", (1423931224, 60.2), (1423931284, 42)), ("param2", (1423931224, -15))]
		testcase{"Complex update",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2" +
				"\n(I1423931284\nI42\ntp3\ntp4\na(S'param2'\np5\n(I1423931224\nI-15\ntp6" +
				"\ntp7\na."),
			[]*Points{
				OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
				OnePoint("param2", -15, 1423931224),
			},
		},
	}

	badPickles = [][]byte{
		// #0 empty
		[]byte(""),
		// #1 incorrect numper of elements
		[]byte("(lp0\n(S'param1'\np1\ntp2\na."),
		// #2 too few elements in a datapoint
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931284\ntp" +
			"3\ntp4\na."),
		// #3 too many elements in a datapoint
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\nI3\ntp2\ntp3\na."),
		// #4 negative timestamp in a datapoint
		[]byte("(lp0\n(S'param1'\np1\n(I-1423931224\nI60\ntp2\ntp3\na."),
		// #5 timestamp too big for uint32
		[]byte("(lp0\n(S'param1'\np1\n(I4294967296\nF60.2\ntp2\ntp3\na."),
	}
)

func TestParsePickle(t *testing.T) {
	for _, tc := range goodPickles {
		output, err := ParsePickle(tc.input)
		assert.Nil(t, err)
		assert.Equal(t, tc.output, output, fmt.Sprintf("failed while parsing: '%s'", tc.description))
	}

	for casenum, input := range badPickles {
		_, err := ParsePickle(input)
		assert.Error(t, err, fmt.Sprintf("bad pickle #%d failed to raise and error", casenum))
	}

}

func TestPointCompatWithWhisperTimeseriesPoint(t *testing.T) {
	var w whisper.TimeSeriesPoint
	var p Point
	assert.Equal(t, unsafe.Offsetof(w.Time), unsafe.Offsetof(p.Timestamp))
	assert.Equal(t, unsafe.Sizeof(w.Time), unsafe.Sizeof(p.Timestamp))
	assert.Equal(t, unsafe.Offsetof(w.Value), unsafe.Offsetof(p.Value))
	assert.Equal(t, unsafe.Sizeof(w.Value), unsafe.Sizeof(p.Value))
}

func TestPointShift(t *testing.T) {
	ref := OnePoint("p", 10, 9000)
	assert.Equal(t, ref, OnePoint("p", 10, 9000).Shift(0), "Failed shifting 0 points")
	assert.Equal(t, ref, OnePoint("p", 10, 9000).Shift(-1), "Failed shifting -1 points")
	assert.Equal(t, ref, OnePoint("p", 5, 5000).Add(10, 9000).Shift(1), "Failed shifting 1 points")
	assert.Equal(t, &Points{Metric: "p", Data: make([]Point, 0)}, OnePoint("p", 10, 9000).Shift(1), "Failed shifting all points")
	assert.Equal(t, &Points{Metric: "p", Data: make([]Point, 0)}, OnePoint("p", 10, 9000).Add(20, 9020).Shift(999), "Failed shifting more than all  points")
}

func BenchmarkParsePickle(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		_, err := ParsePickle(goodPickles[n%len(goodPickles)].input)
		if err != nil {
			b.Fatalf("Error raised while benchmarking")
		}
	}
}
