package tcp

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestPickle(t *testing.T) {
	// > python
	// >>> import pickle, struct
	// >>> listOfMetricTuples = [("hello.world", (1452200952, 42))]
	// >>> payload = pickle.dumps(listOfMetricTuples, protocol=2)
	// >>> header = struct.pack("!L", len(payload))
	// >>> message = header + payload
	// >>> print repr(message)
	// '\x00\x00\x00#\x80\x02]q\x00U\x0bhello.worldq\x01J\xf8\xd3\x8eVK*\x86q\x02\x86q\x03a.'

	test := newTCPTestCase(t, true)
	defer test.Finish()

	test.Send("\x00\x00\x00#\x80\x02]q\x00U\x0bhello.worldq\x01J\xf8\xd3\x8eVK*\x86q\x02\x86q\x03a.")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42, 1452200952))
	default:
		t.Fatalf("Message #0 not received")
	}
}

func TestBadPickle(t *testing.T) {
	defer zapwriter.Test()()

	assert := assert.New(t)
	test := newTCPTestCase(t, true)
	defer test.Finish()

	test.Send("\x00\x00\x00#\x80\x02]q\x00q\x0bhello.worldq\x01Rixf8\xd3\x8eVK*\x86q\x02\x86q\x03a.")
	time.Sleep(10 * time.Millisecond)
	assert.Contains(zapwriter.TestString(), "can't unpickle message")
}

// https://github.com/lomik/go-carbon/issues/30
func TestPickleMemoryError(t *testing.T) {
	defer zapwriter.Test()()

	assert := assert.New(t)
	test := newTCPTestCase(t, true)
	defer test.Finish()

	test.Send("\x80\x00\x00\x01") // 2Gb message length
	time.Sleep(10 * time.Millisecond)

	assert.Contains(zapwriter.TestString(), "bad message size")
}

type testcase struct {
	description string
	input       []byte
	output      []*points.Points
}

var (
	goodPickles = []testcase{
		// [("param1", (1423931224, 60.2))]
		testcase{"One metric with one datapoint",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na."),
			[]*points.Points{points.OnePoint("param1", 60.2, 1423931224)},
		},
		// [("param1", (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))]
		testcase{"One metric with multiple datapoints",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I14239" +
				"31225\nF50.2\ntp3\n(I1423931226\nF40.2\ntp4\ntp5\na."),
			[]*points.Points{points.OnePoint("param1", 60.2, 1423931224).Add(50.2, 1423931225).Add(40.2, 1423931226)}},
		// [("param1", (1423931224, 60.2)), ("param2", (1423931224, -15))]
		testcase{"Multiple metrics with single datapoints",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na(S'param2" +
				"'\np4\n(I1423931224\nI-15\ntp5\ntp6\na."),
			[]*points.Points{
				points.OnePoint("param1", 60.2, 1423931224),
				points.OnePoint("param2", -15, 1423931224),
			}},
		// [("param1", (1423931224, 60.2), (1423931284, 42)), ("param2", (1423931224, -15))]
		testcase{"Complex update",
			[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2" +
				"\n(I1423931284\nI42\ntp3\ntp4\na(S'param2'\np5\n(I1423931224\nI-15\ntp6" +
				"\ntp7\na."),
			[]*points.Points{
				points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
				points.OnePoint("param2", -15, 1423931224),
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

func BenchmarkParsePickle(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		_, err := ParsePickle(goodPickles[n%len(goodPickles)].input)
		if err != nil {
			b.Fatalf("Error raised while benchmarking")
		}
	}
}
