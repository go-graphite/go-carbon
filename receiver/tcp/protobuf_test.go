package tcp

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/go-carbon/points"
)

func TestProtobuf(t *testing.T) {
	test := newTCPTestCase(t, "protobuf")
	defer test.Finish()

	test.Send("\x00\x00\x00 \n\x1e\n\x0bhello.world\x12\x0f\x08\xf8\xa7\xbb\xb4\x05\x11\x00\x00\x00\x00\x00\x00E@")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42, 1452200952))
	default:
		t.Fatalf("Message #0 not received")
	}
}

var (
	goodProtobuf = []testcase{
		// [("param1", (1423931224, 60.2))]
		testcase{"One metric with one datapoint",
			[]byte("\n\x19\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@"),
			[]*points.Points{points.OnePoint("param1", 60.2, 1423931224)},
		},
		// [("param1", (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))]
		testcase{"One metric with multiple datapoints",
			[]byte("\n;\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\xd9\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19I@\x12\x0f\x08\xda\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19D@"),
			[]*points.Points{points.OnePoint("param1", 60.2, 1423931224).Add(50.2, 1423931225).Add(40.2, 1423931226)}},
		// [("param1", (1423931224, 60.2)), ("param2", (1423931224, -15))]
		testcase{"Multiple metrics with single datapoints",
			[]byte("\n\x19\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0"),
			[]*points.Points{
				points.OnePoint("param1", 60.2, 1423931224),
				points.OnePoint("param2", -15, 1423931224),
			}},
		// [("param1", (1423931224, 60.2), (1423931284, 42)), ("param2", (1423931224, -15))]
		testcase{"Complex update",
			[]byte("\n*\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\x94\xef\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00E@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0"),
			[]*points.Points{
				points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
				points.OnePoint("param2", -15, 1423931224),
			},
		},
	}

	badProtobuf = [][]byte{}
)

func TestParseProtobuf(t *testing.T) {
	for _, tc := range goodProtobuf {
		output, err := ParseProtobuf(tc.input)
		assert.Nil(t, err)
		assert.Equal(t, tc.output, output, fmt.Sprintf("failed while parsing: '%s'", tc.description))
	}

	for casenum, input := range badProtobuf {
		_, err := ParseProtobuf(input)
		assert.Error(t, err, fmt.Sprintf("bad pickle #%d failed to raise and error", casenum))
	}

}

func BenchmarkParseProtobuf(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		_, err := ParseProtobuf(goodProtobuf[n%len(goodProtobuf)].input)
		if err != nil {
			b.Fatalf("Error raised while benchmarking")
		}
	}
}
