package parse

import (
	"testing"

	"github.com/lomik/go-carbon/points"
)

var protobufs = []testcase{
	// [("param1", (1423931224, 60.2))]
	testcase{"One metric with one datapoint",
		[]byte("\n\x19\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@"),
		[]*points.Points{points.OnePoint("param1", 60.2, 1423931224)},
		false,
	},
	// [("param1", (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))]
	testcase{"One metric with multiple datapoints",
		[]byte("\n;\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\xd9\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19I@\x12\x0f\x08\xda\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19D@"),
		[]*points.Points{points.OnePoint("param1", 60.2, 1423931224).Add(50.2, 1423931225).Add(40.2, 1423931226)},
		false,
	},
	// [("param1", (1423931224, 60.2)), ("param2", (1423931224, -15))]
	testcase{"Multiple metrics with single datapoints",
		[]byte("\n\x19\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0"),
		[]*points.Points{
			points.OnePoint("param1", 60.2, 1423931224),
			points.OnePoint("param2", -15, 1423931224),
		},
		false,
	},
	// [("param1", (1423931224, 60.2), (1423931284, 42)), ("param2", (1423931224, -15))]
	testcase{"Complex update",
		[]byte("\n*\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\x94\xef\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00E@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0"),
		[]*points.Points{
			points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
			points.OnePoint("param2", -15, 1423931224),
		},
		false,
	},
}

func TestProtobuf(t *testing.T) {
	run(t, protobufs, Protobuf)
}
