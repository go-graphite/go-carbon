package parse

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

var pickles = []testcase{
	// good values

	// [("param1", (1423931224, 60.2))]
	{"One metric with one datapoint",
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na."),
		[]*points.Points{points.OnePoint("param1", 60.2, 1423931224)},
		false,
	},
	// [("param1", (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))]
	{"One metric with multiple datapoints",
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931225\nF50.2\ntp3\n(I1423931226\nF40.2\ntp4\ntp5\na."),
		[]*points.Points{points.OnePoint("param1", 60.2, 1423931224).Add(50.2, 1423931225).Add(40.2, 1423931226)},
		false,
	},
	// [("param1", (1423931224, 60.2)), ("param2", (1423931224, -15))]
	{"Multiple metrics with single datapoints",
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na(S'param2'\np4\n(I1423931224\nI-15\ntp5\ntp6\na."),
		[]*points.Points{
			points.OnePoint("param1", 60.2, 1423931224),
			points.OnePoint("param2", -15, 1423931224),
		},
		false,
	},
	// [("param1", (1423931224, 60.2), (1423931284, 42)), ("param2", (1423931224, -15))]
	{"Complex update",
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931284\nI42\ntp3\ntp4\na(S'param2'\np5\n(I1423931224\nI-15\ntp6\ntp7\na."),
		[]*points.Points{
			points.OnePoint("param1", 60.2, 1423931224).Add(42, 1423931284),
			points.OnePoint("param2", -15, 1423931224),
		},
		false,
	},

	// bad values

	{"empty",
		[]byte(""),
		nil,
		true,
	},
	{"incorrect numper of elements",
		[]byte("(lp0\n(S'param1'\np1\ntp2\na."),
		nil,
		true,
	},
	{"too few elements in a datapoint",
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931284\ntp3\ntp4\na."),
		nil,
		true,
	},
	{"too many elements in a datapoint",
		[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\nI3\ntp2\ntp3\na."),
		nil,
		true,
	},
	{"negative timestamp in a datapoint",
		[]byte("(lp0\n(S'param1'\np1\n(I-1423931224\nI60\ntp2\ntp3\na."),
		nil,
		true,
	},
	{"timestamp too big for uint32",
		[]byte("(lp0\n(S'param1'\np1\n(I4294967296\nF60.2\ntp2\ntp3\na."),
		nil,
		true,
	},
}

func TestPickle(t *testing.T) {
	run(t, pickles, Pickle)
}
