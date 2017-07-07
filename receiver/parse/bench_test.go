package parse

import (
	"testing"

	"github.com/lomik/go-carbon/points"
)

// benchmarks
// same for all protocols

/* bench1
data = [
	[('param1', (1423931224, 60.2))],
	[('param1', (1423931224, 60.2), (1423931225, 50.2), (1423931226, 40.2))],
	[('param1', (1423931224, 60.2)), ('param2', (1423931224, -15))],
	[('param1', (1423931224, 60.2), (1423931284, 42)), ('param2', (1423931224, -15))],
]
*/

var pickle1 = [][]byte{
	[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na."),
	[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931225\nF50.2\ntp3\n(I1423931226\nF40.2\ntp4\ntp5\na."),
	[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\ntp3\na(S'param2'\np4\n(I1423931224\nI-15\ntp5\ntp6\na."),
	[]byte("(lp0\n(S'param1'\np1\n(I1423931224\nF60.2\ntp2\n(I1423931284\nI42\ntp3\ntp4\na(S'param2'\np5\n(I1423931224\nI-15\ntp6\ntp7\na."),
}

var protobuf1 = [][]byte{
	[]byte("\n\x19\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@"),
	[]byte("\n;\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\xd9\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19I@\x12\x0f\x08\xda\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19D@"),
	[]byte("\n\x19\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0"),
	[]byte("\n*\n\x06param1\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x9a\x99\x99\x99\x99\x19N@\x12\x0f\x08\x94\xef\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00E@\n\x19\n\x06param2\x12\x0f\x08\xd8\xee\xfd\xa6\x05\x11\x00\x00\x00\x00\x00\x00.\xc0"),
}

var plain1 = [][]byte{
	[]byte("param1 60.2 1423931224\n"),
	[]byte("param1 60.2 1423931224\nparam1 50.2 1423931225\nparam1 40.2 1423931226\n"),
	[]byte("param1 60.2 1423931224\nparam2 -15 1423931224\n"),
	[]byte("param1 60.2 1423931224\nparam1 42 1423931284\nparam2 -15 1423931224\n"),
}

func runBenchmark(b *testing.B, parser func(body []byte) ([]*points.Points, error), data [][]byte) {
	// run function b.N times
	for n := 0; n < b.N; n++ {
		_, err := parser(data[n%len(data)])
		if err != nil {
			b.Fatalf("Error raised while benchmarking")
		}
	}
}

func BenchmarkPickle1(b *testing.B) {
	runBenchmark(b, Pickle, pickle1)
}

func BenchmarkProtobuf1(b *testing.B) {
	runBenchmark(b, Protobuf, protobuf1)
}

func BenchmarkPlain(b *testing.B) {
	runBenchmark(b, Plain, plain1)
}

func BenchmarkOldPlain(b *testing.B) {
	runBenchmark(b, oldPlain, plain1)
}
