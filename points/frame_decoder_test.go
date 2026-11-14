package points

import (
	"bytes"
	"math"
	"testing"
)

func TestFrameDecoderDifferential(t *testing.T) {
	values := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x3ff0000000000001}
	var raw []byte
	for i := range 17 {
		p := &Points{Metric: []string{"", "metric", "metric;z=1;a=2"}[i%3]}
		for j := range i%5 + 1 {
			p.Add(math.Float64frombits(values[(i+j)%len(values)]), int64(i-j)*987654321)
		}
		raw = p.AppendBinary(raw)
	}
	var d FrameDecoder
	for cut := 0; cut <= len(raw); cut++ {
		var got, want []byte
		save := func(dst *[]byte) func([]byte, []Point) {
			return func(n []byte, p []Point) { *dst = (&Points{Metric: string(n), Data: p}).AppendBinary(*dst) }
		}
		oldErr := ReadBinaryBorrowed(bytes.NewReader(raw[:cut]), save(&want))
		err := d.Read(raw[:cut], save(&got))
		if (err == nil) != (oldErr == nil) || !bytes.Equal(got, want) {
			t.Fatalf("prefix %d changed output/errors: %v vs %v", cut, err, oldErr)
		}
	}
}
