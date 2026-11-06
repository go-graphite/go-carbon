package points

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestBorrowedReaderMatchesLegacyAtEveryCut(t *testing.T) {
	var raw []byte
	for i := range 40 {
		p := OnePoint(fmt.Sprintf("metric.%d", i), math.Float64frombits(uint64(i)<<56), -int64(i))
		if i%2 != 0 {
			p.Add(float64(i), int64(i))
		}
		raw = p.AppendBinary(raw)
	}
	for cut := 0; cut <= len(raw); cut++ {
		var want, got []*Points
		original := ReadBinary(bytes.NewReader(raw[:cut]), func(p *Points) { want = append(want, p) })
		buffer := []int{4096, 64 << 10, 4 << 20}[cut%3]
		borrowed := ReadBinaryBorrowedSize(bytes.NewReader(raw[:cut]), buffer, func(name []byte, data []Point) {
			got = append(got, &Points{Metric: string(name), Data: append([]Point(nil), data...)})
		})
		if fmt.Sprint(original) != fmt.Sprint(borrowed) || !reflect.DeepEqual(want, got) {
			t.Fatalf("cut %d: original=%v borrowed=%v", cut, original, borrowed)
		}
	}
}
