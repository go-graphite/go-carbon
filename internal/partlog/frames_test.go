package partlog

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// exactV3 and v3Fixture serve the catalog codec tests: exact frame identity
// and IEEE bits, over a fixture with special values and arbitrary int64 clocks.
func exactV3(t testing.TB, a, b []DictionaryFrame) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatal("frame count")
	}
	for i, f := range a {
		g := b[i]
		if f.Key != g.Key || !bytes.Equal(f.Definition, g.Definition) || f.Clock != g.Clock || len(f.Points) != len(g.Points) {
			t.Fatal("frame identity", i)
		}
		for j, p := range f.Points {
			q := g.Points[j]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				t.Fatal("point bits", i, j)
			}
		}
	}
}
func v3Fixture() []DictionaryFrame {
	r := rand.New(rand.NewSource(184))
	var fs []DictionaryFrame
	special := []uint64{0, 1, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001, 0x7ff0000000000001, ^uint64(0)}
	for i := 0; i < 2000; i++ {
		f := DictionaryFrame{Key: string([]byte{'a', 0, byte(i % 31)}), Definition: []byte{0, 1, 2}, Clock: int64(r.Uint64())}
		for j := 0; j < 1+i%7; j++ {
			v := r.Uint64()
			if i%3 == 0 {
				v = special[(i+j)%len(special)]
			}
			f.Points = append(f.Points, points.Point{Timestamp: int64(r.Uint64()), Value: math.Float64frombits(v)})
		}
		fs = append(fs, f)
	}
	return fs
}
