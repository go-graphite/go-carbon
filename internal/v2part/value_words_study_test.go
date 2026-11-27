//go:build valuecodec

package v2part

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// TestValueWordsStudy sweeps the float-run layout choice (byte planes or raw
// little-endian words) by run length and distinct-value ratio, plus a per-run
// oracle that compresses both layouts alone and keeps the smaller one.
func TestValueWordsStudy(t *testing.T) {
	for _, path := range strings.Split(os.Getenv("VC_IN"), ",") {
		runs, points := studyRuns(t, path)
		enc, err := newDenseEncoder()
		if err != nil {
			t.Fatal(err)
		}
		size := func(encode func([]byte, []sealrollup.Record) []byte) float64 {
			var block, out []byte
			total := 0
			for _, r := range runs {
				block = encode(block, r)
				if len(block) >= 256<<10 {
					out, _ = enc.append(block, out[:0], denseRestLevel)
					total += len(out)
					block = block[:0]
				}
			}
			out, _ = enc.append(block, out[:0], denseRestLevel)
			return float64(total+len(out)) / float64(points)
		}
		planes := func(out []byte, r []sealrollup.Record) []byte {
			out = append(out, valuePlanes)
			for plane := uint(0); plane < 8; plane++ {
				for _, x := range r {
					out = append(out, byte(x.Sample.Bits>>(plane*8)))
				}
			}
			return out
		}
		words := func(out []byte, r []sealrollup.Record) []byte {
			out = append(out, valueWords)
			for _, x := range r {
				out = binary.LittleEndian.AppendUint64(out, x.Sample.Bits)
			}
			return out
		}
		float := func(r []sealrollup.Record) bool {
			k := appendValues(nil, r, nil)[0] &^ 31
			return k == valuePlanes || k == valueWords
		}
		pick := func(minPts int, ratio float64) func([]byte, []sealrollup.Record) []byte {
			return func(out []byte, r []sealrollup.Record) []byte {
				if !float(r) {
					return appendValues(out, r, nil)
				}
				if len(r) < minPts {
					return planes(out, r)
				}
				seen := make(map[uint64]struct{}, len(r))
				for _, x := range r {
					seen[x.Sample.Bits] = struct{}{}
				}
				if float64(len(seen)) >= ratio*float64(len(r)) {
					return planes(out, r)
				}
				return words(out, r)
			}
		}
		name := strings.TrimSuffix(filepath.Base(path), ".json.gz")
		t.Logf("%s shipped=%.4f planes-only=%.4f", name, size(func(out []byte, r []sealrollup.Record) []byte { return appendValues(out, r, nil) }), size(pick(1<<30, 0)))
		for _, minPts := range []int{16, 32, 64, 128, 256} {
			line := ""
			for _, ratio := range []float64{0.75, 0.85, 0.9, 0.95, 1.01} {
				line += fmt.Sprintf(" d<%.2f=%.4f", ratio, size(pick(minPts, ratio)))
			}
			t.Logf("%s min%d:%s", name, minPts, line)
		}
		var a, b []byte
		oracle := size(func(out []byte, r []sealrollup.Record) []byte {
			if !float(r) {
				return appendValues(out, r, nil)
			}
			a, _ = enc.append(planes(nil, r), a[:0], denseRestLevel)
			b, _ = enc.append(words(nil, r), b[:0], denseRestLevel)
			if len(b) < len(a) {
				return words(out, r)
			}
			return planes(out, r)
		})
		t.Logf("%s oracle(alone)=%.4f", name, oracle)
	}
}
