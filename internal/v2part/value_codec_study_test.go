//go:build valuecodec

package v2part

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// legacyAppendSlots is the pre-tag value codec (mode 0 planes, mode 1 signed
// varint first differences of integer values), kept here as the study baseline.
func legacyAppendSlots(out []byte, records []sealrollup.Record) []byte {
	integer := true
	for _, r := range records {
		f := math.Float64frombits(r.Sample.Bits)
		if !(f >= -(1<<53) && f <= 1<<53) || math.Float64bits(float64(int64(f))) != r.Sample.Bits {
			integer = false
		}
	}
	if integer {
		var prev int64
		for _, r := range records {
			v := int64(math.Float64frombits(r.Sample.Bits))
			out = binary.AppendVarint(out, v-prev)
			prev = v
		}
		return out
	}
	for plane := uint(0); plane < 8; plane++ {
		for _, r := range records {
			out = append(out, byte(r.Sample.Bits>>(plane*8)))
		}
	}
	return out
}

type studySeries struct {
	Start  int64 `json:"start"`
	Step   int64 `json:"step"`
	Values []any `json:"values"`
}

// TestValueCodecStudy measures the value column alone per cohort: every
// capture series cut into 4,096-point runs (the at-rest run size), value
// columns concatenated into 256 KiB blocks and compressed at the rest level.
// VC_IN is a comma-separated list of floatcap .json.gz captures.
func TestValueCodecStudy(t *testing.T) {
	for _, path := range strings.Split(os.Getenv("VC_IN"), ",") {
		runs, points := studyRuns(t, path)
		enc, err := newDenseEncoder()
		if err != nil {
			t.Fatal(err)
		}
		compressed := func(encode func([]byte, []sealrollup.Record) []byte) (int, float64) {
			var block, out []byte
			total := 0
			var spent time.Duration
			for _, r := range runs {
				t0 := time.Now()
				block = encode(block, r)
				spent += time.Since(t0)
				if len(block) >= 256<<10 {
					out, _ = enc.append(block, out[:0], denseRestLevel)
					total += len(out)
					block = block[:0]
				}
			}
			out, _ = enc.append(block, out[:0], denseRestLevel)
			return total + len(out), float64(spent.Nanoseconds()) / float64(points)
		}
		before, beforeNs := compressed(legacyAppendSlots)
		after, afterNs := compressed(func(out []byte, r []sealrollup.Record) []byte { return appendValues(out, r, nil) })
		rawIf := func(short bool) func([]byte, []sealrollup.Record) []byte {
			return func(out []byte, r []sealrollup.Record) []byte {
				v := appendValues(nil, r, nil)
				if v[0] != valuePlanes || (len(r) < 64) != short {
					return append(out, v...)
				}
				out = append(out, valuePlanes)
				for _, x := range r {
					out = binary.LittleEndian.AppendUint64(out, x.Sample.Bits)
				}
				return out
			}
		}
		rawDistinct := func(ratio float64) func([]byte, []sealrollup.Record) []byte {
			return func(out []byte, r []sealrollup.Record) []byte {
				v := appendValues(nil, r, nil)
				if v[0] != valuePlanes || len(r) < 64 {
					return append(out, v...)
				}
				seen := make(map[uint64]struct{}, len(r))
				for _, x := range r {
					seen[x.Sample.Bits] = struct{}{}
				}
				if float64(len(seen)) > ratio*float64(len(r)) {
					return append(out, v...)
				}
				out = append(out, valuePlanes)
				for _, x := range r {
					out = binary.LittleEndian.AppendUint64(out, x.Sample.Bits)
				}
				return out
			}
		}
		d25, _ := compressed(rawDistinct(0.25))
		d50, _ := compressed(rawDistinct(0.5))
		d75, _ := compressed(rawDistinct(0.75))
		t.Logf("%s distinct selector B/pt: d25=%.4f d50=%.4f d75=%.4f", filepath.Base(path), float64(d25)/float64(points), float64(d50)/float64(points), float64(d75)/float64(points))
		rawShort, _ := compressed(rawIf(true))
		rawLong, _ := compressed(rawIf(false))
		t.Logf("%s selector B/pt: raw<64=%.4f raw>=64=%.4f", filepath.Base(path), float64(rawShort)/float64(points), float64(rawLong)/float64(points))
		nocap, _ := compressed(func(out []byte, r []sealrollup.Record) []byte { return studyValues(out, r, 64, false) })
		xorPlanes, _ := compressed(func(out []byte, r []sealrollup.Record) []byte { return studyValues(out, r, 64, true) })
		cap2, _ := compressed(func(out []byte, r []sealrollup.Record) []byte {
			v := studyValues(nil, r, 64, false)
			if v[0] != valuePlanes {
				return append(out, v...)
			}
			out = append(out, valuePlanes)
			for _, x := range r {
				out = binary.LittleEndian.AppendUint64(out, x.Sample.Bits)
			}
			return out
		})
		cap6, _ := compressed(func(out []byte, r []sealrollup.Record) []byte { return studyValues(out, r, 6, false) })
		t.Logf("%s variants B/pt: nocap+rawLE=%.4f cap4(prod)=%.4f cap6=%.4f nocap=%.4f cap4+xorPlanes=%.4f", filepath.Base(path),
			float64(cap2)/float64(points), float64(after)/float64(points), float64(cap6)/float64(points),
			float64(nocap)/float64(points), float64(xorPlanes)/float64(points))
		tags := map[string]int{}
		var decodeNs time.Duration
		dst := make([]sealrollup.Record, MaxDenseSlots)
		for _, r := range runs {
			v := appendValues(nil, r, nil)
			kind := map[byte]string{valuePlanes: "planes", valueDelta1: "delta1", valueDelta2: "delta2"}[v[0]&^31]
			tags[fmt.Sprintf("%s e%d", kind, v[0]&31)] += len(r)
			rs := dst[:len(r)]
			clear(rs)
			t0 := time.Now()
			rest, err := decodeValues(rs, v[0], false, v[1:])
			decodeNs += time.Since(t0)
			if err != nil || len(rest) != 0 {
				t.Fatalf("decode: %v", err)
			}
			for i := range r {
				if rs[i].Sample.Bits != r[i].Sample.Bits {
					t.Fatalf("%s: value %d bits %x != %x", path, i, rs[i].Sample.Bits, r[i].Sample.Bits)
				}
			}
		}
		name := strings.TrimSuffix(filepath.Base(path), ".json.gz")
		t.Logf("%s series=%d(n/a) runs=%d points=%d values B/pt before=%.4f after=%.4f saved=%.4f encode ns/pt before=%.1f after=%.1f decode ns/pt after=%.1f",
			name, -1, len(runs), points, float64(before)/float64(points), float64(after)/float64(points),
			float64(before-after)/float64(points), beforeNs, afterNs, float64(decodeNs.Nanoseconds())/float64(points))
		t.Logf("%s tags (points): %v", name, tags)
	}
}

// studyValues is appendValues with the decimal cost cap and the float codec
// (planes, or planes of each value XORed with its predecessor) as knobs.
func studyValues(out []byte, records []sealrollup.Record, maxCost int, xor bool) []byte {
	e := 0
	for _, r := range records {
		for ; e <= maxDecimalExp; e++ {
			if _, ok := decimalMantissa(r.Sample.Bits, e); ok {
				break
			}
		}
		if e > maxDecimalExp {
			break
		}
	}
	if e <= maxDecimalExp {
		var prev, prevDelta int64
		cost1, cost2 := 0, 0
		for _, r := range records {
			m, _ := decimalMantissa(r.Sample.Bits, e)
			d := m - prev
			cost1 += uvarintLen(zigzag(d))
			cost2 += uvarintLen(zigzag(d - prevDelta))
			prev, prevDelta = m, d
		}
		if min(cost1, cost2) <= maxCost*len(records) {
			second := cost2 < cost1
			out = append(out, byte(e))
			prev, prevDelta = 0, 0
			for _, r := range records {
				m, _ := decimalMantissa(r.Sample.Bits, e)
				d := m - prev
				if second {
					out = binary.AppendUvarint(out, zigzag(d-prevDelta))
				} else {
					out = binary.AppendUvarint(out, zigzag(d))
				}
				prev, prevDelta = m, d
			}
			return out
		}
	}
	out = append(out, valuePlanes)
	for plane := uint(0); plane < 8; plane++ {
		var prev uint64
		for _, r := range records {
			b := r.Sample.Bits
			if xor {
				b, prev = b^prev, b
			}
			out = append(out, byte(b>>(plane*8)))
		}
	}
	return out
}

// studyRuns cuts every series of a floatcap capture into 4,096-point runs.
func studyRuns(t *testing.T, path string) ([][]sealrollup.Record, int) {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var series map[string]studySeries
	if err := json.NewDecoder(z).Decode(&series); err != nil {
		t.Fatal(err)
	}
	f.Close()
	var runs [][]sealrollup.Record
	points := 0
	for _, s := range series {
		var run []sealrollup.Record
		for i, v := range s.Values {
			var x float64
			switch v := v.(type) {
			case float64:
				x = v
			case string:
				if x, err = strconv.ParseFloat(v, 64); err != nil {
					continue
				}
			default:
				continue
			}
			run = append(run, sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: int64(i),
				Order: 1, Sample: sealrollup.Sample{Time: s.Start + int64(i)*s.Step, Bits: math.Float64bits(x)}}})
			if len(run) == MaxDenseSlots {
				runs, run = append(runs, run), nil
			}
		}
		if len(run) > 0 {
			runs = append(runs, run)
		}
	}
	for _, r := range runs {
		points += len(r)
	}
	return runs, points
}
