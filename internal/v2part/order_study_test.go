//go:build partstudy && cgo

package v2part

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math/bits"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func studyOrderColumn(rs []sealrollup.Record, scheme string) []byte {
	if scheme == "original" {
		return nil
	}
	if scheme != "dod_planes" && scheme != "delta_planes" && scheme != "linear" && scheme != "linear_planes" {
		return studyExtraOrders(rs, scheme)
	}
	out := binary.AppendUvarint(nil, rs[0].Order)
	n := len(rs)
	vals := make([]uint64, n-1)
	var delta int64
	if scheme == "linear" || scheme == "linear_planes" {
		if n > 1 {
			delta = int64(rs[n-1].Order-rs[0].Order) / int64(n-1)
		}
		out = binary.AppendVarint(out, delta)
	}
	prev := rs[0].Order
	var pd int64
	var union uint64
	for i, r := range rs[1:] {
		var v uint64
		switch scheme {
		case "dod_planes":
			d := int64(r.Order - prev)
			v = zigzag(d - pd)
			pd = d
			prev = r.Order
		case "delta_planes":
			v = zigzag(int64(r.Order - prev))
			prev = r.Order
		default:
			v = zigzag(int64(r.Order - (rs[0].Order + uint64(int64(i+1)*delta))))
		}
		vals[i] = v
		union |= v
	}
	if scheme == "linear" {
		for _, v := range vals {
			out = binary.AppendUvarint(out, v)
		}
		return out
	}
	width := (bits.Len64(union) + 7) / 8
	out = append(out, byte(width))
	for p := 0; p < width; p++ {
		for _, v := range vals {
			out = append(out, byte(v>>uint(p*8)))
		}
	}
	return out
}

func TestPartOrderStudy(t *testing.T) {
	path := os.Getenv("PART_STUDY")
	if path == "" {
		t.Skip("PART_STUDY")
	}
	schemes := []string{"original", "dod_planes", "delta_planes", "linear", "linear_planes", "raw_planes", "raw_bits", "linear_bits", "linear_unsigned", "delta_bits", "xor_planes", "linear_shift8", "linear_shift10", "linear_shift16", "linear_nibbles", "linear_base", "xor_predict", "round8", "round12", "round16", "round17"}
	blocks := make([]denseBlock, len(schemes))
	totals := make([]int, len(schemes))
	spent := make([]time.Duration, len(schemes))
	points := 0
	enc, e := newDenseEncoder()
	if e != nil {
		t.Fatal(e)
	}
	var rewrite *denseSSTWriter
	if target := os.Getenv("PART_REWRITE"); target != "" {
		f, e := os.Create(target)
		if e != nil {
			t.Fatal(e)
		}
		out := &hashedOutput{f: f, buf: bufio.NewWriterSize(f, 256<<10), h: sha256.New()}
		rewrite, e = newDenseSSTWriter(out)
		if e != nil {
			t.Fatal(e)
		}
	}
	var compressed []byte
	flush := func(i int) {
		if blocks[i].entries == 0 {
			return
		}
		b := blocks[i].finish()
		compressed = binary.AppendUvarint(compressed[:0], uint64(len(b)))
		compressed, e = enc.append(b, compressed, 3)
		if e != nil {
			t.Fatal(e)
		}
		sz := len(compressed)
		if sz >= len(b)-len(b)/8 {
			sz = len(b)
		}
		totals[i] += sz + 5
		blocks[i].reset()
	}
	visit := func(k Key, v []byte) error {
		key, e := Encode(k)
		if e != nil {
			return e
		}
		key = binary.LittleEndian.AppendUint64(key, 1)
		_, run, e := SplitHeader(v)
		if e != nil {
			return e
		}
		off := len(v) - len(run)
		var rs []sealrollup.Record
		start, end := 0, 0
		if len(run) > 6 && string(run[:4]) == string(denseMagic) {
			rs, e = DecodeSlots(k.Archive, k.BlockStart, run)
			if e != nil {
				return e
			}
			points += len(rs)
			p := 5
			take := func() { _, n := binary.Uvarint(run[p:]); p += n }
			take()
			for range rs {
				take()
			}
			if run[4]&denseAligned != 0 {
				take()
				take()
			} else {
				for range rs {
					take()
				}
			}
			start = p
			if run[4]&denseModeOrderLinear != 0 {
				take()
				take()
				width := int(run[p])
				if run[4]&denseModeOrderBits != 0 {
					p += 1 + width*((len(rs)-1+7)/8)
				} else {
					p += 1 + width*(len(rs)-1)
				}
			} else {
				for range rs {
					take()
				}
			}
			end = p
		}
		if rewrite != nil {
			b := v
			if len(rs) > 0 {
				encoded, e := appendSlotsOrder(nil, rs, nil, true)
				if e != nil {
					return e
				}
				got, e := DecodeSlots(k.Archive, k.BlockStart, encoded)
				if e != nil || !reflect.DeepEqual(got, rs) {
					t.Fatalf("rewrite changed records: %v", e)
				}
				b = append(append([]byte(nil), v[:off]...), encoded...)
			}
			if e := rewrite.Set(key[:len(key)-8], b); e != nil {
				return e
			}
		}
		for i, s := range schemes {
			b := v
			if len(rs) > 0 && i > 0 {
				st := time.Now()
				o := studyOrderColumn(rs, s)
				spent[i] += time.Since(st)
				b = append(append(append([]byte(nil), v[:off+start]...), o...), v[off+end:]...)
			}
			if blocks[i].entries > 0 && len(blocks[i].data)+len(key)+len(b)+4*len(blocks[i].restarts)+32 > denseBlockSize {
				flush(i)
			}
			blocks[i].add(key, b, 64)
		}
		return nil
	}
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := sstable.NewSimpleReadable(f)
	if e != nil {
		t.Fatal(e)
	}
	reader, e := sstable.NewReader(rd, sstable.ReaderOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	it, e := reader.NewIter(nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer it.Close()
	for ik, v := it.First(); ik != nil; ik, v = it.Next() {
		k, e := Decode(ik.UserKey)
		if e != nil {
			t.Fatal(e)
		}
		b, _, e := v.Value(nil)
		if e != nil {
			t.Fatal(e)
		}
		if e = visit(k, b); e != nil {
			t.Fatal(e)
		}
	}
	if e = it.Error(); e != nil {
		t.Fatal(e)
	}
	if rewrite != nil {
		if e := rewrite.Close(); e != nil {
			t.Fatal(e)
		}
	}
	for i, s := range schemes {
		flush(i)
		j, _ := json.Marshal(map[string]any{"scheme": s, "data_bytes": totals[i], "points": points, "bpp": float64(totals[i]) / float64(points), "encode_ns_per_point": float64(spent[i].Nanoseconds()) / float64(points)})
		t.Log(string(j))
	}
}

func studyExtraOrders(rs []sealrollup.Record, scheme string) []byte {
	n := len(rs)
	first := rs[0].Order
	step := uint64(int64(rs[n-1].Order-first) / int64(n-1))
	if scheme == "round8" {
		step = (step + 128) &^ 255
	}
	if scheme == "round12" {
		step = (step + 2048) &^ 4095
	}
	if scheme == "round16" {
		step = (step + 32768) &^ 65535
	}
	if scheme == "round17" {
		step = (step + 65536) &^ 131071
	}
	vals := make([]uint64, n-1)
	var union uint64
	shift := uint(0)
	if scheme == "linear_shift8" {
		shift = 8
	}
	if scheme == "linear_shift10" {
		shift = 10
	}
	if scheme == "linear_shift16" {
		shift = 16
	}
	for i := 1; i < n; i++ {
		v := rs[i].Order - first - step*uint64(i)
		switch scheme {
		case "raw_planes", "raw_bits":
			v = rs[i].Order
		case "xor_predict":
			v = rs[i].Order ^ (first + step*uint64(i))
		case "xor_planes":
			v = rs[i].Order ^ rs[i-1].Order
		case "delta_bits":
			v = zigzag(int64(rs[i].Order - rs[i-1].Order))
		case "linear_unsigned", "linear_base":
		default:
			v = zigzag(int64(v))
		}
		if shift > 0 {
			v = (v >> shift) | (v << (64 - shift))
		}
		vals[i-1] = v
		union |= v
	}
	if scheme == "linear_base" {
		lo := int64(vals[0])
		for _, v := range vals {
			if int64(v) < lo {
				lo = int64(v)
			}
		}
		union = 0
		for i, v := range vals {
			vals[i] = v - uint64(lo)
			union |= vals[i]
		}
	}
	out := binary.AppendUvarint(nil, first)
	out = binary.AppendUvarint(out, step)
	width := (bits.Len64(union) + 7) / 8
	out = append(out, byte(width))
	if scheme == "raw_bits" || scheme == "linear_bits" || scheme == "delta_bits" {
		for p := 0; p < bits.Len64(union); p++ {
			for i := 0; i < len(vals); i += 8 {
				var b byte
				for j := 0; j < 8 && i+j < len(vals); j++ {
					b |= byte((vals[i+j]>>uint(p))&1) << uint(j)
				}
				out = append(out, b)
			}
		}
	} else if scheme == "linear_nibbles" {
		for p := 0; p < (bits.Len64(union)+3)/4; p++ {
			for i := 0; i < len(vals); i += 2 {
				b := byte(vals[i]>>uint(p*4)) & 15
				if i+1 < len(vals) {
					b |= (byte(vals[i+1]>>uint(p*4)) & 15) << 4
				}
				out = append(out, b)
			}
		}
	} else {
		for p := 0; p < width; p++ {
			for _, v := range vals {
				out = append(out, byte(v>>uint(p*8)))
			}
		}
	}
	return out
}
