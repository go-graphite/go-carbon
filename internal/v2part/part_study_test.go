//go:build partstudy && cgo

package v2part

import (
	"encoding/binary"
	"encoding/json"
	"math/bits"
	"os"
	"testing"

	"github.com/DataDog/zstd"
	"github.com/cockroachdb/pebble/sstable"
)

// TestPartStudy attributes shared ZSTD blocks with a five-player Shapley
// decomposition. Bytes outside a coalition are zeroed in place, retaining
// block boundaries. The all-zero block cost and physical trailer go to headers.
// This is an allocation of joint compression, not independently stored columns.
func TestPartStudy(t *testing.T) {
	path := os.Getenv("PART_STUDY")
	if path == "" {
		t.Skip("PART_STUDY")
	}
	b, _ := json.Marshal(partStudy(t, path))
	t.Log(string(b))
}

// partStudy returns the column attribution of one part file.
func partStudy(t *testing.T, path string) map[string]any {
	t.Helper()
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	rd, e := sstable.NewSimpleReadable(f)
	if e != nil {
		t.Fatal(e)
	}
	r, e := sstable.NewReader(rd, sstable.ReaderOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	layout, e := r.Layout()
	if e != nil {
		t.Fatal(e)
	}
	file, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	enc, e := newDenseEncoder()
	if e != nil {
		t.Fatal(e)
	}
	names := []string{"keys", "timestamps_slots", "orders", "values", "headers_envelopes_flags"}
	raw := make([]int64, 5)
	allocated := make([]float64, 5)
	var physical, points, runs int64
	tags := map[byte]int64{}
	modes := map[byte]int64{}
	for _, h := range layout.Data {
		stored := file[h.Offset : h.Offset+h.Length]
		var b []byte
		switch file[h.Offset+h.Length] {
		case 0:
			b = append([]byte(nil), stored...)
		case 7:
			_, n := binary.Uvarint(stored)
			b, e = zstd.Decompress(nil, stored[n:])
			if e != nil {
				t.Fatal(e)
			}
		default:
			t.Fatal("compression")
		}
		label := make([]byte, len(b))
		for i := range label {
			label[i] = 4
		}
		restart := len(b) - 4 - 4*int(binary.LittleEndian.Uint32(b[len(b)-4:]))
		pos := 0
		take := func() uint64 {
			v, n := binary.Uvarint(b[pos:])
			if n <= 0 {
				t.Fatal("varint")
			}
			pos += n
			return v
		}
		mark := func(start, end int, c byte) {
			for i := start; i < end; i++ {
				label[i] = c
			}
		}
		for pos < restart {
			start := pos
			_ = take()
			nk, nv := int(take()), int(take())
			pos += nk
			mark(start, pos, 0)
			end := pos + nv
			v := b[pos:end]
			if len(v) >= 4 && string(v[:4]) == string(headerMagic) {
				pos += 4
				n := int(take())
				pos += n
			}
			if end-pos < 6 || string(b[pos:pos+4]) != string(denseMagic) {
				pos = end
				continue
			}
			mode := b[pos+4]
			modes[mode]++
			pos += 5
			n := int(take())
			points += int64(n)
			runs++
			start = pos
			for i := 0; i < n; i++ {
				take()
			}
			if mode&denseAligned != 0 {
				take()
				take()
			} else {
				for i := 0; i < n; i++ {
					take()
				}
			}
			mark(start, pos, 1)
			start = pos
			if mode&denseModeOrderLinear != 0 {
				take()
				take()
				width := int(b[pos])
				pos += 1 + width*(n-1)
			} else {
				for i := 0; i < n; i++ {
					take()
				}
			}
			mark(start, pos, 2)
			pos += (n + 7) / 8
			start = pos
			if mode&denseTagged != 0 {
				tags[b[pos]]++
			}
			pos = end
			mark(start, pos, 3)
		}
		for _, c := range label {
			raw[c]++
		}
		cost := make([]float64, 32)
		var out []byte
		masked := make([]byte, len(b))
		for m := 0; m < 32; m++ {
			for i, c := range label {
				masked[i] = 0
				if m&(1<<c) != 0 {
					masked[i] = b[i]
				}
			}
			out = binary.AppendUvarint(out[:0], uint64(len(b)))
			out, e = enc.append(masked, out, denseRestLevel)
			if e != nil {
				t.Fatal(e)
			}
			cost[m] = float64(len(out) + 5)
			if len(out) >= len(b)-len(b)/8 {
				cost[m] = float64(len(b) + 5)
			}
		}
		// Preserve measured size exactly, including blocks with legacy compression.
		scale := float64(h.Length+5) / cost[31]
		allocated[4] += cost[0] * scale
		choose := []float64{1, 4, 6, 4, 1}
		for c := 0; c < 5; c++ {
			for m := 0; m < 32; m++ {
				if m&(1<<c) == 0 {
					allocated[c] += (cost[m|1<<c] - cost[m]) * scale / (5 * choose[bits.OnesCount(uint(m))])
				}
			}
		}
		physical += int64(h.Length + 5)
	}
	result := map[string]any{"path": path, "points": points, "runs": runs, "data_bytes": physical, "file_bytes": len(file), "tags": tags, "modes": modes}
	for c, n := range names {
		result[n] = map[string]any{"raw": raw[c], "attributed": allocated[c], "bpp": allocated[c] / float64(points)}
	}
	return result
}
