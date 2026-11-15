package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// censusReader presents a single journal partition as an unchanged logical
// segment. Only its payloads are read; all frame headers are inspected.
type censusFrame struct{ logical, physical, length int64 }
type censusReader struct {
	f      *os.File
	frames []censusFrame
}

func (r censusReader) ReadAt(b []byte, off int64) (int, error) {
	if off < 64 {
		return r.f.ReadAt(b, off)
	}
	i := sort.Search(len(r.frames), func(i int) bool { return r.frames[i].logical+r.frames[i].length > off })
	if i == len(r.frames) {
		return 0, io.EOF
	}
	f := r.frames[i]
	if off < f.logical || off+int64(len(b)) > f.logical+f.length {
		return 0, io.ErrUnexpectedEOF
	}
	return r.f.ReadAt(b, f.physical+off-f.logical)
}

// Opt-in read-only bounded journal census. Event intervals are not receive
// intervals; per-frame counts measure only the existing journal grouping.
func TestSealDensityCensus(t *testing.T) {
	dir := os.Getenv("SEAL_DENSITY_SOURCE")
	if dir == "" {
		t.Skip("read-only journal census")
	}
	paths, e := filepath.Glob(filepath.Join(dir, "*.log"))
	if e != nil || len(paths) < 2 {
		t.Fatal("need closed segment", e)
	}
	f, e := os.Open(paths[len(paths)-2])
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	var original [64]byte
	if _, e = f.ReadAt(original[:], 0); e != nil {
		t.Fatal(e)
	}
	cr := censusReader{f: f}
	logical := int64(64)
	stat, _ := f.Stat()
	for physical := int64(64); physical+52 < stat.Size(); {
		var h [52]byte
		if _, e = f.ReadAt(h[:], physical); e != nil {
			t.Fatal(e)
		}
		if binary.LittleEndian.Uint32(h[:]) != 0x4c494347 || string(h[8:16]) != string(original[16:24]) {
			break
		}
		length := int64(52) + int64(binary.LittleEndian.Uint32(h[16:]))
		if length <= 52 || length > 64<<20 {
			t.Fatal("invalid frame size")
		}
		if binary.LittleEndian.Uint16(h[6:]) == 0 {
			cr.frames = append(cr.frames, censusFrame{logical, physical, length})
			logical += length
		}
		physical += length
	}
	r, e := inputlog.NewSegmentReplay(cr)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	counts := map[int]int64{}
	intervals := map[int64]int64{}
	last := map[string]int64{}
	sampled := map[string]bool{}
	var total, groups int64
	off := int64(64)
	for off < 128<<20 {
		var h [52]byte
		if _, e = cr.ReadAt(h[:], off); e != nil {
			t.Fatal(e)
		}
		end := off + 52 + int64(binary.LittleEndian.Uint32(h[16:]))
		if end > 128<<20 {
			break
		}
		rows, next, n, e := r.Next(end)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		off = next
		total += int64(n)
		for _, p := range rows {
			groups++
			counts[len(p.Data)]++
			// Bound sampled identities, avoiding retention of the full catalogue.
			if !sampled[p.Metric] && len(sampled) < 100000 {
				sampled[p.Metric] = true
			}
			if !sampled[p.Metric] {
				continue
			}
			for _, v := range p.Data {
				if prev, ok := last[p.Metric]; ok {
					intervals[v.Timestamp-prev]++
				}
				last[p.Metric] = v.Timestamp
			}
		}
	}
	var after [64]byte
	_, e = f.ReadAt(after[:], 0)
	if e != nil || after != original {
		t.Fatal("source recycled")
	}
	out := map[string]any{"path": f.Name(), "bytes": off, "points": total, "groups": groups, "points_per_group": float64(total) / float64(groups), "group_density_histogram": counts, "sampled_names": len(sampled), "event_interval_seconds": intervals, "partition": 0, "selected_frames": len(cr.frames), "segment_created_ns": binary.LittleEndian.Uint64(original[24:])}
	b, e := json.Marshal(out)
	if e != nil {
		t.Fatal(e)
	}
	fmt.Println("DENSITY", string(b))
}
