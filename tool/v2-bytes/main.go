// v2-bytes measures immutable input segments without opening a live database.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/partlog"
)

type segment struct {
	Path                  string
	Created               int64
	Bytes, Points, Frames int64
	f                     *os.File
}

func scan(path string) (s segment, err error) {
	s.Path = path
	s.f, err = os.Open(path)
	if err != nil {
		return
	}
	h := make([]byte, 64)
	_, err = s.f.ReadAt(h, 0)
	if err != nil {
		return
	}
	tab := crc32.MakeTable(crc32.Castagnoli)
	if string(h[:8]) != "GCINLOG1" || binary.LittleEndian.Uint32(h[32:]) != crc32.Checksum(h[:32], tab) {
		err = fmt.Errorf("segment header: %s", path)
		return
	}
	salt := binary.LittleEndian.Uint64(h[16:])
	s.Created = int64(binary.LittleEndian.Uint64(h[24:]))
	s.Bytes = 64
	b := make([]byte, 52)
	for {
		_, e := s.f.ReadAt(b, s.Bytes)
		if e == io.EOF {
			break
		}
		if e != nil {
			err = e
			return
		}
		if binary.LittleEndian.Uint32(b) != 0x4c494347 || binary.LittleEndian.Uint64(b[8:]) != salt {
			break
		}
		if binary.LittleEndian.Uint32(b[48:]) != crc32.Checksum(b[:48], tab) {
			err = fmt.Errorf("frame CRC: %s:%d", path, s.Bytes)
			return
		}
		s.Bytes += 52 + int64(binary.LittleEndian.Uint32(b[16:]))
		s.Points += int64(binary.LittleEndian.Uint32(b[24:]))
		s.Frames++
	}
	return
}
func main() {
	dir := flag.String("input", "", "input-log directory (read only)")
	part := flag.Int("partition", -1, "select one independent input-log partition (0-15), -1 all")
	limit := flag.Int("segments", 7, "oldest complete segments, newest excluded")
	wait := flag.Int("wait", 0, "seconds to hold read descriptors before decoding")
	secondClock := flag.Bool("arrival-seconds", false, "lossy arrival-clock diagnostic; default preserves nanoseconds")
	manifest := flag.Bool("manifest", false, "headers only")
	archives := flag.String("archives", "", "directory of complete native .transfer exports")
	flag.Parse()
	if *archives != "" {
		archiveProbe(*archives)
		return
	}
	runtime.GOMAXPROCS(2)
	paths, e := filepath.Glob(filepath.Join(*dir, "*.log"))
	must(e)
	if len(paths) > 0 {
		paths = paths[:len(paths)-1]
	}
	if len(paths) > *limit {
		paths = paths[:*limit]
	}
	if len(paths) == 0 {
		panic("no complete source segments")
	}
	var segs []segment
	for _, p := range paths {
		s, e := scan(p)
		must(e)
		defer s.f.Close()
		segs = append(segs, s)
	}
	emit(map[string]any{"stage": "manifest", "arrival_seconds": *secondClock, "segments": segs, "when_cet": time.Now().Format(time.RFC3339)})
	if *manifest {
		return
	}
	time.Sleep(time.Duration(*wait) * time.Second)
	var total, raw, journal, frames, missing int64
	var groups, singletonGroups, maxGroup int64
	began := time.Now()
	for _, s := range segs {
		var enc partlog.DictionaryBlockEncoder
		var dec partlog.DictionaryBlockDecoder
		originalHeader := make([]byte, 64)
		_, e := s.f.ReadAt(originalHeader, 0)
		must(e)
		view, frontier, selectedPoints, e := selectPartition(s.f, s.Bytes, *part)
		must(e)
		emit(map[string]any{"stage": "selection", "path": s.Path, "partition": *part, "bytes": frontier, "points": selectedPoints})
		replay, e := inputlog.NewSegmentReplay(view)
		must(e)
		var offset int64 = 64
		var count, bytesOut int64
		for {
			ps, end, n, e := replay.Next(frontier)
			if e == io.EOF {
				break
			}
			must(e)
			// Bound records below PDC's 8MiB expanded-record limit. Preserve apply boundaries.
			var fs []partlog.DictionaryFrame
			size := 0
			flush := func() {
				if len(fs) == 0 {
					return
				}
				b, e := enc.Encode(fs)
				must(e)
				back, e := dec.Decode(b)
				must(e)
				if len(back) != len(fs) {
					panic("frame count mismatch")
				}
				for i, f := range fs {
					g := back[i]
					if f.Key != g.Key || f.Clock != g.Clock || len(f.Points) != len(g.Points) {
						panic("identity mismatch")
					}
					for j, p := range f.Points {
						q := g.Points[j]
						if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
							panic("point mismatch")
						}
					}
				}
				bytesOut += int64(len(b) + 32)
				fs = nil
				size = 0
			}
			for _, p := range ps {
				groups++
				if len(p.Data) == 1 {
					singletonGroups++
				}
				maxGroup = max(maxGroup, int64(len(p.Data)))
				if p.FirstArrival == 0 {
					missing += int64(len(p.Data))
				}
				sz := len(p.Metric) + 64 + len(p.Data)*20
				if size+sz > 4<<20 {
					flush()
				}
				clock := p.FirstArrival
				if *secondClock {
					clock /= 1e9
				}
				fs = append(fs, partlog.DictionaryFrame{Key: p.Metric, Clock: clock, Points: p.Data})
				size += sz
			}
			flush()
			count += int64(n)
			offset = end
			frames++
			// Keep source reads below 50MB/s; this is not a throughput benchmark.
			if delay := time.Duration(float64(raw+offset)/50e6*float64(time.Second)) - time.Since(began); delay > 0 {
				time.Sleep(delay)
			}
			if frames%100 == 0 {
				emit(map[string]any{"stage": "progress", "points": total + count, "input_bytes": raw + offset, "elapsed": time.Since(began).Seconds()})
			}
		}
		finalHeader := make([]byte, 64)
		_, e = s.f.ReadAt(finalHeader, 0)
		must(e)
		if !bytes.Equal(originalHeader, finalHeader) {
			panic("source segment recycled during replay")
		}
		if *part >= 0 && count != selectedPoints {
			panic("selected source count changed")
		}
		replay.Close()
		enc.Close()
		dec.Close()
		total += count
		raw += offset
		journal += bytesOut
		emit(map[string]any{"stage": "segment", "path": s.Path, "points": count, "input_bytes": offset, "pdc2_bytes_including_32B_headers": bytesOut, "when_cet": time.Now().Format(time.RFC3339)})
	}
	emit(map[string]any{"stage": "done", "points": total, "input_bytes": raw, "pdc2_bytes": journal, "input_bpp": float64(raw) / float64(total), "pdc2_bpp": float64(journal) / float64(total), "missing_arrival_points": missing, "apply_groups": groups, "singleton_apply_groups": singletonGroups, "max_points_per_apply_group": maxGroup, "elapsed": time.Since(began).Seconds(), "when_cet": time.Now().Format(time.RFC3339), "exact": true})
}
func emit(v any) { must(json.NewEncoder(os.Stdout).Encode(v)) }
func must(e error) {
	if e != nil {
		panic(e)
	}
}
