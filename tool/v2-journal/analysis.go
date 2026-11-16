package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/partlog"
)

// analyze keeps the same complete source selections as the ordinary comparison.
// It reports exact serialized field bytes before joint zstd, never a fabricated
// attribution of compressed bytes to fields.
func analyze(segs []segment, partition int, rate float64, streamList string, sampleLimit, repeats int) {
	var accounts [2]partlog.PredictorV3Accounting
	ms := []*measurement{{Codec: "PDC3-series-clock", BlockMiB: 4}, {Codec: "PDC3-sparse-clock", BlockMiB: 4}}
	var sample []partlog.DictionaryFrame
	samplePoints := 0
	var sourceBytes, total int64
	started := time.Now()
	for _, s := range segs {
		h := make([]byte, 64)
		_, err := s.f.ReadAt(h, 0)
		must(err)
		if !bytes.Equal(h, s.header[:]) {
			panic("source recycled before analysis")
		}
		view, frontier, selected, err := selectPartition(s.f, s.Bytes, partition)
		must(err)
		emit(map[string]any{"stage": "analysis_source", "path": s.Path, "points": selected, "bytes": frontier, "created_ns": s.Created})
		for i, m := range ms {
			m.encoder = &partlog.PredictorV3Encoder{SeriesClocks: true, SparseClocks: i == 1, Accounting: &accounts[i]}
			m.decoder = &partlog.PredictorV3Decoder{}
		}
		r, err := inputlog.NewSegmentReplay(view)
		must(err)
		var count int64
		for {
			ps, offset, n, err := r.Next(frontier)
			if err == io.EOF {
				break
			}
			must(err)
			for _, p := range ps {
				f := partlog.DictionaryFrame{Key: p.Metric, Clock: p.FirstArrival, Points: p.Data}
				for _, m := range ms {
					m.add(f)
				}
				if samplePoints < sampleLimit {
					sample = append(sample, f)
					samplePoints += len(f.Points)
				}
			}
			count += int64(n)
			if delay := time.Duration(float64(sourceBytes+offset)/(rate*1e6)*float64(time.Second)) - time.Since(started); delay > 0 {
				time.Sleep(delay)
			}
		}
		for i, m := range ms {
			m.flush()
			m.report()
			emit(map[string]any{"stage": "fields", "codec": m.Codec, "pre_zstd": accounts[i]})
			m.encoder.Close()
			m.decoder.Close()
		}
		r.Close()
		_, err = s.f.ReadAt(h, 0)
		must(err)
		if !bytes.Equal(h, s.header[:]) || count != selected {
			panic("source recycled/count changed")
		}
		total += count
		sourceBytes += frontier
	}
	emit(map[string]any{"stage": "analysis_done", "points": total, "source_bytes": sourceBytes, "exact": true, "sample_points": samplePoints})
	if streamList != "" {
		for _, token := range strings.Split(streamList, ",") {
			n, err := strconv.Atoi(token)
			must(err)
			if n < 1 || n > 192 {
				panic("streams out of range")
			}
			parallelCodec(sample, n, repeats, false)
			parallelCodec(sample, n, repeats, true)
		}
	}
}

func parallel(fs []partlog.DictionaryFrame, streams, repeats int) {
	parallelCodec(fs, streams, repeats, false)
}

func parallelCodec(fs []partlog.DictionaryFrame, streams, repeats int, sparse bool) {
	if repeats < 1 {
		panic("invalid repeats")
	}
	// One encoder/decoder belongs to each stream; input is immutable and shared.
	// Repeated real source corpus is explicit, not a claim about sustained capture IO.
	runtime.GOMAXPROCS(streams)
	defer runtime.GOMAXPROCS(1)
	var blocks [][]partlog.DictionaryFrame
	start, size := 0, 0
	var points int64
	for i, f := range fs {
		n := 24 + len(f.Key) + len(f.Definition) + 16*len(f.Points)
		if i > start && size+n > 4<<20 {
			blocks = append(blocks, fs[start:i])
			start = i
			size = 0
		}
		size += n
		points += int64(len(f.Points))
	}
	if start < len(fs) {
		blocks = append(blocks, fs[start:])
	}
	encoded := make([][][]byte, streams)
	var wg sync.WaitGroup
	ready := make(chan struct{})
	for worker := range streams {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-ready
			e := partlog.PredictorV3Encoder{SeriesClocks: true, SparseClocks: sparse}
			defer e.Close()
			for range repeats {
				for _, block := range blocks {
					b, err := e.Encode(block)
					must(err)
					encoded[w] = append(encoded[w], b)
				}
			}
		}(worker)
	}
	cpuStart := cpu()
	t := time.Now()
	close(ready)
	wg.Wait()
	encWall := time.Since(t).Seconds()
	encCPU := cpu() - cpuStart
	ready = make(chan struct{})
	for worker := range streams {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-ready
			var d partlog.PredictorV3Decoder
			defer d.Close()
			for i, b := range encoded[w] {
				got, err := d.Decode(b)
				must(err)
				want := blocks[i%len(blocks)]
				if len(got) != len(want) {
					panic("parallel frame count")
				}
				for j, g := range got {
					f := want[j]
					if f.Key != g.Key || f.Clock != g.Clock || !bytes.Equal(f.Definition, g.Definition) || len(f.Points) != len(g.Points) {
						panic("parallel identity")
					}
					for k, p := range f.Points {
						q := g.Points[k]
						if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
							panic("parallel bits")
						}
					}
				}
			}
		}(worker)
	}
	cpuStart = cpu()
	t = time.Now()
	close(ready)
	wg.Wait()
	decWall := time.Since(t).Seconds()
	decCPU := cpu() - cpuStart
	n := points * int64(streams) * int64(repeats)
	emit(map[string]any{"stage": "parallel", "sparse_clocks": sparse, "streams": streams, "gomaxprocs": streams, "points": n, "sample_points": points, "repeats": repeats, "encode_points_per_second": float64(n) / encWall, "decode_and_exact_compare_points_per_second": float64(n) / decWall, "encode_cpu_ns_per_point": float64(encCPU) / float64(n), "decode_compare_cpu_ns_per_point": float64(decCPU) / float64(n), "encode_wall_seconds": encWall, "decode_compare_wall_seconds": decWall, "exact": true, "scope": fmt.Sprintf("%d independent stream instances on repeated real immutable source; codec CPU+alloc, no admission/fsync/IO", streams)})
}
