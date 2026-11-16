// v2-journal compares codecs on the same verified read-only input-log stream.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/partlog"
)

type codec interface {
	Encode([]partlog.DictionaryFrame) ([]byte, error)
	Close()
}
type decoder interface {
	Decode([]byte) ([]partlog.DictionaryFrame, error)
	Close()
}
type measurement struct {
	Codec                                     string `json:"codec"`
	BlockMiB                                  int    `json:"block_mib"`
	Points, Bytes, Blocks, EncodeNS, DecodeNS int64
	frames                                    []partlog.DictionaryFrame
	size                                      int
	encoder                                   codec
	decoder                                   decoder
	seriesClocks                              bool
}

func cpu() int64 {
	var r syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &r))
	return r.Utime.Nano() + r.Stime.Nano()
}
func (m *measurement) flush() {
	if len(m.frames) == 0 {
		return
	}
	start := cpu()
	b, err := m.encoder.Encode(m.frames)
	m.EncodeNS += cpu() - start
	must(err)
	start = cpu()
	got, err := m.decoder.Decode(b)
	m.DecodeNS += cpu() - start
	must(err)
	if len(got) != len(m.frames) {
		panic("frame count")
	}
	for i, f := range m.frames {
		g := got[i]
		if f.Key != g.Key || !bytes.Equal(f.Definition, g.Definition) || f.Clock != g.Clock || len(f.Points) != len(g.Points) {
			panic("identity/clock/order")
		}
		for j, p := range f.Points {
			q := g.Points[j]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				panic("point bits")
			}
		}
		m.Points += int64(len(f.Points))
	}
	m.Bytes += int64(len(b) + 32)
	m.Blocks++
	clear(m.frames)
	m.frames = m.frames[:0]
	m.size = 0
}
func (m *measurement) add(f partlog.DictionaryFrame) {
	// Cuts use canonical decoded bytes: 24B/group + name/definition + 16B/point.
	// Complete apply groups are never split. Actual compressed bytes are charged.
	size := 24 + len(f.Key) + len(f.Definition) + 16*len(f.Points)
	if m.size+size > m.BlockMiB<<20 {
		m.flush()
	}
	m.frames = append(m.frames, f)
	m.size += size
}
func (m *measurement) report() {
	emit(map[string]any{"stage": "codec", "codec": m.Codec, "block_mib": m.BlockMiB, "points": m.Points, "bytes_including_32B_headers": m.Bytes, "blocks": m.Blocks, "bpp": float64(m.Bytes) / float64(m.Points), "encode_ns_per_point": float64(m.EncodeNS) / float64(m.Points), "decode_ns_per_point": float64(m.DecodeNS) / float64(m.Points), "exact": true})
}
func main() {
	dir := flag.String("input", "", "read-only source directory")
	partition := flag.Int("partition", 0, "one independent source partition, 0-15")
	nsegments := flag.Int("segments", 7, "oldest complete segments; newest excluded")
	rate := flag.Float64("read-mbps", 20, "selected source read limit MB/s")
	compareClocks := flag.Bool("compare-series-clocks", false, "measure both global and per-series arrival clock prediction on the same input")
	seriesClocks := flag.Bool("series-clocks", false, "predict arrival clock from the previous apply for the same series")
	independent := flag.Bool("independent-blocks", false, "reset all state per block rather than per segment")
	parallelOnly := flag.Bool("parallel-only", false, "load selected source and benchmark each stream on the complete original corpus")
	analysis := flag.Bool("analysis", false, "field accounting and exact clock delta-of-delta comparison")
	streams := flag.String("streams", "", "analysis parallel codec stream counts, comma separated")
	sample := flag.Int("sample-points", 250000, "real points retained for parallel codec benchmark")
	repeats := flag.Int("repeats", 8, "parallel real-corpus repetitions per stream")
	manifest := flag.Bool("manifest", false, "headers/selection only")
	flag.Parse()
	if *catalogVerifyDir != "" {
		n := catalogVerify(*catalogVerifyDir, *captureStreams)
		emit(map[string]any{"stage": "capture_replay", "points": n, "exact": true})
		return
	}
	if *catalogSender != "" {
		catalogSenderMain()
		return
	}
	runtime.GOMAXPROCS(1)
	if *rate <= 0 || *nsegments <= 0 || *partition < 0 || *partition > 15 {
		panic("invalid flags")
	}
	paths, err := filepath.Glob(filepath.Join(*dir, "*.log"))
	must(err)
	if len(paths) > 0 {
		paths = paths[:len(paths)-1]
	}
	if len(paths) > *nsegments {
		if *analysis || *parallelOnly || *catalogDir != "" {
			paths = paths[len(paths)-*nsegments:]
		} else {
			paths = paths[:*nsegments]
		}
	}
	if len(paths) == 0 {
		panic("no complete segments")
	}
	var segs []segment
	for _, p := range paths {
		s, err := scan(p)
		must(err)
		defer s.f.Close()
		segs = append(segs, s)
	}
	emit(map[string]any{"stage": "manifest", "segments": segs, "partition": *partition, "coverage_lower_bound_seconds": float64(segs[len(segs)-1].Created-segs[0].Created) / 1e9, "full_arrival_nanoseconds": true, "series_clock_prediction": *seriesClocks})
	if *catalogDir != "" {
		catalogProbe(segs, *partition, *rate)
		return
	}
	if *analysis || *parallelOnly {
		analyze(segs, *partition, *rate, *streams, *sample, *repeats, *parallelOnly)
		return
	}
	var pdc2 measurement
	pdc2.Codec = "PDC2"
	pdc2.BlockMiB = 4
	ms := []*measurement{&pdc2}
	for _, sz := range []int{1, 4, 16} {
		ms = append(ms, &measurement{Codec: "PDC3", BlockMiB: sz, encoder: &partlog.PredictorV3Encoder{}, decoder: &partlog.PredictorV3Decoder{}})
	}
	if *compareClocks {
		for _, sz := range []int{1, 4, 16} {
			ms = append(ms, &measurement{Codec: "PDC3-series-clock", BlockMiB: sz, encoder: &partlog.PredictorV3Encoder{}, decoder: &partlog.PredictorV3Decoder{}, seriesClocks: true})
		}
	}
	defer func() {
		for _, m := range ms {
			if m.encoder != nil {
				m.encoder.Close()
			}
			if m.decoder != nil {
				m.decoder.Close()
			}
		}
	}()
	var total, input, groups, singletons, missing, sourceDecodeNS int64
	start := time.Now()
	for _, s := range segs {
		original := make([]byte, 64)
		_, err = s.f.ReadAt(original, 0)
		must(err)
		if !bytes.Equal(original, s.header[:]) {
			panic("source recycled after scan")
		}
		view, frontier, selected, err := selectPartition(s.f, s.Bytes, *partition)
		must(err)
		emit(map[string]any{"stage": "selection", "path": s.Path, "bytes": frontier, "points": selected})
		if *manifest {
			continue
		}
		for _, m := range ms[1:] {
			m.encoder.Close()
			m.decoder.Close()
			m.encoder = &partlog.PredictorV3Encoder{Independent: *independent, SeriesClocks: *seriesClocks || m.seriesClocks}
			m.decoder = &partlog.PredictorV3Decoder{}
		}
		pdc2.encoder = &partlog.DictionaryBlockEncoder{}
		pdc2.decoder = &partlog.DictionaryBlockDecoder{}
		r, err := inputlog.NewSegmentReplay(view)
		must(err)
		var count int64
		for {
			decodeStart := cpu()
			ps, offset, n, err := r.Next(frontier)
			sourceDecodeNS += cpu() - decodeStart
			if err == io.EOF {
				break
			}
			must(err)
			// Match the incumbent probe's PDC2 record cuts and segment dictionary.
			for _, p := range ps {
				f := partlog.DictionaryFrame{Key: p.Metric, Clock: p.FirstArrival, Points: p.Data}
				size := len(p.Metric) + 64 + len(p.Data)*20
				if pdc2.size+size > 4<<20 {
					pdc2.flush()
				}
				pdc2.frames = append(pdc2.frames, f)
				pdc2.size += size
				for _, m := range ms[1:] {
					m.add(f)
				}
				groups++
				if len(p.Data) == 1 {
					singletons++
				}
				if p.FirstArrival == 0 {
					missing += int64(len(p.Data))
				}
			}
			pdc2.flush()
			count += int64(n)
			if delay := time.Duration(float64(input+offset)/(*rate*1e6)*float64(time.Second)) - time.Since(start); delay > 0 {
				time.Sleep(delay)
			}
		}
		for _, m := range ms {
			m.flush()
		}
		pdc2.encoder.Close()
		pdc2.decoder.Close()
		r.Close()
		final := make([]byte, 64)
		_, err = s.f.ReadAt(final, 0)
		must(err)
		if !bytes.Equal(original, final) || count != selected {
			panic("source recycled/count changed")
		}
		total += count
		input += frontier
		emit(map[string]any{"stage": "segment", "path": s.Path, "points": count, "total_points": total, "source_bytes": input, "elapsed_seconds": time.Since(start).Seconds()})
		for _, m := range ms {
			m.report()
		}
	}
	if *manifest {
		return
	}
	for _, m := range ms {
		if m.Points != total {
			panic("codec point count")
		}
	}
	emit(map[string]any{"stage": "done", "source_bytes": input, "points": total, "today_bpp": float64(input) / float64(total), "today_decode_ns_per_point": float64(sourceDecodeNS) / float64(total), "apply_groups": groups, "singleton_groups": singletons, "missing_arrival_points": missing, "elapsed_seconds": time.Since(start).Seconds(), "exact": true})
}
func emit(v any) {
	if m, ok := v.(map[string]any); ok {
		m["when_cet"] = time.Now().In(time.FixedZone("CET", 3600)).Format("2006-01-02 15:04:05")
	}
	must(json.NewEncoder(os.Stdout).Encode(v))
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
