package inputlog

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/xxh3"

	"github.com/go-graphite/go-carbon/internal/inputlog/loadgen"
	"github.com/go-graphite/go-carbon/points"
)

// simVariant encodes one partition's input of a cycle as raw frame payloads;
// reset starts a new segment.
type simVariant struct {
	name   string
	encode func(entries []Entry, emit func(raw []byte))
	reset  func()
}

// framed cuts a cycle's entries into frames of at most 40000 points, about
// what maxFrameRaw holds with names.
func framed(frame func(entries []Entry, dst []byte) []byte) func([]Entry, func([]byte)) {
	var raw []byte
	return func(entries []Entry, emit func([]byte)) {
		for len(entries) > 0 {
			n := min(len(entries), 40000)
			raw = frame(entries[:n], raw[:0])
			emit(raw)
			entries = entries[n:]
		}
	}
}

// simDictionary assigns segment-wide ids, and keeps each name's last point.
type simDictionary struct {
	ids  map[string]uint32
	last []lastPoint
}

func (s *simDictionary) id(name string, names []byte) (uint32, []byte) {
	if id, ok := s.ids[name]; ok {
		return id, names
	}
	id := uint32(len(s.ids))
	s.ids[name] = id
	s.last = append(s.last, lastPoint{})
	names = binary.AppendUvarint(names, uint64(len(name)))
	return id, append(names, name...)
}

func (s *simDictionary) reset() {
	clear(s.ids)
	s.last = s.last[:0]
}

func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func simVariants() []func() simVariant {
	legacy := func() simVariant {
		var scratch []points.Point
		return simVariant{name: "legacy records", encode: framed(func(entries []Entry, dst []byte) []byte {
			dst, _, scratch = appendRecords(dst, entries, scratch, math.MaxInt)
			return dst
		}), reset: func() {}}
	}
	// (a): records refer to names by id, points stay as they were.
	dictionary := func() simVariant {
		s := &simDictionary{ids: map[string]uint32{}}
		return simVariant{name: "(a) name dictionary", encode: framed(func(entries []Entry, dst []byte) []byte {
			var names, recs []byte
			for _, e := range entries {
				var id uint32
				id, names = s.id(e.Metric(), names)
				recs = binary.AppendUvarint(recs, uint64(id))
				recs = binary.AppendVarint(recs, int64(math.Float64bits(e.Value)))
				recs = binary.AppendVarint(recs, e.Timestamp)
			}
			return append(append(dst, names...), recs...)
		}), reset: s.reset}
	}
	// (b): a frame groups its points by name, with delta timestamps and xor
	// values within the group.
	compact := func() simVariant {
		index := map[string]int{}
		var groups [][]Entry
		return simVariant{name: "(b) compact points in frame", encode: framed(func(entries []Entry, dst []byte) []byte {
			clear(index)
			groups = groups[:0]
			for _, e := range entries {
				g, ok := index[e.Metric()]
				if !ok {
					g = len(groups)
					index[e.Metric()] = g
					groups = append(groups, nil)
				}
				groups[g] = append(groups[g], e)
			}
			for _, g := range groups {
				dst = binary.AppendUvarint(dst, uint64(len(g[0].Metric())))
				dst = append(dst, g[0].Metric()...)
				dst = binary.AppendUvarint(dst, uint64(len(g)))
				prev := lastPoint{}
				for _, e := range g {
					bits := math.Float64bits(e.Value)
					dst = binary.AppendUvarint(dst, zigzag(e.Timestamp-prev.ts))
					dst = appendXor(dst, bits^prev.bits)
					prev = lastPoint{e.Timestamp, bits}
				}
			}
			return dst
		}), reset: func() {}}
	}
	// (a+b) in arrival order: ids, and points against the name's previous
	// point in the segment, as rows.
	unsorted := func() simVariant {
		s := &simDictionary{ids: map[string]uint32{}}
		return simVariant{name: "(a+b) unsorted rows", encode: framed(func(entries []Entry, dst []byte) []byte {
			var names, rows []byte
			for _, e := range entries {
				var id uint32
				id, names = s.id(e.Metric(), names)
				bits := math.Float64bits(e.Value)
				rows = binary.AppendUvarint(rows, uint64(id))
				rows = binary.AppendUvarint(rows, zigzag(e.Timestamp-s.last[id].ts))
				rows = appendXor(rows, bits^s.last[id].bits)
				s.last[id] = lastPoint{e.Timestamp, bits}
			}
			return append(append(dst, names...), rows...)
		}), reset: s.reset}
	}
	// (a+b) sorted by id into columns, points against the previous point of
	// the name in the frame only: no state across frames.
	frameLocal := func() simVariant {
		s := &simDictionary{ids: map[string]uint32{}}
		var rows []row
		return simVariant{name: "(a+b) sorted, frame-local", encode: framed(func(entries []Entry, dst []byte) []byte {
			var names, ids, stamps, values []byte
			rows = rows[:0]
			for _, e := range entries {
				var id uint32
				id, names = s.id(e.Metric(), names)
				rows = append(rows, row{id, e.Timestamp, math.Float64bits(e.Value)})
			}
			slices.SortStableFunc(rows, func(a, b row) int { return int(a.id) - int(b.id) })
			prev := row{}
			for _, r := range rows {
				want := lastPoint{ts: prev.ts}
				if r.id == prev.id {
					want.bits = prev.bits
				}
				ids = binary.AppendUvarint(ids, uint64(r.id-prev.id))
				stamps = binary.AppendUvarint(stamps, zigzag(r.ts-want.ts))
				values = appendXor(values, r.bits^want.bits)
				prev = r
			}
			return append(append(append(append(dst, names...), ids...), stamps...), values...)
		}), reset: s.reset}
	}
	// The log's codec: (a+b) sorted by id into columns, points against the
	// name's previous point in the segment.
	stream := func() simVariant {
		var enc streamEncoder
		var w streamScratch
		var raw []byte
		seq := uint64(1)
		return simVariant{name: "(a+b) stream codec (chosen)", encode: func(entries []Entry, emit func([]byte)) {
			enc.begin(seq)
			for _, e := range entries {
				enc.add(&w, e)
				if len(w.rows) >= maxSortRows {
					raw = enc.flush(&w, raw, func(raw []byte, _ int) []byte { emit(raw); return raw })
				}
			}
			raw = enc.flush(&w, raw, func(raw []byte, _ int) []byte { emit(raw); return raw })
		}, reset: func() { seq++ }}
	}
	return []func() simVariant{legacy, dictionary, compact, unsorted, frameLocal, stream}
}

// TestFormatSimulation compares frame encodings on the load test mix, one
// partition at a time. Run it explicitly:
//
//	INPUTLOG_FORMAT_SIM=1 go test -run TestFormatSimulation -v ./internal/inputlog/
//
// INPUTLOG_SIM_METRICS and INPUTLOG_SIM_INTERVAL set the metrics of the whole
// host and their send interval in cycles; INPUTLOG_SIM_SHUFFLE reorders
// arrivals every cycle; INPUTLOG_SIM_SEGMENT_MB sets the segment size.
func TestFormatSimulation(t *testing.T) {
	if os.Getenv("INPUTLOG_FORMAT_SIM") == "" {
		t.Skip("set INPUTLOG_FORMAT_SIM to run")
	}
	env := func(name string, def int) int {
		if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
			return v
		}
		return def
	}
	hostMetrics, interval := env("INPUTLOG_SIM_METRICS", 4_500_000), env("INPUTLOG_SIM_INTERVAL", 1)
	cycles, segment := env("INPUTLOG_SIM_CYCLES", 60), int64(env("INPUTLOG_SIM_SEGMENT_MB", 1024))<<20
	shuffle := os.Getenv("INPUTLOG_SIM_SHUFFLE") != ""
	// One partition of sixteen: its own metrics, and its share of a segment.
	mix := loadgen.New(hostMetrics, 1)
	var mine []int
	for i, name := range mix.Names {
		if xxh3.Hash(name)&1023>>6 == 5 {
			mine = append(mine, i)
		}
	}
	names := make([]string, len(mix.Names))
	for _, i := range mine {
		names[i] = string(mix.Names[i])
	}
	budget := segment / partitions
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false))
	type result struct {
		bytes, raw, points int64
		elapsed            time.Duration
		segBytes           int64
		resets             int
	}
	variants := simVariants()
	results := make([]result, len(variants))
	encoders := make([]simVariant, len(variants))
	for i, v := range variants {
		encoders[i] = v()
	}
	r := rand.New(rand.NewPCG(9, 9))
	order := append([]int(nil), mine...)
	r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	var entries []Entry
	var out []byte
	for cycle := range cycles {
		if shuffle {
			r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		}
		entries = entries[:0]
		ts := int64(1700000000 + cycle)
		for _, i := range order {
			if i%interval == cycle%interval {
				entries = append(entries, NewStreamRef(names[i]).Entry(mix.Next(i, r), ts))
			}
		}
		for v := range encoders {
			res := &results[v]
			if res.segBytes >= budget {
				encoders[v].reset()
				res.segBytes, res.resets = 0, res.resets+1
			}
			started := time.Now()
			encoders[v].encode(entries, func(raw []byte) {
				out = enc.EncodeAll(raw, out[:0])
				res.raw += int64(len(raw))
				res.bytes += int64(len(out)) + frameHeaderSize
				res.segBytes += int64(len(out)) + frameHeaderSize
			})
			res.elapsed += time.Since(started)
			res.points += int64(len(entries))
		}
	}
	t.Logf("%d metrics on the host (%d in this partition), mean name %.0f B, one point per %d cycles, %d cycles, %d MiB segments, shuffle=%v",
		hostMetrics, len(mine), mix.MeanNameLength(), interval, cycles, segment>>20, shuffle)
	for v, res := range results {
		perPoint := float64(res.bytes) / float64(res.points)
		nsPerPoint := float64(res.elapsed.Nanoseconds()) / float64(res.points)
		t.Logf("%-28s %6.2f B/point (raw %6.1f)  %6.1f MB/s at 4.5M points/s  encode+zstd %4.0f ns/point = %.2f cores  segments %d",
			encoders[v].name, perPoint, float64(res.raw)/float64(res.points), perPoint*4.5e6/1e6, nsPerPoint, nsPerPoint*4.5e6/1e9, res.resets+1)
	}
}
