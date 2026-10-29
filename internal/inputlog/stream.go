package inputlog

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/points"
)

// One partition's frames in one segment form a stream. The first frame of a
// stream that uses a name defines it; every row refers to its name by a small
// id, and encodes its timestamp and value against the previous point of the
// same name in the stream. Rows are sorted by id, so each name's rows are
// adjacent in recorded order. A codec 2 payload, before zstd:
//
//	uvarint    frame index in the stream
//	uvarint    first id defined here: the names earlier frames defined
//	uvarint    names defined here, then each: uvarint length, bytes
//	uvarint    rows
//	uvarint    bytes of the id column, then of the timestamp column
//	ids        uvarint of id minus the previous row's id
//	timestamps zigzag varint of timestamp minus the name's previous one
//	values     value bits xor the name's previous value bits, see appendXor
//
// A name's first row in a stream has no previous point: it is predicted from
// the previous row's timestamp and a zero value.
//
// A stream depends only on frames of its own segment, which replay decodes
// from the segment's first frame. A recycled segment's frames from an earlier
// use carry another salt, so the scan ends before them and their names are
// never read. A frame that does not verify, or does not continue its stream,
// ends the stream for the rest of the segment: its later frames could refer
// to names or values that frame held.
const codecStream = 2

// maxRowBytes bounds an encoded row: id, timestamp and value.
const maxRowBytes = binary.MaxVarintLen32 + binary.MaxVarintLen64 + 9

// maxSortRows bounds the rows a worker sorts at once, and so its memory:
// about 14 MB. A partition's cycle at 4.5M points/s fits in one sort, which
// keeps the id column regular; a quarter of it costs 20% more bytes.
const maxSortRows = 1 << 18

type row struct {
	id   uint32
	ts   int64
	bits uint64
}

type lastPoint struct {
	ts   int64
	bits uint64
}

// streamEncoder is a partition's encoding state for the segment seq.
type streamEncoder struct {
	seq    uint64
	frames uint64
	// epoch identifies this stream among every stream encoded by the
	// process, so a StreamRef's id never outlives its stream.
	epoch uint32
	ids   map[string]uint32
	last  []lastPoint // by id, for every name a written frame defined
}

// streamEpochs numbers streams; zero is never used, as unset refs hold it.
var streamEpochs atomic.Uint32

func nextEpoch() uint32 {
	for {
		if e := streamEpochs.Add(1); e != 0 {
			return e
		}
	}
}

// begin starts a frame batch for segment seq, resetting the state when the
// partition's frames go to a new segment.
func (s *streamEncoder) begin(seq uint64) {
	if s.ids == nil {
		s.ids = make(map[string]uint32)
	}
	if s.seq != seq || s.epoch == 0 {
		s.seq, s.frames, s.last = seq, 0, s.last[:0]
		s.epoch = nextEpoch()
		clear(s.ids)
	}
}

// streamScratch is a worker's memory for encoding one partition at a time.
type streamScratch struct {
	rows, tmp []row
	names     []byte // definitions of ids from len(last) on, in id order
	nameEnds  []int
	cols      [3][]byte
}

// add assigns e's name an id, defining new names in scratch. An entry whose
// ref was given an id in this stream already carries it.
func (s *streamEncoder) add(w *streamScratch, e Entry) {
	var id uint32
	cached := e.cached
	if uint32(cached>>32) != s.epoch {
		// Earlier entries in this cycle may already have assigned the id.
		cached = e.Ref.state.Load()
	}
	if uint32(cached>>32) == s.epoch {
		id = uint32(cached)
	} else {
		id = s.lookup(w, e.Ref)
	}
	w.rows = append(w.rows, row{id: id, ts: e.Timestamp, bits: math.Float64bits(e.Value)})
}

// lookup returns ref's id in the stream, defining its name if new, and
// remembers the id in ref.
func (s *streamEncoder) lookup(w *streamScratch, ref *StreamRef) uint32 {
	id, ok := s.ids[ref.Metric]
	if !ok {
		id = uint32(len(s.ids))
		s.ids[ref.Metric] = id
		w.names = binary.AppendUvarint(w.names, uint64(len(ref.Metric)))
		w.names = append(w.names, ref.Metric...)
		w.nameEnds = append(w.nameEnds, len(w.names))
	}
	ref.state.Store(uint64(s.epoch)<<32 | uint64(id))
	return id
}

// flush encodes the rows added since the previous flush as frames, calling
// emit with each raw payload and its row count.
func (s *streamEncoder) flush(w *streamScratch, raw []byte, emit func(raw []byte, rows int) []byte) []byte {
	if len(w.rows) == 0 {
		return raw
	}
	w.rows, w.tmp = sortRows(w.rows, w.tmp, uint32(len(s.ids)-1))
	// firstNew is the first id this batch defines; offset(i) is where the
	// definition of id firstNew+i starts in w.names.
	firstNew := len(s.last)
	offset := func(i int) int {
		if i == 0 {
			return 0
		}
		return w.nameEnds[i-1]
	}
	for rows := w.rows; len(rows) > 0; {
		// Cut a frame at maxFrameRaw, counting each row at its largest. Rows
		// are sorted, so a name's first row is the next undefined id.
		base := len(s.last)
		n, size, defined := 0, 0, 0
		for ; n < len(rows) && size < maxFrameRaw; n++ {
			size += maxRowBytes
			if int(rows[n].id) == base+defined {
				size += w.nameEnds[base-firstNew+defined] - offset(base-firstNew+defined)
				defined++
			}
		}
		names := w.names[offset(base-firstNew):offset(base-firstNew+defined)]
		raw = emit(s.encodeFrame(w, rows[:n], defined, names, raw[:0]), n)
		rows = rows[n:]
	}
	w.rows, w.names, w.nameEnds = w.rows[:0], w.names[:0], w.nameEnds[:0]
	return raw
}

func (s *streamEncoder) encodeFrame(w *streamScratch, rows []row, defined int, names, dst []byte) []byte {
	dst = binary.AppendUvarint(dst, s.frames)
	dst = binary.AppendUvarint(dst, uint64(len(s.last)))
	dst = binary.AppendUvarint(dst, uint64(defined))
	dst = append(dst, names...)
	ids, stamps, values := w.cols[0][:0], w.cols[1][:0], w.cols[2][:0]
	prev := row{}
	for _, r := range rows {
		ids = binary.AppendUvarint(ids, uint64(r.id-prev.id))
		want := lastPoint{ts: prev.ts}
		if int(r.id) < len(s.last) {
			want = s.last[r.id]
		} else {
			s.last = append(s.last, lastPoint{})
		}
		stamps = binary.AppendVarint(stamps, r.ts-want.ts)
		values = appendXor(values, r.bits^want.bits)
		s.last[r.id] = lastPoint{ts: r.ts, bits: r.bits}
		prev = r
	}
	dst = binary.AppendUvarint(dst, uint64(len(rows)))
	dst = binary.AppendUvarint(dst, uint64(len(ids)))
	dst = binary.AppendUvarint(dst, uint64(len(stamps)))
	dst = append(append(append(dst, ids...), stamps...), values...)
	w.cols[0], w.cols[1], w.cols[2] = ids, stamps, values
	s.frames++
	return dst
}

// sortRows sorts rows by id, keeping the order of equal ids: a radix sort of
// 11-bit digits, two passes below four million names. Rows that arrive in id
// order already, as a fixed sending order produces, are left as they are.
func sortRows(rows, tmp []row, maxID uint32) ([]row, []row) {
	sorted := true
	for i := 1; i < len(rows) && sorted; i++ {
		sorted = rows[i-1].id <= rows[i].id
	}
	if sorted {
		return rows, tmp
	}
	const digit = 11
	for shift := 0; shift == 0 || maxID>>shift > 0; shift += digit {
		var counts [1<<digit + 1]int
		for _, r := range rows {
			counts[(r.id>>shift)&(1<<digit-1)+1]++
		}
		for i := 1; i < len(counts); i++ {
			counts[i] += counts[i-1]
		}
		if cap(tmp) < len(rows) {
			tmp = make([]row, len(rows), 2*len(rows))
		}
		tmp = tmp[:len(rows)]
		for _, r := range rows {
			d := (r.id >> shift) & (1<<digit - 1)
			tmp[counts[d]] = r
			counts[d]++
		}
		rows, tmp = tmp, rows
	}
	return rows, tmp
}

// appendXor encodes x as a header byte and its significant bytes, most
// significant first: 0 for x == 0, else 1 + 8*leading zero bytes + (count-1).
func appendXor(dst []byte, x uint64) []byte {
	if x == 0 {
		return append(dst, 0)
	}
	lead, trail := bits.LeadingZeros64(x)/8, bits.TrailingZeros64(x)/8
	n := 8 - lead - trail
	dst = append(dst, byte(1+lead*8+n-1))
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, byte(x>>(8*(trail+i))))
	}
	return dst
}

var errStream = errors.New("input log frame does not continue its stream")

// streamDecoder is a partition's decoding state for one segment.
type streamDecoder struct {
	seq    uint64
	frames uint64
	broken bool
	names  []string
	last   []lastPoint
	data   []points.Point
	groups []group
}

// group is a run of rows of one name in a decoded frame.
type group struct {
	id    uint32
	start int
}

// begin switches to the stream of segment seq.
func (d *streamDecoder) begin(seq uint64) {
	if d.seq != seq {
		d.seq, d.frames, d.broken = seq, 0, false
		d.names, d.last = nil, d.last[:0]
	}
}

// decode reads a codec 2 payload, and passes each name's points to visit
// unless visit is nil. It returns the number of points. A frame that fails
// is not applied at all.
func (d *streamDecoder) decode(raw []byte, visit func(*points.Points)) (int, error) {
	r := reader{data: raw}
	if r.uvarint() != d.frames || r.uvarint() != uint64(len(d.names)) || r.err != nil {
		return 0, errStream
	}
	for defined := r.uvarint(); defined > 0 && r.err == nil; defined-- {
		d.names = append(d.names, string(r.bytes(r.uvarint())))
	}
	count, idsLength, stampsLength := r.uvarint(), r.uvarint(), r.uvarint()
	ids := reader{data: r.bytes(idsLength)}
	stamps := reader{data: r.bytes(stampsLength)}
	values := r
	if r.err != nil || count > uint64(len(raw)) {
		return 0, errFrame
	}
	d.data, d.groups = d.data[:0], d.groups[:0]
	id, prev := uint64(0), lastPoint{}
	for i := range int(count) {
		next := id + ids.uvarint()
		if next >= uint64(len(d.names)) || next > uint64(len(d.last)) {
			return 0, errFrame
		}
		if i == 0 || next != id {
			d.groups = append(d.groups, group{id: uint32(next), start: i})
		}
		id = next
		want := lastPoint{ts: prev.ts}
		if id < uint64(len(d.last)) {
			want = d.last[id]
		} else {
			d.last = append(d.last, lastPoint{})
		}
		prev = lastPoint{ts: want.ts + stamps.varint(), bits: want.bits ^ values.xor()}
		d.last[id] = prev
		d.data = append(d.data, points.Point{Value: math.Float64frombits(prev.bits), Timestamp: prev.ts})
	}
	if ids.err != nil || stamps.err != nil || values.err != nil || len(ids.data)+len(stamps.data)+len(values.data) > 0 {
		return 0, errFrame
	}
	d.frames++
	if visit != nil {
		for i, g := range d.groups {
			end := len(d.data)
			if i+1 < len(d.groups) {
				end = d.groups[i+1].start
			}
			visit(&points.Points{Metric: d.names[g.id], Data: append([]points.Point(nil), d.data[g.start:end]...)})
		}
	}
	return int(count), nil
}

// reader decodes a payload, remembering the first error.
type reader struct {
	data []byte
	err  error
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.data)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.data = r.data[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.data)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.data = r.data[n:]
	return v
}

func (r *reader) bytes(n uint64) []byte {
	if n > uint64(len(r.data)) {
		r.fail()
		return nil
	}
	b := r.data[:n]
	r.data = r.data[n:]
	return b
}

func (r *reader) xor() uint64 {
	h := r.bytes(1)
	if len(h) == 0 || h[0] == 0 {
		return 0
	}
	lead, n := int(h[0]-1)/8, int(h[0]-1)%8+1
	if lead+n > 8 {
		r.fail()
		return 0
	}
	var x uint64
	for _, b := range r.bytes(uint64(n)) {
		x = x<<8 | uint64(b)
	}
	return x << (8 * (8 - lead - n))
}

func (r *reader) fail() {
	if r.err == nil {
		r.err = errFrame
	}
	r.data = nil
}
