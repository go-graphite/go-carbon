package recovery

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// A compact input file holds the input a dump stop diverts while it writes a
// compact checkpoint (see FormatCompact): input-compact.<pid>.<ns>.bin.
// Releases without it ignore the file. It is a sequence of frames, each
//
//	header   magic "GCWF", partition, codec, rows, raw length, payload
//	         length and payload crc32c (24 bytes, little endian)
//	payload  zstd of the raw frame:
//
//	uvarint  names defined here, then each: uvarint id, uvarint length, name
//	uvarint  rows, then bytes of the id column and of the timestamp column
//	ids      uvarint of id minus the previous row's id; rows sorted by id
//	stamps   zigzag varint of timestamp minus the previous row's
//	values   value bits xor the previous row's of the same id, or zero, as
//	         the input log encodes them (appendXor)
//
// A metric's id is its checkpoint builder id, so the checkpoint index maps it
// to a slot without names; the file still defines every name the first time
// it uses an id, so it restores without the index. Ids are split by id%16
// into partitions; a partition's frames hold its rows in arrival order, so
// a metric's points keep their order. Frames end at a torn or corrupt frame.
const (
	inputFrameMagic      = 0x46574347 // "GCWF"
	inputFrameHeaderSize = 24
	inputPartitions      = 16
	// inputFrameRows bounds the rows a partition buffers before encoding a
	// frame, and so the work left when input stops.
	inputFrameRows = 1 << 15
	// maxInputFrameRaw bounds a frame before compression.
	maxInputFrameRaw = 64 << 20
)

// CompactInputPrefix starts the name of a compact input file.
const CompactInputPrefix = "input-compact."

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// InputName names the input file of format f with the given suffix.
func InputName(f Format, suffix string) string {
	if f == FormatCompact {
		return CompactInputPrefix + suffix
	}
	return "input." + suffix
}

// InputFormat reports the format of the input file called name.
func InputFormat(name string) Format {
	if strings.HasPrefix(name, CompactInputPrefix) {
		return FormatCompact
	}
	return FormatLegacy
}

// IsDumpFile reports whether name is a dump or input file of some generation.
func IsDumpFile(name string) bool { return isGeneration(name) }

type walRow struct {
	id   uint32
	ts   int64
	bits uint64
}

type inputPart struct {
	mu      sync.Mutex
	rows    []walRow
	defs    []byte
	ndefs   int
	defined []uint64 // bit id/inputPartitions: the file defines that id
	closed  bool

	// emit is held while a frame is encoded and appended, and is taken
	// before mu is released, so frames go out in the order rows came in. It
	// guards spare and spareDf, the buffers of the frame before.
	emit    sync.Mutex
	spare   []walRow
	spareDf []byte
}

// frameScratch is the memory of one frame encoding, shared by partitions.
type frameScratch struct {
	tmp   []walRow
	cols  [3][]byte
	raw   []byte
	frame []byte
}

var frameScratchPool = sync.Pool{New: func() any { return new(frameScratch) }}

// compactInput is a Writer's state for a compact input file.
type compactInput struct {
	builder atomic.Pointer[Builder] // nil once closed
	parts   [inputPartitions]inputPart
	enc     *zstd.Encoder
}

func newCompactInput(builder *Builder) *compactInput {
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(inputPartitions), zstd.WithEncoderCRC(false))
	in := &compactInput{enc: enc}
	in.builder.Store(builder)
	return in
}

func (in *compactInput) write(w *Writer, p *points.Points) error {
	builder := in.builder.Load()
	if builder == nil {
		return os.ErrClosed
	}
	id, err := builder.ID(p.Metric)
	if err != nil {
		return err
	}
	part := &in.parts[id%inputPartitions]
	part.mu.Lock()
	if part.closed {
		part.mu.Unlock()
		return os.ErrClosed
	}
	word, bit := int(id/inputPartitions/64), id/inputPartitions%64
	if word >= len(part.defined) {
		part.defined = append(part.defined, make([]uint64, word+1-len(part.defined))...)
	}
	if part.defined[word]&(1<<bit) == 0 {
		part.defined[word] |= 1 << bit
		part.defs = binary.AppendUvarint(part.defs, uint64(id))
		part.defs = binary.AppendUvarint(part.defs, uint64(len(p.Metric)))
		part.defs = append(part.defs, p.Metric...)
		part.ndefs++
	}
	for _, point := range p.Data {
		part.rows = append(part.rows, walRow{id: id, ts: point.Timestamp, bits: math.Float64bits(point.Value)})
	}
	if len(part.rows) < inputFrameRows && len(part.defs) < maxInputFrameRaw/2 {
		part.mu.Unlock()
		return nil
	}
	return in.emitLocked(w, part, int(id%inputPartitions))
}

// emitLocked encodes part's buffered rows as a frame and appends it. It is
// called with part.mu held and releases it. It waits for the partition's
// previous frame before swapping buffers, so the spares it hands to writers
// are no longer being encoded.
func (in *compactInput) emitLocked(w *Writer, part *inputPart, p int) error {
	part.emit.Lock()
	defer part.emit.Unlock()
	rows, defs, ndefs := part.rows, part.defs, part.ndefs
	part.rows, part.defs, part.ndefs = part.spare[:0], part.spareDf[:0], 0
	part.mu.Unlock()
	var err error
	if len(rows) > 0 || ndefs > 0 {
		sc := frameScratchPool.Get().(*frameScratch)
		sc.frame = in.encodeFrame(sc, p, rows, defs, ndefs, sc.frame[:0])
		err = w.appendFrame(sc.frame)
		frameScratchPool.Put(sc)
	}
	part.spare, part.spareDf = rows[:0], defs[:0]
	return err
}

func (in *compactInput) encodeFrame(part *frameScratch, p int, rows []walRow, defs []byte, ndefs int, dst []byte) []byte {
	rows, part.tmp = sortWalRows(rows, part.tmp)
	raw := binary.AppendUvarint(part.raw[:0], uint64(ndefs))
	raw = append(raw, defs...)
	ids, stamps, values := part.cols[0][:0], part.cols[1][:0], part.cols[2][:0]
	var prev walRow
	for i, r := range rows {
		ids = binary.AppendUvarint(ids, uint64(r.id-prev.id))
		stamps = binary.AppendVarint(stamps, r.ts-prev.ts)
		var want uint64
		if i > 0 && r.id == prev.id {
			want = prev.bits
		}
		values = appendXorBits(values, r.bits^want)
		prev = r
	}
	raw = binary.AppendUvarint(raw, uint64(len(rows)))
	raw = binary.AppendUvarint(raw, uint64(len(ids)))
	raw = binary.AppendUvarint(raw, uint64(len(stamps)))
	raw = append(append(append(raw, ids...), stamps...), values...)
	part.cols[0], part.cols[1], part.cols[2], part.raw = ids, stamps, values, raw
	start := len(dst)
	dst = append(dst, make([]byte, inputFrameHeaderSize)...)
	dst = in.enc.EncodeAll(raw, dst)
	payload := dst[start+inputFrameHeaderSize:]
	h := dst[start:]
	binary.LittleEndian.PutUint32(h, inputFrameMagic)
	h[4], h[5] = byte(p), 1
	binary.LittleEndian.PutUint32(h[8:], uint32(len(rows)))
	binary.LittleEndian.PutUint32(h[12:], uint32(len(raw)))
	binary.LittleEndian.PutUint32(h[16:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(h[20:], crc32.Checksum(payload, castagnoli))
	return dst
}

// close emits every partition's remaining rows and refuses later writes.
func (in *compactInput) close(w *Writer) error {
	var errs error
	for p := range in.parts {
		part := &in.parts[p]
		part.mu.Lock()
		part.closed = true
		errs = errors.Join(errs, in.emitLocked(w, part, p))
	}
	in.enc.Close()
	in.builder.Store(nil)
	return errs
}

// sortWalRows sorts rows by id, keeping the order of equal ids: a radix sort
// of 11-bit digits. The sorted rows may be in either buffer; the scratch it
// returns for reuse is never rows, which the caller gives back to its
// partition.
func sortWalRows(rows, tmp []walRow) (sorted, scratch []walRow) {
	maxID := uint32(0)
	inOrder := true
	for i, r := range rows {
		maxID = max(maxID, r.id)
		if i > 0 && rows[i-1].id > r.id {
			inOrder = false
		}
	}
	if inOrder {
		return rows, tmp
	}
	const digit = 11
	if cap(tmp) < len(rows) {
		tmp = make([]walRow, len(rows))
	}
	scratch = tmp[:len(rows)]
	src, dst := rows, scratch
	for shift := 0; shift == 0 || maxID>>shift > 0; shift += digit {
		var counts [1<<digit + 1]int
		for _, r := range src {
			counts[(r.id>>shift)&(1<<digit-1)+1]++
		}
		for i := 1; i < len(counts); i++ {
			counts[i] += counts[i-1]
		}
		for _, r := range src {
			d := (r.id >> shift) & (1<<digit - 1)
			dst[counts[d]] = r
			counts[d]++
		}
		src, dst = dst, src
	}
	return src, scratch
}

// appendXorBits encodes x as a header byte and its significant bytes, most
// significant first: 0 for x == 0, else 1 + 8*leading zero bytes + (count-1).
func appendXorBits(dst []byte, x uint64) []byte {
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

var errInputFrame = errors.New("invalid compact input frame")

type inputFrameHeader struct {
	partition       int
	rows, rawLength int
	payload         []byte
}

// parseInputFrame returns the frame at the start of data and its size.
func parseInputFrame(data []byte) (inputFrameHeader, int, error) {
	if len(data) < inputFrameHeaderSize || binary.LittleEndian.Uint32(data) != inputFrameMagic || data[5] != 1 || int(data[4]) >= inputPartitions {
		return inputFrameHeader{}, 0, errInputFrame
	}
	h := inputFrameHeader{partition: int(data[4]), rows: int(binary.LittleEndian.Uint32(data[8:])), rawLength: int(binary.LittleEndian.Uint32(data[12:]))}
	length := int(binary.LittleEndian.Uint32(data[16:]))
	if length > len(data)-inputFrameHeaderSize || h.rawLength > maxInputFrameRaw+maxInputFrameRaw/2 {
		return inputFrameHeader{}, 0, errInputFrame
	}
	h.payload = data[inputFrameHeaderSize : inputFrameHeaderSize+length]
	if crc32.Checksum(h.payload, castagnoli) != binary.LittleEndian.Uint32(data[20:]) {
		return inputFrameHeader{}, 0, errInputFrame
	}
	return h, inputFrameHeaderSize + length, nil
}

// decodeInputFrame decompresses a frame into raw and calls def for each name
// it defines and row for each row, in order. The name passed to def borrows
// raw.
func decodeInputFrame(dec *zstd.Decoder, h inputFrameHeader, raw []byte, def func(id uint32, name []byte) error, row func(id uint32, ts int64, bits uint64) error) ([]byte, error) {
	raw, err := dec.DecodeAll(h.payload, raw[:0])
	if err != nil || len(raw) != h.rawLength {
		return raw, errInputFrame
	}
	r := reader{data: raw}
	for n := r.uvarint(); n > 0 && r.err == nil; n-- {
		id := r.uvarint()
		name := r.bytes(r.uvarint())
		if r.err != nil || id > math.MaxUint32 || int(id%inputPartitions) != h.partition {
			return raw, errInputFrame
		}
		if err = def(uint32(id), name); err != nil {
			return raw, err
		}
	}
	count, idsLength, stampsLength := r.uvarint(), r.uvarint(), r.uvarint()
	ids := reader{data: r.bytes(idsLength)}
	stamps := reader{data: r.bytes(stampsLength)}
	values := r
	if r.err != nil || count != uint64(h.rows) {
		return raw, errInputFrame
	}
	var id uint64
	var ts int64
	var prev uint64
	for i := range count {
		delta := ids.uvarint()
		next := id + delta
		if next > math.MaxUint32 || int(next%inputPartitions) != h.partition {
			return raw, errInputFrame
		}
		if i == 0 || delta != 0 {
			prev = 0
		}
		id = next
		ts += stamps.varint()
		prev ^= values.xor()
		if ids.err != nil || stamps.err != nil || values.err != nil {
			return raw, errInputFrame
		}
		if err = row(uint32(id), ts, prev); err != nil {
			return raw, err
		}
	}
	if len(ids.data)+len(stamps.data)+len(values.data) > 0 {
		return raw, errInputFrame
	}
	return raw, nil
}

// ReadCompactInputFile visits the points of a compact input file in order,
// for an ordered restore. A metric's points of one frame are visited
// together; the points passed to visit own their storage.
func ReadCompactInputFile(path string, visit func(*points.Points)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return ReadCompactInput(f, visit)
}

// ReadCompactInput is ReadCompactInputFile on a reader.
func ReadCompactInput(r io.Reader, visit func(*points.Points)) error {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.IgnoreChecksum(true))
	if err != nil {
		return err
	}
	defer dec.Close()
	br := bufio.NewReaderSize(r, points.MB)
	names := make(map[uint32]string)
	var frame, raw []byte
	var run *points.Points
	runID := uint32(0)
	flush := func() {
		if run != nil {
			visit(run)
			run = nil
		}
	}
	for {
		header, err := br.Peek(inputFrameHeaderSize)
		if len(header) == 0 && errors.Is(err, io.EOF) {
			return nil
		}
		if len(header) < inputFrameHeaderSize {
			return io.ErrUnexpectedEOF
		}
		size := inputFrameHeaderSize + int(binary.LittleEndian.Uint32(header[16:]))
		if binary.LittleEndian.Uint32(header) != inputFrameMagic || size > 2*maxInputFrameRaw {
			return errInputFrame
		}
		frame = slices.Grow(frame[:0], size)[:size]
		if _, err = io.ReadFull(br, frame); err != nil {
			return unexpected(err)
		}
		h, _, err := parseInputFrame(frame)
		if err != nil {
			return err
		}
		raw, err = decodeInputFrame(dec, h, raw, func(id uint32, name []byte) error {
			if _, ok := names[id]; ok {
				return fmt.Errorf("compact input id %d defined twice", id)
			}
			names[id] = string(name)
			return nil
		}, func(id uint32, ts int64, bits uint64) error {
			if run == nil || id != runID {
				flush()
				name, ok := names[id]
				if !ok {
					return fmt.Errorf("compact input id %d used before its definition", id)
				}
				run, runID = &points.Points{Metric: name}, id
			}
			run.Data = append(run.Data, points.Point{Value: math.Float64frombits(bits), Timestamp: ts})
			return nil
		})
		flush()
		if err != nil {
			return err
		}
	}
}

// reader decodes a frame, remembering the first error.
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
		r.err = errInputFrame
	}
	r.data = nil
}
