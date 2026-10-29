package recovery

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/go-graphite/go-carbon/points"
)

// A cache dump of FormatBlocks (cache-blocks.<pid>.<ns>.bin) holds the
// records of FormatCompact without their names. Metric names are most of a
// compact dump's bytes when metrics hold few points each: on one store, 495
// of 553 bytes per metric. Each dump segment instead becomes one or more
// blocks that name their records once, sorted, front coded and compressed:
//
//	header   magic "GCNB", codec, records, raw and compressed names length,
//	         records length, payload crc32c, header crc32c (32 bytes, little
//	         endian; the crcs are Castagnoli)
//	names    zstd of the records' names, sorted: per name a uvarint of the
//	         bytes it shares with the previous name, a uvarint of the bytes
//	         that follow, then those bytes
//	records  per record, in the order of the names:
//	         uvarint point count, uvarint encoded length, encoded points
//	         (see FormatCompact)
//
// The payload crc covers the names and the records. A metric's records keep
// their order within a block, and a metric is in one segment only, so an
// ordered restore replays each metric's records in order. It stops at a torn
// or corrupt block without applying any of its records.
//
// A checkpoint addresses records by offset and length, reconstructed from
// the blocks for GCPOINT3 or stored in older indexes. Opening the checkpoint
// decompresses every block's names into memory, with every nameRestart-th
// name kept whole, so a record's name is found by decoding at most
// nameRestart-1 names.
const (
	blockMagic      = 0x424e4347 // "GCNB"
	blockHeaderSize = 32
	blockCodecZstd  = 1
	// maxBlockNames bounds the names a reader decompresses for one block:
	// blockNameBytes plus one name.
	maxBlockNames = 16<<20 + points.MB + 2*binary.MaxVarintLen64
	// nameRestart trades memory for lookups: every nameRestart-th name is
	// kept whole, so a lookup decodes at most nameRestart-1 names. With 8,
	// claims read names about as fast as from a compact dump.
	nameRestart = 8
)

// blockRecordBytes and blockNameBytes end a block once its records or its
// front-coded names reach them. A segment holds one cache shard, normally
// well below both. Tests lower them.
var (
	blockRecordBytes = 16 << 20
	blockNameBytes   = 16 << 20
)

var errBlock = errors.New("invalid cache dump block")

type blockHeader struct {
	records    int
	namesRaw   int
	namesLen   int
	recordsLen int
	crc        uint32
}

func (h blockHeader) append(dst []byte) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, blockMagic)
	dst = append(dst, blockCodecZstd, 0, 0, 0)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(h.records))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(h.namesRaw))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(h.namesLen))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(h.recordsLen))
	dst = binary.LittleEndian.AppendUint32(dst, h.crc)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

// parseBlockHeader checks a header's own consistency and bounds.
func parseBlockHeader(b []byte) (blockHeader, error) {
	if len(b) < blockHeaderSize || binary.LittleEndian.Uint32(b) != blockMagic || b[4] != blockCodecZstd ||
		b[5]|b[6]|b[7] != 0 || binary.LittleEndian.Uint32(b[28:]) != crc32.Checksum(b[:28], castagnoli) {
		return blockHeader{}, errBlock
	}
	records := uint64(binary.LittleEndian.Uint32(b[8:]))
	namesRaw := uint64(binary.LittleEndian.Uint32(b[12:]))
	namesLen := uint64(binary.LittleEndian.Uint32(b[16:]))
	recordsLen := uint64(binary.LittleEndian.Uint32(b[20:]))
	// Every record holds a point and is named; a repeated name takes two
	// bytes, the first at least three. Check before converting to int so
	// malformed 32-bit extents cannot wrap on 32-bit builds.
	if records == 0 || namesRaw < 2*records+1 || namesRaw > maxBlockNames || recordsLen < 3*records ||
		recordsLen > math.MaxInt32 || namesLen == 0 || namesLen > namesRaw+namesRaw/8+1024 {
		return blockHeader{}, errBlock
	}
	return blockHeader{records: int(records), namesRaw: int(namesRaw), namesLen: int(namesLen),
		recordsLen: int(recordsLen), crc: binary.LittleEndian.Uint32(b[24:])}, nil
}

// blockRef is a record waiting in a segment for its block. data is either
// copied into the worker's body or borrowed from an immutable cache item.
type blockRef struct {
	name  string
	data  []byte
	n     int
	order int
}

func (r blockRef) size() int {
	return len(r.data) + (bits.Len(uint(r.n)|1)+6)/7 + (bits.Len(uint(len(r.data))|1)+6)/7
}

func (r blockRef) header(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(r.n))
	return binary.AppendUvarint(dst, uint64(len(r.data)))
}

func (r blockRef) appendTo(dst []byte) []byte { return append(r.header(dst), r.data...) }

// blockScratch is a dump worker's memory for the blocks of one segment at a
// time.
type blockScratch struct {
	refs  []blockRef
	body  []byte
	names []byte
	z     []byte
	head  []byte
}

var blockScratchPool = sync.Pool{New: func() any { return new(blockScratch) }}

// add copies a compact record of metric into the segment's body.
func (b *blockScratch) add(metric string, data []byte, n int) {
	off := len(b.body)
	b.body = append(b.body, data...)
	b.borrow(metric, b.body[off:], n)
}

// borrow retains data until the segment's blocks are encoded. Its caller
// must keep the bytes immutable until then.
func (b *blockScratch) borrow(metric string, data []byte, n int) {
	b.refs = append(b.refs, blockRef{name: metric, data: data, n: n, order: len(b.refs)})
}

func (b *blockScratch) reset() {
	clear(b.refs) // drop names and borrowed point data
	b.refs, b.body = b.refs[:0], b.body[:0]
	if cap(b.body) > 64<<20 {
		b.body = nil
	}
}

// sharedPrefix returns the length of the common prefix of a and b. Sorted
// neighbours share hundreds of bytes, so it compares in large steps first.
func sharedPrefix(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i+64 <= n && a[i:i+64] == b[i:i+64] {
		i += 64
	}
	for i+8 <= n && a[i:i+8] == b[i:i+8] {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// appendBlocks sorts the segment's records by metric, keeping each metric's
// records in order, and appends them as blocks through emit, which receives
// each block's header and compressed names, then its records. emit writes
// the records and fills both CRC fields from the completed output. head is
// reused after emit returns. The scratch is reset afterwards.
func (b *blockScratch) appendBlocks(enc *zstd.Encoder, emit func(head []byte, records []blockRef) error) error {
	defer b.reset()
	refs := b.refs
	slices.SortFunc(refs, func(x, y blockRef) int {
		if c := strings.Compare(x.name, y.name); c != 0 {
			return c
		}
		return cmp.Compare(x.order, y.order)
	})
	for start := 0; start < len(refs); {
		names := b.names[:0]
		recordsLen, prev := 0, ""
		end := start
		for end < len(refs) && recordsLen < blockRecordBytes && len(names) < blockNameBytes {
			r := refs[end]
			shared := 0
			if end > start {
				shared = sharedPrefix(prev, r.name)
			}
			names = binary.AppendUvarint(names, uint64(shared))
			names = binary.AppendUvarint(names, uint64(len(r.name)-shared))
			names = append(names, r.name[shared:]...)
			recordsLen += r.size()
			prev = r.name
			end++
		}
		b.names = names
		if recordsLen > math.MaxInt32 {
			return fmt.Errorf("cache dump record too large")
		}
		b.z = enc.EncodeAll(names, b.z[:0])
		// The emitter fills the payload and header checksums from its
		// completed output buffers, one CRC update per buffer rather than
		// one per record and its transient varint header.
		b.head = blockHeader{records: end - start, namesRaw: len(names), namesLen: len(b.z), recordsLen: recordsLen}.append(b.head[:0])
		b.head = append(b.head, b.z...)
		if err := emit(b.head, refs[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// newBlockEncoder returns the names encoder of a dump written by up to
// parallel workers.
func newBlockEncoder(parallel int) *zstd.Encoder {
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(parallel),
		zstd.WithEncoderCRC(false), zstd.WithLowerEncoderMem(true))
	return enc
}

// ReadBlocksFile visits the records of a FormatBlocks cache dump in order,
// for an ordered restore. The points passed to visit own their storage.
func ReadBlocksFile(path string, visit func(*points.Points)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return ReadBlocks(f, visit)
}

// ReadBlocks is ReadBlocksFile on a reader. Each block's crc and names are
// verified before any of its records is visited.
func ReadBlocks(r io.Reader, visit func(*points.Points)) error {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.IgnoreChecksum(true))
	if err != nil {
		return err
	}
	defer dec.Close()
	br := bufio.NewReaderSize(r, points.MB)
	header := make([]byte, blockHeaderSize)
	var payload, names, name []byte
	for {
		if _, err := io.ReadFull(br, header); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return unexpected(err)
		}
		h, err := parseBlockHeader(header)
		if err != nil {
			return err
		}
		size := h.namesLen + h.recordsLen
		payload = slices.Grow(payload[:0], size)[:size]
		if _, err = io.ReadFull(br, payload); err != nil {
			return unexpected(err)
		}
		if crc32.Checksum(payload, castagnoli) != h.crc {
			return errBlock
		}
		names, err = dec.DecodeAll(payload[:h.namesLen], slices.Grow(names[:0], h.namesRaw))
		if err != nil || len(names) != h.namesRaw {
			return errBlock
		}
		if err = checkNames(names, h.records, nil); err != nil {
			return err
		}
		records := payload[h.namesLen:]
		pos := 0
		for range h.records {
			var shared, suffix int
			if shared, suffix, pos, err = nextName(names, pos, len(name)); err != nil {
				return err
			}
			name = append(name[:shared], names[pos-suffix:pos]...)
			count, k := binary.Uvarint(records)
			if k <= 0 || count == 0 || count > uint64(len(records)) {
				return errCompact
			}
			records = records[k:]
			length, k := binary.Uvarint(records)
			if k <= 0 || length > uint64(len(records)-k) {
				return errCompact
			}
			data := records[k : k+int(length)]
			records = records[k+int(length):]
			values, _, err := decodeCompact(make([]points.Point, 0, count), data, int(count), false)
			if err != nil {
				return err
			}
			visit(&points.Points{Metric: string(name), Data: values})
		}
		if len(records) != 0 {
			return errBlock
		}
	}
}

// nextName decodes the front-coded name at pos, after a name of prevLen
// bytes. It returns the shared and suffix lengths and the position after
// the name; the suffix is names[next-suffix:next].
func nextName(names []byte, pos, prevLen int) (shared, suffix, next int, err error) {
	s, k := binary.Uvarint(names[pos:])
	if k <= 0 || s > uint64(prevLen) {
		return 0, 0, 0, errBlock
	}
	pos += k
	l, k := binary.Uvarint(names[pos:])
	if k <= 0 || l > uint64(len(names)-pos-k) || s+l == 0 || s+l > points.MB {
		return 0, 0, 0, errBlock
	}
	pos += k + int(l)
	return int(s), int(l), pos, nil
}

// checkNames verifies that names holds exactly count front-coded names in
// sorted order, each sharing as much as it can with the one before, and
// calls visit, if set, with each name, the bytes it shares with the one
// before and the position after it. A name repeats when its metric has
// several records.
func checkNames(names []byte, count int, visit func(i int, name []byte, shared, next int) error) error {
	var cur []byte
	pos := 0
	for i := range count {
		shared, suffix, next, err := nextName(names, pos, len(cur))
		if err != nil {
			return err
		}
		s := names[next-suffix : next]
		// Past the shared prefix the name must continue with a greater
		// byte; otherwise the previous name is a prefix of it, or equal.
		if i > 0 && shared < len(cur) && (suffix == 0 || s[0] <= cur[shared]) {
			return errBlock
		}
		cur = append(cur[:shared], s...)
		pos = next
		if visit != nil {
			if err = visit(i, cur, shared, pos); err != nil {
				return err
			}
		}
	}
	if pos != len(names) {
		return errBlock
	}
	return nil
}

// nameBlock is one block's names, as a checkpoint open keeps them: in
// groups of nameRestart, each a uvarint length and the group's first name,
// then the others front coded as in the file. A lookup reads one group.
type nameBlock struct {
	group []uint32 // offset of each group in data
	data  []byte
	// For a GCPOINT3 index, which lists no records: where the block's
	// records start, each record's offset from there (and the end), and
	// which records are named like the record before them.
	base   uint64
	starts []uint32
	repeat []uint64
}

// nameTable holds the names of a FormatBlocks cache dump's records.
type nameTable struct {
	first  []uint64 // record id of each block's first record
	blocks []nameBlock
	runs   uint64 // GCPOINT3: runs of records named alike
}

// locate returns the block holding record id and the record's ordinal in it.
func (t *nameTable) locate(id uint64) (*nameBlock, uint64) {
	i, found := slices.BinarySearch(t.first, id)
	if !found {
		i--
	}
	return &t.blocks[i], id - t.first[i]
}

// record returns record id's extent and, as its previous record, the record
// before it if both are named alike: a GCPOINT3 index's record table.
func (t *nameTable) record(id uint64) record {
	b, o := t.locate(id)
	r := record{offset: b.base + uint64(b.starts[o]), length: uint64(b.starts[o+1] - b.starts[o])}
	if b.repeat[o/64]&(1<<(o%64)) != 0 {
		r.previous = id - 1
	}
	return r
}

// repeats reports whether record id is named like the record before it.
func (t *nameTable) repeats(id uint64) bool {
	b, o := t.locate(id)
	return b.repeat[o/64]&(1<<(o%64)) != 0
}

// group returns the group holding record id, which must be in the table,
// its whole first name, the position after that name, and how many names
// follow it up to the record's.
func (t *nameTable) group(id uint64) (data, whole []byte, pos, skip int) {
	b, o := t.locate(id)
	ord := int(o)
	pos = int(b.group[ord/nameRestart])
	l, k := binary.Uvarint(b.data[pos:])
	pos += k
	return b.data, b.data[pos : pos+int(l)], pos + int(l), ord % nameRestart
}

// appendName appends the name of record id, which must be in the table.
func (t *nameTable) appendName(dst []byte, id uint64) []byte {
	data, whole, pos, skip := t.group(id)
	base := len(dst)
	dst = append(dst, whole...)
	for range skip {
		s, k := binary.Uvarint(data[pos:])
		pos += k
		l, k := binary.Uvarint(data[pos:])
		pos += k
		dst = append(dst[:base+int(s)], data[pos:pos+int(l)]...)
		pos += int(l)
	}
	return dst
}

// is reports whether record id is named metric, without copying the name:
// it tracks how much of metric the names decoded so far match.
func (t *nameTable) is(id uint64, metric string) bool {
	data, whole, pos, skip := t.group(id)
	matched, length := prefixLen(whole, metric), len(whole)
	for range skip {
		s, k := binary.Uvarint(data[pos:])
		pos += k
		l, k := binary.Uvarint(data[pos:])
		pos += k
		// The name keeps s bytes of the previous one. Past a mismatch
		// within them it mismatches at the same byte.
		if int(s) <= matched {
			matched = int(s) + prefixLen(data[pos:pos+int(l)], metric[s:])
		}
		length = int(s + l)
		pos += int(l)
	}
	return matched == len(metric) && length == len(metric)
}

// prefixLen returns the length of the common prefix of a and b.
func prefixLen(a []byte, b string) int {
	n := min(len(a), len(b))
	if string(a[:n]) == b[:n] {
		return n
	}
	i := 0
	for i+64 <= n && string(a[i:i+64]) == b[i:i+64] {
		i += 64
	}
	for i+8 <= n && string(a[i:i+8]) == b[i:i+8] {
		i += 8
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// loadBlocks decompresses and checks the names of a FormatBlocks cache dump
// and checks its records against the index's: each block's records must be
// the next records of the index, at the offsets and with the lengths they
// have in the block. Names must be canonical, as for every checkpoint.
func (in *Index) loadBlocks(cache []byte) error {
	type span struct {
		h     blockHeader
		at    int // offset of the names
		first uint64
	}
	var spans []span
	first := uint64(1)
	for at := 0; at < len(cache); {
		h, err := parseBlockHeader(cache[at:])
		if err != nil {
			return err
		}
		if h.namesLen+h.recordsLen > len(cache)-at-blockHeaderSize {
			return errBlock
		}
		spans = append(spans, span{h: h, at: at + blockHeaderSize, first: first})
		first += uint64(h.records)
		at += blockHeaderSize + h.namesLen + h.recordsLen
	}
	if first-1 != in.recordCounts[0] {
		return fmt.Errorf("recovery index and cache dump blocks differ")
	}
	t := &nameTable{first: make([]uint64, len(spans)), blocks: make([]nameBlock, len(spans))}
	for i := range spans {
		t.first[i] = spans[i].first
	}
	workers := min(32, runtime.GOMAXPROCS(0), max(len(spans), 1))
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(workers), zstd.IgnoreChecksum(true))
	if err != nil {
		return err
	}
	defer dec.Close()
	errs := make([]error, workers)
	scratch := make([]loadScratch, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := w; i < len(spans); i += workers {
				if errs[w] = in.loadBlock(dec, cache, spans[i].h, spans[i].at, spans[i].first, &t.blocks[i], &scratch[w]); errs[w] != nil {
					return
				}
			}
		})
	}
	wg.Wait()
	if err = errors.Join(errs...); err != nil {
		return err
	}
	in.names = t
	if in.v3 {
		// A metric's records may continue into the next block.
		var last []byte
		for i := 1; i < len(t.blocks); i++ {
			last = t.appendName(last[:0], t.first[i]-1)
			if t.is(t.first[i], string(last)) {
				t.blocks[i].repeat[0] |= 1
			}
		}
		t.runs = in.recordCounts[0]
		for i := range t.blocks {
			for _, word := range t.blocks[i].repeat {
				t.runs -= uint64(bits.OnesCount64(word))
			}
		}
	}
	return nil
}

// loadScratch is a loading worker's memory for one block at a time.
type loadScratch struct{ names, data []byte }

func (in *Index) loadBlock(dec *zstd.Decoder, cache []byte, h blockHeader, at int, first uint64, b *nameBlock, scratch *loadScratch) error {
	names, err := dec.DecodeAll(cache[at:at+h.namesLen], scratch.names[:0])
	scratch.names = names
	if err != nil || len(names) != h.namesRaw {
		return errBlock
	}
	b.group = make([]uint32, 0, (h.records+nameRestart-1)/nameRestart)
	if in.v3 {
		b.repeat = make([]uint64, (h.records+63)/64)
	}
	data := scratch.data[:0]
	pos, prevLen := 0, 0
	err = checkNames(names, h.records, func(i int, name []byte, shared, next int) error {
		if b.repeat != nil && i > 0 {
			shared, k := binary.Uvarint(names[pos:])
			if suffix, _ := binary.Uvarint(names[pos+k:]); suffix == 0 && int(shared) == prevLen {
				b.repeat[i/64] |= 1 << (i % 64) // the previous name again
			}
		}
		prevLen = len(name)
		// Canonical names must match filesystem spelling before pending
		// reads open.
		if !canonicalAfter(name, shared) {
			return fmt.Errorf("pending reads require canonical metric names")
		}
		if i%nameRestart == 0 {
			if len(data) > math.MaxUint32 {
				return errBlock
			}
			b.group = append(b.group, uint32(len(data)))
			data = binary.AppendUvarint(data, uint64(len(name)))
			data = append(data, name...)
		} else {
			data = append(data, names[pos:next]...)
		}
		pos = next
		return nil
	})
	scratch.data = data
	if err != nil {
		return err
	}
	b.data = slices.Clone(data)
	pos = at + h.namesLen
	end := pos + h.recordsLen
	if in.v3 {
		// The index lists no records: the block defines them.
		b.base = uint64(pos)
		b.starts = make([]uint32, h.records+1)
		for o := range h.records {
			b.starts[o] = uint32(pos - at - h.namesLen)
			count, k := binary.Uvarint(cache[pos:end])
			if k <= 0 || count == 0 {
				return errCompact
			}
			size, m := binary.Uvarint(cache[pos+k : end])
			if m <= 0 || size > uint64(end-pos-k-m) || size < count {
				return errCompact
			}
			pos += k + m + int(size)
		}
		b.starts[h.records] = uint32(pos - at - h.namesLen)
		if pos != end {
			return errBlock
		}
		return nil
	}
	for id := first; id < first+uint64(h.records); id++ {
		r := in.record(0, id)
		if r.offset != uint64(pos) || r.length == 0 || r.length > uint64(end-pos) || r.previous >= id {
			return fmt.Errorf("invalid recovery record chain")
		}
		if _, _, err = blockBody(cache[pos : pos+int(r.length)]); err != nil {
			return err
		}
		pos += int(r.length)
	}
	if pos != end {
		return fmt.Errorf("unindexed recovery source bytes")
	}
	return nil
}

// canonicalAfter is canonical for a name whose first shared bytes are those
// of a canonical name: only its other bytes need checking, and the byte
// before them for "..".
func canonicalAfter(name []byte, shared int) bool {
	if shared == 0 {
		return canonical(name)
	}
	rest := name[shared:]
	return name[len(name)-1] != '.' && !bytes.Contains(name[shared-1:], []byte("..")) &&
		bytes.IndexByte(rest, '/') < 0 && bytes.IndexByte(rest, 0) < 0
}

// blockBody returns a block record's point count and encoded points.
func blockBody(raw []byte) (int, []byte, error) {
	count, k := binary.Uvarint(raw)
	if k <= 0 || count == 0 || count > uint64(len(raw)) {
		return 0, nil, errCompact
	}
	size, m := binary.Uvarint(raw[k:])
	if m <= 0 || size != uint64(len(raw)-k-m) {
		return 0, nil, errCompact
	}
	return int(count), raw[k+m:], nil
}

// VisitMetrics visits every slot holding points once, with its name. Over a
// FormatBlocks dump it goes in file order, decoding names one after the
// other and reading records as they lie, so it claims a dump faster than
// random lookups; then come metrics without records in the dump. Otherwise
// it goes in slot order. visit owns name.
func (in *Index) VisitMetrics(visit func(slot uint64, name string) error) error {
	if in.names == nil {
		for slot := range in.Slots() {
			name, found, err := in.Name(slot)
			if err != nil {
				return err
			}
			if found {
				if err = visit(slot, name); err != nil {
					return err
				}
			}
		}
		return nil
	}
	heads := in.headSlots()
	var cur []byte
	for i := range in.names.blocks {
		b := &in.names.blocks[i]
		id := in.names.first[i]
		for g, at := range b.group {
			end := len(b.data)
			if g+1 < len(b.group) {
				end = int(b.group[g+1])
			}
			l, k := binary.Uvarint(b.data[at:])
			pos := int(at) + k
			cur = append(cur[:0], b.data[pos:pos+int(l)]...)
			pos += int(l)
			for {
				if slot := heads[id-1]; slot != 0 {
					if err := visit(uint64(slot-1), string(cur)); err != nil {
						return err
					}
				}
				id++
				if pos == end {
					break
				}
				s, k := binary.Uvarint(b.data[pos:])
				pos += k
				l, k := binary.Uvarint(b.data[pos:])
				pos += k
				cur = append(cur[:s], b.data[pos:pos+int(l)]...)
				pos += int(l)
			}
		}
	}
	// Metrics without dump records, and any slot the pass above missed.
	for slot := range in.Slots() {
		_, h := in.slot(slot)
		if h.count == 0 || (h.cache != 0 && uint64(heads[h.cache-1]) == slot+1) {
			continue
		}
		name, _, err := in.Name(slot)
		if err != nil {
			return err
		}
		if err = visit(slot, name); err != nil {
			return err
		}
	}
	return nil
}

// headSlots maps each dump record that heads a slot's chain to the slot
// plus one.
func (in *Index) headSlots() []uint32 {
	heads := make([]uint32, in.recordCounts[0])
	workers := min(32, runtime.GOMAXPROCS(0), max(int(in.slots>>16), 1))
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for slot := in.slots * uint64(w) / uint64(workers); slot < in.slots*uint64(w+1)/uint64(workers); slot++ {
				if _, h := in.slot(slot); h.cache != 0 && h.count != 0 {
					heads[h.cache-1] = uint32(slot + 1)
				}
			}
		})
	}
	wg.Wait()
	return heads
}
