package v2dict

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/zeebo/xxh3"
	"hash/crc32"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"sync"
	"syscall"
)

// A level is an immutable sorted run, published by fsync + rename + directory
// fsync and read through a read-only mapping. L2 stores:
//
// filter version + block entry count (u32) | data blocks | block offsets (u64) | ID column |
// scope table | cache-line Bloom filter + CRC32C | footer
//
// A data block stores its first key uncompressed, its raw length, and a Zstd(3)
// frame of front-coded entries. Binary search compares only uncompressed first
// keys; a lookup decompresses one block. Iterators reuse the current block until
// they advance. Scratch buffers contain no cached blocks across lookups.
// Entries store a tagged varint ID, with policy u16 / flags u8 only if nonzero.
// Dense scopes store one base ID and direct reverse positions; sparse scopes
// retain sorted (ID, position) pairs. The scope table is the only mapped index
// copied to the heap. L1 retains 32-entry uncompressed blocks, seven-byte values
// and explicit reverse pairs. Publication, dictionary WAL and ID assignment are
// common to both formats; an L1-only binary cannot read L2.
// The footer stores data bytes, blocks, entries, scope-table bytes (u64 each),
// then levelMagic. The first-key header uses the same 0 + length + key framing
// as L1, so both versions share the binary-search index reader.
const (
	levelMagic          = "v2dictL2"
	legacyLevelMagic    = "v2dictL1"
	footerSize          = 4*8 + 8
	blockEntries        = 32
	defaultBlockEntries = 128
	filterVersion       = 2 << 16 // cache-line Bloom layout; old experimental L2 rejects it
	valueSize           = 7       // ID u32, policy u16, flags u8, little endian
	// keyBuf is the stack buffer a lookup builds its key in; only a longer
	// key (scope and name) allocates.
	keyBuf = 1024
)

// scope is one (group, vshard) range of a level.
type scope struct {
	key                 string // group 0x00 vshard (2 bytes, big endian)
	first, count, maxID uint32
	idOff               int
	baseID              uint32 // NoID means the reverse column stores explicit IDs
}

// level is a mapped level file. Its mapping is released when no snapshot
// references it, so readers never see an unmapped level.
type level struct {
	m              []byte
	dataLen, nb, n int
	idOff          int
	hashOff        int    // compact: 64-bit live-name hashes in entry order
	filter         []byte // immutable mapped Bloom bits, 10 bits/name
	compact        bool
	blockN         int
	scopes         []scope
	minSeq, maxSeq uint64
	path           string
}

func levelPath(dir string, minSeq, maxSeq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016x-%016x.lvl", minSeq, maxSeq))
}

func (l *level) block(b int) int { return int(binary.LittleEndian.Uint64(l.m[l.dataLen+8*b:])) }

// first returns block b's first key without copying it.
func (l *level) first(b int) []byte {
	off, end := l.block(b), l.dataLen
	if b+1 < l.nb {
		end = l.block(b + 1)
	}
	if l.compact && (off < 4 || end <= off+1 || end > l.dataLen || l.m[off] != 0) {
		panic(errCorrupt)
	}
	size, k := binary.Uvarint(l.m[off+1 : end])
	if l.compact && (k <= 0 || size > uint64(end-off-1-k)) {
		panic(errCorrupt)
	}
	return l.m[off+1+k : off+1+k+int(size)]
}

// Scratch holds only codec workspace, never a cached block or name.
var blockScratch = sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}

func (l *level) readBlock(b int, dst []byte) []byte {
	return l.readBlockWithDecoder(b, dst, nil)
}

func (l *level) readBlockWithDecoder(b int, dst []byte, decoder *scanDecoder) []byte {
	off, end := l.block(b), l.dataLen
	if b+1 < l.nb {
		end = l.block(b + 1)
	}
	if !l.compact {
		return l.m[off:end]
	}
	_ = hook("block-decode")
	first := l.first(b) // validate this frame header on access, never by scanning data at open
	off++
	_, k := binary.Uvarint(l.m[off:end])
	off += k + len(first)
	raw, k := binary.Uvarint(l.m[off:end])
	off += k
	if k <= 0 || raw == 0 || raw > 64<<20 || off >= end {
		panic(errCorrupt)
	}
	if cap(dst) < int(raw) {
		dst = make([]byte, int(raw))
	}
	var out []byte
	var err error
	if decoder == nil {
		out, err = decompressBlock(dst[:int(raw)], l.m[off:end])
	} else {
		out, err = decoder.decode(dst[:int(raw)], l.m[off:end])
	}
	if err != nil || len(out) != int(raw) {
		panic(fmt.Errorf("%w: block %d: %v", errCorrupt, b, err))
	}
	return out
}

func (l *level) fileBytes() int { return len(l.m) }

// levelIter decodes one level forward; key and val are valid after a true
// step. Values are decoded into the iterator; keys reuse its scratch.
type levelIter struct {
	l       *level
	off     int // offset of entry i
	i       int // entries consumed
	key     []byte
	val     []byte
	valid   bool
	value   [valueSize]byte
	data    []byte
	scratch []byte
	decoder *scanDecoder
}

func (it *levelIter) next() bool {
	if it.i >= it.l.n {
		it.valid = false
		return false
	}
	if it.i%it.l.blockN == 0 {
		it.data = it.l.readBlockWithDecoder(it.i/it.l.blockN, it.scratch, it.decoder)
		if it.l.compact {
			it.scratch = it.data
		}
		it.off = 0
	}
	var e Entry
	it.key, e, it.off = it.l.entryData(it.data, it.off, it.key)
	it.val = e.put(it.value[:0])
	it.i++
	it.valid = true
	return true
}

// entry decodes the entry at off onto key, the previous entry's key, and
// returns its key, decoded value and the next offset.
func (l *level) entryData(d []byte, off int, key []byte) ([]byte, Entry, int) {
	shared, k := binary.Uvarint(d[off:])
	off += k
	size, k := binary.Uvarint(d[off:])
	off += k
	key = append(key[:shared], d[off:off+int(size)]...)
	off += int(size)
	e, off := l.valueData(d, off)
	return key, e, off
}

// value decodes the legacy fixed value or an ID with optional metadata.
// Most entries have policy and flags zero, so they store only the tagged ID.
func (l *level) valueData(d []byte, off int) (Entry, int) {
	if !l.compact {
		return decode(d[off:]), off + valueSize
	}
	v, n := binary.Uvarint(d[off:])
	off += n
	e := Entry{ID: uint32(v >> 1)}
	if v&1 != 0 {
		e.Policy = binary.LittleEndian.Uint16(d[off:])
		e.Flags = d[off+2]
		off += 3
	}
	return e, off
}

// seekBlock returns the first block of key's scope that can hold key and
// the scope's end entry; false when the level lacks the scope. The heap scope table narrows the block search to the
// scope's blocks (a few per vshard), and a level without the scope answers
// without touching the mapping.
func (l *level) seekBlock(key []byte) (b, end int, ok bool) {
	sc, ok := l.scope(key[:scopeLen(key)])
	if !ok {
		return 0, 0, false
	}
	lo, hi := int(sc.first)/l.blockN, (int(sc.first+sc.count)-1)/l.blockN+1
	b = lo + sort.Search(hi-lo, func(j int) bool { return bytes.Compare(l.first(lo+j), key) > 0 }) - 1
	return max(b, lo), int(sc.first + sc.count), true
}

// seekGE positions the iterator on the first key >= key of key's scope;
// false when the scope has none.
func (it *levelIter) seekGE(key []byte) bool {
	b, end, ok := it.l.seekBlock(key)
	if !ok {
		it.valid = false
		return false
	}
	it.off, it.i, it.key = 0, b*it.l.blockN, it.key[:0]
	for it.i < end && it.next() {
		if bytes.Compare(it.key, key) >= 0 {
			return true
		}
	}
	it.valid = false
	return false
}

// at returns entry pos's key, decoded onto buf[:0], and value: one block
// decode at most. buf is the caller's scratch; the key may outgrow it.
func (l *level) at(pos int, buf []byte) (key []byte, val Entry) {
	b := pos / l.blockN
	scratch := blockScratch.Get().(*[]byte)
	defer blockScratch.Put(scratch)
	d := l.readBlock(b, *scratch)
	if l.compact {
		*scratch = d
	}
	key, off := buf[:0], 0
	for i := b * l.blockN; i <= pos; i++ {
		key, val, off = l.entryData(d, off, key)
	}
	return key, val
}

// get returns key's value and whether it is present. It decodes no key: each entry is
// compared with key through the bytes it shares with the previous entry
// (match is how many of those equal key's; entries ascend, and a block's
// first entry shares none), so lookups need no key buffer.
func (l *level) get(key []byte) (Entry, bool) {
	return l.getHashed(key, xxh3.Hash(key))
}

func (l *level) getHashed(key []byte, h uint64) (Entry, bool) {
	if !l.mayContainHash(h) {
		return Entry{}, false
	}
	b, end, ok := l.seekBlock(key)
	if !ok {
		return Entry{}, false
	}
	scratch := blockScratch.Get().(*[]byte)
	defer blockScratch.Put(scratch)
	d, off, match := l.readBlock(b, *scratch), 0, 0
	if l.compact {
		*scratch = d
	}
	// A missing key must not scan into the next compressed frame.
	end = min(end, (b+1)*l.blockN)
	for i := b * l.blockN; i < end; i++ {
		shared, k := binary.Uvarint(d[off:])
		off += k
		size, k := binary.Uvarint(d[off:])
		off += k
		suffix := d[off : off+int(size)]
		off += int(size)
		e, next := l.valueData(d, off)
		off = next
		switch s := int(shared); {
		case s < match:
			return Entry{}, false // the entry sorts after key
		case s > match:
			continue // still before key
		}
		c := sharedPrefix(suffix, key[match:])
		match += c
		if c == len(suffix) {
			if match == len(key) {
				return e, true
			}
			continue // a prefix of key: before it
		}
		if match == len(key) || suffix[c] > key[match] {
			return Entry{}, false
		}
	}
	return Entry{}, false
}

func (l *level) scope(key []byte) (scope, bool) {
	i := sort.Search(len(l.scopes), func(i int) bool { return l.scopes[i].key >= string(key) })
	if i < len(l.scopes) && l.scopes[i].key == string(key) {
		return l.scopes[i], true
	}
	return scope{}, false
}

// position returns the entry position of (scope, id), or -1.
func (l *level) position(sk []byte, id uint32) int {
	s, ok := l.scope(sk)
	if !ok || id > s.maxID {
		return -1
	}
	if s.baseID != NoID {
		if id < s.baseID || id-s.baseID >= s.count {
			return -1
		}
		return int(binary.LittleEndian.Uint32(l.m[s.idOff+4*int(id-s.baseID):]))
	}
	col := l.m[s.idOff : s.idOff+8*int(s.count)]
	j := sort.Search(int(s.count), func(j int) bool { return binary.LittleEndian.Uint32(col[8*j:]) >= id })
	if j < int(s.count) && binary.LittleEndian.Uint32(col[8*j:]) == id {
		return int(binary.LittleEndian.Uint32(col[8*j+4:]))
	}
	return -1
}

// levelWriter streams entries in strictly increasing key order to a file
// and keeps only block offsets, the current scope's ID pairs, the ID column
// and the scope table until finish.
type levelWriter struct {
	compact    bool // merges use L2; transient flush runs retain L1
	f          *os.File
	w          *bufio.Writer
	size       int
	n          int
	hashes     []uint64
	liveHashes []uint64
	blocks     []uint64
	blockN     int
	raw        []byte
	firstKey   []byte
	compressed []byte
	prev       []byte
	enc        []byte
	pairs      []uint64 // current scope: id<<32 | position
	idcol      []byte
	stab       []byte
	sk         []byte // current scope key
	first      uint32
	maxID      uint32
	nscope     int
	err        error
}

// reserve uses the input-entry bound already known by flush and merge. Avoid
// growing full-run columns repeatedly while older snapshots remain readable.
func (w *levelWriter) reserve(n int) {
	w.hashes = make([]uint64, 0, n)
	width := 8
	if w.compact {
		w.liveHashes = make([]uint64, 0, n)
		width = 4
	}
	w.idcol = make([]byte, 0, width*n)
	w.blocks = make([]uint64, 0, (n+w.blockN-1)/w.blockN)
}

// scopeLen returns the length of key's scope prefix: group, 0x00, vshard.
func scopeLen(key []byte) int { return bytes.IndexByte(key, 0) + 3 }

func (w *levelWriter) add(key, val []byte) {
	if w.err != nil {
		return
	}
	if w.n > 0 && bytes.Compare(key, w.prev) <= 0 {
		w.err = fmt.Errorf("v2dict: level keys out of order: %q after %q", key, w.prev)
		return
	}
	if sl := scopeLen(key); w.n == 0 || !bytes.Equal(key[:sl], w.sk) {
		w.endScope()
		w.sk, w.first, w.maxID = append(w.sk[:0], key[:sl]...), uint32(w.n), 0
	}
	shared := 0
	if w.n%w.blockN == 0 {
		w.flushBlock()
		w.firstKey = append(w.firstKey[:0], key...)
	} else {
		shared = sharedPrefix(w.prev, key)
	}
	w.enc = binary.AppendUvarint(w.enc[:0], uint64(shared))
	w.enc = binary.AppendUvarint(w.enc, uint64(len(key)-shared))
	w.enc = append(w.enc, key[shared:]...)
	if !w.compact {
		w.enc = append(w.enc, val...)
	} else {
		e := decode(val)
		tagged := uint64(e.ID) << 1
		if e.Policy != 0 || e.Flags != 0 {
			tagged |= 1
		}
		w.enc = binary.AppendUvarint(w.enc, tagged)
		if tagged&1 != 0 {
			w.enc = append(w.enc, val[4:valueSize]...)
		}
	}
	w.raw = append(w.raw, w.enc...)
	id := binary.LittleEndian.Uint32(val)
	w.pairs = append(w.pairs, uint64(id)<<32|uint64(w.n))
	w.maxID = max(w.maxID, id)
	w.prev = append(w.prev[:0], key...)
	h := xxh3.Hash(key)
	w.hashes = append(w.hashes, h)
	if w.compact {
		if decode(val).Deleted() {
			h = 0
		}
		w.liveHashes = append(w.liveHashes, h)
	}
	w.n++
}

func (w *levelWriter) flushBlock() {
	if len(w.raw) == 0 || w.err != nil {
		return
	}
	w.blocks = append(w.blocks, uint64(w.size))
	if !w.compact {
		_, w.err = w.w.Write(w.raw)
		w.size += len(w.raw)
		w.raw = w.raw[:0]
		return
	}
	w.compressed, w.err = compressBlock(w.compressed[:0], w.raw)
	if w.err != nil {
		return
	}
	header := binary.AppendUvarint(w.enc[:0], 0)
	header = binary.AppendUvarint(header, uint64(len(w.firstKey)))
	header = append(header, w.firstKey...)
	header = binary.AppendUvarint(header, uint64(len(w.raw)))
	for _, data := range [][]byte{header, w.compressed} {
		if w.err == nil {
			_, w.err = w.w.Write(data)
			w.size += len(data)
		}
	}
	w.raw = w.raw[:0]
}

func (w *levelWriter) endScope() {
	if len(w.pairs) == 0 {
		return
	}
	slices.Sort(w.pairs)
	count := uint32(len(w.pairs))
	dense := w.compact && uint64(w.maxID)-uint64(w.pairs[0]>>32)+1 == uint64(count)
	baseID := uint32(NoID)
	if dense {
		baseID = uint32(w.pairs[0] >> 32)
	}
	for i, p := range w.pairs {
		if i > 0 && p>>32 == w.pairs[i-1]>>32 {
			w.err = fmt.Errorf("v2dict: duplicate ID %d in scope %q", p>>32, w.sk)
		}
		if !dense {
			w.idcol = binary.LittleEndian.AppendUint32(w.idcol, uint32(p>>32))
		}
		w.idcol = binary.LittleEndian.AppendUint32(w.idcol, uint32(p))
	}
	w.stab = binary.LittleEndian.AppendUint16(w.stab, uint16(len(w.sk)))
	w.stab = append(w.stab, w.sk...)
	w.stab = binary.LittleEndian.AppendUint32(w.stab, w.first)
	w.stab = binary.LittleEndian.AppendUint32(w.stab, count)
	w.stab = binary.LittleEndian.AppendUint32(w.stab, w.maxID)
	if w.compact {
		w.stab = binary.LittleEndian.AppendUint32(w.stab, baseID)
	}
	w.pairs = w.pairs[:0]
	w.nscope++
}

// sharedPrefix compares eight bytes at a time: names share long prefixes.
func sharedPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for ; i+8 <= n; i += 8 {
		if x := binary.LittleEndian.Uint64(a[i:]) ^ binary.LittleEndian.Uint64(b[i:]); x != 0 {
			return i + bits.TrailingZeros64(x)/8
		}
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// testHook, when set, is called at named crash points; an error stops the
// operation there, leaving the files as a kill would.
var testHook func(point string) error

func hook(point string) error {
	if testHook != nil {
		return testHook(point)
	}
	return nil
}

// writeLevel writes the entries fill adds to a new level file and maps it.
func writeLevel(path string, minSeq, maxSeq uint64, fill func(*levelWriter)) (*level, error) {
	return writeLevelKind(path, minSeq, maxSeq, true, fill)
}

// Flush runs are short-lived L1 files; merges publish compact L2 files.
func writeLevelKind(path string, minSeq, maxSeq uint64, compact bool, fill func(*levelWriter)) (*level, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &levelWriter{f: f, w: bufio.NewWriterSize(f, 256<<10), compact: compact, blockN: blockEntries}
	// L2 records its block entry count; L1 always used 32.
	if compact {
		w.blockN = defaultBlockEntries
		w.size = 4
		_, w.err = w.w.Write(make([]byte, 4))
	}
	fill(w)
	w.endScope()
	w.flushBlock()
	// Short-lived L1 files keep this filter only in memory. Their disk format
	// remains readable by L1 readers; reopened L1 files use exact probes.
	filter := make([]byte, filterBytes(w.n))
	for _, h := range w.hashes {
		bloomAdd(filter, h)
	}
	if w.err == nil {
		tail := make([]byte, 0, 8*len(w.blocks))
		for _, b := range w.blocks {
			tail = binary.LittleEndian.AppendUint64(tail, b)
		}
		var hashes []byte
		if compact {
			hashes = make([]byte, 0, 8*w.n)
			for _, h := range w.liveHashes {
				hashes = binary.LittleEndian.AppendUint64(hashes, h)
			}
		}
		sections := [][]byte{tail, hashes, w.idcol, w.stab}
		if compact {
			checksum := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(filter, castagnoli))
			sections = append(sections, filter, checksum)
		}
		for _, sec := range sections {
			if _, err := w.w.Write(sec); err != nil && w.err == nil {
				w.err = err
			}
		}
		var foot []byte
		for _, v := range []int{w.size, len(w.blocks), w.n, len(w.stab)} {
			foot = binary.LittleEndian.AppendUint64(foot, uint64(v))
		}
		magic := legacyLevelMagic
		if compact {
			magic = levelMagic
		}
		foot = append(foot, magic...)
		if _, err := w.w.Write(foot); err != nil && w.err == nil {
			w.err = err
		}
	}
	if w.err == nil {
		w.err = w.w.Flush()
		if w.err == nil && compact {
			var header [4]byte
			binary.LittleEndian.PutUint32(header[:], uint32(w.blockN)|filterVersion)
			_, w.err = f.WriteAt(header[:], 0)
		}
	}
	if w.err == nil {
		w.err = hook("level-written")
	}
	if w.err == nil {
		w.err = f.Sync()
	}
	if cerr := f.Close(); w.err == nil {
		w.err = cerr
	}
	if w.err == nil {
		w.err = os.Rename(tmp, path)
	}
	if w.err == nil {
		w.err = syncDir(filepath.Dir(path))
	}
	if w.err != nil {
		os.Remove(tmp)
		return nil, w.err
	}
	l, err := openLevel(path, minSeq, maxSeq)
	if err == nil && !compact {
		l.filter = filter
	}
	return l, err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

var errCorrupt = errors.New("v2dict: corrupt level file")

// openLevel maps a level file read-only and validates its layout.
func openLevel(path string, minSeq, maxSeq uint64) (*level, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(st.Size())
	if size < footerSize {
		return nil, fmt.Errorf("%w: %s: truncated", errCorrupt, path)
	}
	m, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	foot := m[size-footerSize:]
	u := func(i int) int { return int(binary.LittleEndian.Uint64(foot[8*i:])) }
	l := &level{compact: string(foot[32:]) == levelMagic, m: m, dataLen: u(0), nb: u(1), n: u(2), minSeq: minSeq, maxSeq: maxSeq, path: path}
	l.blockN = blockEntries
	if l.compact && l.dataLen >= 4 && l.dataLen <= size-footerSize {
		l.blockN = int(binary.LittleEndian.Uint32(m) & 0xffff)
	}
	stabLen := u(3)
	l.hashOff = l.dataLen + 8*l.nb
	l.idOff = l.hashOff
	if l.compact {
		l.idOff += 8 * l.n
	}
	stabOff := size - footerSize - stabLen
	if l.compact && l.n >= 0 {
		end := size - footerSize
		start := end - filterBytes(l.n) - 4
		if start < l.idOff || start > end-4 {
			syscall.Munmap(m)
			return nil, errCorrupt
		}
		l.filter = m[start : end-4]
		if crc32.Checksum(l.filter, castagnoli) != binary.LittleEndian.Uint32(m[end-4:end]) {
			syscall.Munmap(m)
			return nil, errCorrupt
		}
		stabOff = start - stabLen
	}
	colOff := l.idOff
	bad := (!l.compact && string(foot[32:]) != legacyLevelMagic) || l.dataLen < 0 || l.n < 0 || l.blockN < 1 || l.blockN > 4096 || (l.blockN > 0 && l.nb != (l.n+l.blockN-1)/l.blockN) ||
		stabLen < 0 || stabOff < l.idOff || (!l.compact && stabOff != l.idOff+8*l.n)
	if l.compact && !bad {
		bad = l.dataLen < 4 || l.dataLen > size-footerSize || l.idOff < l.dataLen || l.idOff > stabOff || binary.LittleEndian.Uint32(m)&0xffff0000 != filterVersion
		previous := 3
		for b := 0; !bad && b < l.nb; b++ {
			off := l.block(b)
			if off <= previous || off >= l.dataLen {
				bad = true
				break
			}
			end := l.dataLen
			if b+1 < l.nb {
				end = l.block(b + 1)
			}
			if end <= off || end > l.dataLen {
				bad = true
				break
			}
			previous = off
		}
	}
	for p := stabOff; !bad && p < stabOff+stabLen; {
		if p+2 > stabOff+stabLen {
			bad = true
			break
		}
		kl := int(binary.LittleEndian.Uint16(m[p:]))
		rowSize := 12
		if l.compact {
			rowSize = 16
		}
		if p+2+kl+rowSize > stabOff+stabLen {
			bad = true
			break
		}
		s := scope{key: string(m[p+2 : p+2+kl]), idOff: colOff, baseID: NoID}
		p += 2 + kl
		s.first, s.count, s.maxID = binary.LittleEndian.Uint32(m[p:]), binary.LittleEndian.Uint32(m[p+4:]), binary.LittleEndian.Uint32(m[p+8:])
		p += 12
		if l.compact {
			s.baseID = binary.LittleEndian.Uint32(m[p:])
			p += 4
		}
		if s.baseID != NoID {
			colOff += 4 * int(s.count)
			if uint64(s.baseID)+uint64(s.count) != uint64(s.maxID)+1 {
				bad = true
				break
			}
		} else {
			colOff += 8 * int(s.count)
		}
		bad = colOff > stabOff || int(s.first)+int(s.count) > l.n || (len(l.scopes) > 0 && l.scopes[len(l.scopes)-1].key >= s.key)
		l.scopes = append(l.scopes, s)
	}
	bad = bad || colOff != stabOff
	if bad {
		syscall.Munmap(m)
		return nil, fmt.Errorf("%w: %s", errCorrupt, path)
	}
	// Validation reads immutable metadata, including the filter checksum. Release
	// those mapping references before publishing; the page cache stays intact.
	if l.compact {
		if err := releaseLevelPages(m); err != nil {
			syscall.Munmap(m)
			return nil, err
		}
	}
	runtime.AddCleanup(l, func(m []byte) { syscall.Munmap(m) }, m)
	return l, nil
}

// Newly written L1 has a transient heap filter; L2 persists its filter.
// Seven double-hashed probes use a stable on-disk hash;
// collisions only cause exact block reads, never false negatives. The checksum
// makes a damaged filter fail openLevel rather than hide a persisted name.
func filterBytes(n int) int { return max(64, (n*10+511)/512*64) }
func bloomAdd(filter []byte, h uint64) {
	base := int((h>>32)%uint64(len(filter)/64)) * 64
	x, delta := bits.RotateLeft64(h, 31), h|1
	for range 7 {
		bit := x & 511
		filter[base+int(bit/8)] |= 1 << (bit % 8)
		x += delta
	}
}
func (l *level) mayContain(key []byte) bool { return l.mayContainHash(xxh3.Hash(key)) }

func (l *level) mayContainHash(h uint64) bool {
	if len(l.filter) == 0 {
		return true
	}
	base := int((h>>32)%uint64(len(l.filter)/64)) * 64
	x, delta := bits.RotateLeft64(h, 31), h|1
	for range 7 {
		bit := x & 511
		if l.filter[base+int(bit/8)]&(1<<(bit%8)) == 0 {
			return false
		}
		x += delta
	}
	return true
}
