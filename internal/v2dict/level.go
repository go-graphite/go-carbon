package v2dict

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"syscall"
)

// A level file is an immutable sorted run of dictionary entries, written once
// (temporary name, fsync, rename, directory fsync) and read through a
// read-only mapping. The entry encoding is v2index's base file (LevelDB data
// blocks, VictoriaMetrics mergeset items): each entry stores the length of
// the key prefix it shares with the previous key, the rest of the key, then
// the fixed 7-byte value; every block of blockEntries entries starts with a
// whole key, so a seek binary-searches block starts and decodes at most one
// block. Unlike v2index, the block offsets stay in the file (no heap copy),
// and two sections follow them:
//
//	entries | block offsets (u64 each) | ID column | scope table | footer
//
// The ID column holds, per scope and sorted by ID, one (ID u32, position u32)
// pair per entry: ID -> name is a binary search plus one block decode. The
// scope table lists each scope's key, first position, entry count and
// highest ID; it is the only part copied to the heap (one row per scope).
// The footer is the entry bytes, blocks, entries and scope-table bytes (u64
// each), then levelMagic.
const (
	levelMagic   = "v2dictL1"
	footerSize   = 4*8 + 8
	blockEntries = 32
	valueSize    = 7 // ID u32, policy u16, flags u8, little endian
	// keyBuf is the stack buffer a lookup builds its key in; only a longer
	// key (scope and name) allocates.
	keyBuf = 1024
)

// scope is one (group, vshard) range of a level.
type scope struct {
	key                 string // group 0x00 vshard (2 bytes, big endian)
	first, count, maxID uint32
}

// level is a mapped level file. Its mapping is released when no snapshot
// references it, so readers never see an unmapped level.
type level struct {
	m              []byte
	dataLen, nb, n int
	idOff          int
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
	off := l.block(b) + 1 // shared length is always 0
	size, k := binary.Uvarint(l.m[off:])
	return l.m[off+k : off+k+int(size)]
}

func (l *level) fileBytes() int { return len(l.m) }

// levelIter decodes one level forward; key and val are valid after a true
// step (val points into the mapping).
type levelIter struct {
	l     *level
	off   int // offset of entry i
	i     int // entries consumed
	key   []byte
	val   []byte
	valid bool
}

func (it *levelIter) next() bool {
	if it.i >= it.l.n {
		it.valid = false
		return false
	}
	it.key, it.val, it.off = it.l.entry(it.off, it.key)
	it.i++
	it.valid = true
	return true
}

// entry decodes the entry at off onto key, the previous entry's key, and
// returns its key, its value (pointing into the mapping) and the next offset.
func (l *level) entry(off int, key []byte) ([]byte, []byte, int) {
	d := l.m
	shared, k := binary.Uvarint(d[off:])
	off += k
	size, k := binary.Uvarint(d[off:])
	off += k
	key = append(key[:shared], d[off:off+int(size)]...)
	off += int(size)
	return key, d[off : off+valueSize : off+valueSize], off + valueSize
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
	lo, hi := int(sc.first)/blockEntries, (int(sc.first+sc.count)-1)/blockEntries+1
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
	it.off, it.i, it.key = it.l.block(b), b*blockEntries, it.key[:0]
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
func (l *level) at(pos int, buf []byte) (key, val []byte) {
	b := pos / blockEntries
	key, off := buf[:0], l.block(b)
	for i := b * blockEntries; i <= pos; i++ {
		key, val, off = l.entry(off, key)
	}
	return key, val
}

// get returns key's value, or nil. It decodes no key: each entry is
// compared with key through the bytes it shares with the previous entry
// (match is how many of those equal key's; entries ascend, and a block's
// first entry shares none), so lookups need no key buffer.
func (l *level) get(key []byte) []byte {
	b, end, ok := l.seekBlock(key)
	if !ok {
		return nil
	}
	d, off, match := l.m, l.block(b), 0
	for i := b * blockEntries; i < end; i++ {
		shared, k := binary.Uvarint(d[off:])
		off += k
		size, k := binary.Uvarint(d[off:])
		off += k
		suffix := d[off : off+int(size)]
		off += int(size) + valueSize
		switch s := int(shared); {
		case s < match:
			return nil // the entry sorts after key
		case s > match:
			continue // still before key
		}
		c := sharedPrefix(suffix, key[match:])
		match += c
		if c == len(suffix) {
			if match == len(key) {
				return d[off-valueSize : off : off]
			}
			continue // a prefix of key: before it
		}
		if match == len(key) || suffix[c] > key[match] {
			return nil
		}
	}
	return nil
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
	col := l.m[l.idOff+8*int(s.first) : l.idOff+8*int(s.first+s.count)]
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
	f      *os.File
	w      *bufio.Writer
	size   int
	n      int
	blocks []uint64
	prev   []byte
	enc    []byte
	pairs  []uint64 // current scope: id<<32 | position
	idcol  []byte
	stab   []byte
	sk     []byte // current scope key
	first  uint32
	maxID  uint32
	nscope int
	err    error
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
	if w.n%blockEntries == 0 {
		w.blocks = append(w.blocks, uint64(w.size))
	} else {
		shared = sharedPrefix(w.prev, key)
	}
	w.enc = binary.AppendUvarint(w.enc[:0], uint64(shared))
	w.enc = binary.AppendUvarint(w.enc, uint64(len(key)-shared))
	w.enc = append(w.enc, key[shared:]...)
	w.enc = append(w.enc, val[:valueSize]...)
	_, w.err = w.w.Write(w.enc)
	w.size += len(w.enc)
	id := binary.LittleEndian.Uint32(val)
	w.pairs = append(w.pairs, uint64(id)<<32|uint64(w.n))
	w.maxID = max(w.maxID, id)
	w.prev = append(w.prev[:0], key...)
	w.n++
}

func (w *levelWriter) endScope() {
	if len(w.pairs) == 0 {
		return
	}
	slices.Sort(w.pairs)
	for i, p := range w.pairs {
		if i > 0 && p>>32 == w.pairs[i-1]>>32 {
			w.err = fmt.Errorf("v2dict: duplicate ID %d in scope %q", p>>32, w.sk)
		}
		w.idcol = binary.LittleEndian.AppendUint32(w.idcol, uint32(p>>32))
		w.idcol = binary.LittleEndian.AppendUint32(w.idcol, uint32(p))
	}
	w.stab = binary.LittleEndian.AppendUint16(w.stab, uint16(len(w.sk)))
	w.stab = append(w.stab, w.sk...)
	w.stab = binary.LittleEndian.AppendUint32(w.stab, w.first)
	w.stab = binary.LittleEndian.AppendUint32(w.stab, uint32(len(w.pairs)))
	w.stab = binary.LittleEndian.AppendUint32(w.stab, w.maxID)
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
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &levelWriter{f: f, w: bufio.NewWriterSize(f, 256<<10)}
	fill(w)
	w.endScope()
	if w.err == nil {
		tail := make([]byte, 0, 8*len(w.blocks))
		for _, b := range w.blocks {
			tail = binary.LittleEndian.AppendUint64(tail, b)
		}
		for _, sec := range [][]byte{tail, w.idcol, w.stab} {
			if _, err := w.w.Write(sec); err != nil && w.err == nil {
				w.err = err
			}
		}
		var foot []byte
		for _, v := range []int{w.size, len(w.blocks), w.n, len(w.stab)} {
			foot = binary.LittleEndian.AppendUint64(foot, uint64(v))
		}
		foot = append(foot, levelMagic...)
		if _, err := w.w.Write(foot); err != nil && w.err == nil {
			w.err = err
		}
	}
	if w.err == nil {
		w.err = w.w.Flush()
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
	return openLevel(path, minSeq, maxSeq)
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
	l := &level{m: m, dataLen: u(0), nb: u(1), n: u(2), minSeq: minSeq, maxSeq: maxSeq, path: path}
	stabLen := u(3)
	l.idOff = l.dataLen + 8*l.nb
	stabOff := l.idOff + 8*l.n
	bad := string(foot[32:]) != levelMagic || l.dataLen < 0 || l.n < 0 || l.nb != (l.n+blockEntries-1)/blockEntries ||
		stabLen < 0 || stabOff+stabLen+footerSize != size
	for p := stabOff; !bad && p < stabOff+stabLen; {
		kl := int(binary.LittleEndian.Uint16(m[p:]))
		if p+2+kl+12 > stabOff+stabLen {
			bad = true
			break
		}
		s := scope{key: string(m[p+2 : p+2+kl])}
		p += 2 + kl
		s.first, s.count, s.maxID = binary.LittleEndian.Uint32(m[p:]), binary.LittleEndian.Uint32(m[p+4:]), binary.LittleEndian.Uint32(m[p+8:])
		p += 12
		bad = int(s.first)+int(s.count) > l.n || (len(l.scopes) > 0 && l.scopes[len(l.scopes)-1].key >= s.key)
		l.scopes = append(l.scopes, s)
	}
	if bad {
		syscall.Munmap(m)
		return nil, fmt.Errorf("%w: %s", errCorrupt, path)
	}
	runtime.AddCleanup(l, func(m []byte) { syscall.Munmap(m) }, m)
	return l, nil
}
