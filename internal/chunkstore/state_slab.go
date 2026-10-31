package chunkstore

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
	"time"
	"unsafe"

	"github.com/golang/snappy"
)

// A stateShard keeps its cached states in memory the garbage collector does
// not scan: each state is one record, written once into a large byte chunk
// (see stateArena), and found through pointer-free slots and an open
// addressing index of slot numbers. A Go map of pointerful entries, each
// state's own maps, key strings and value slices, and a list element made a
// cached metric 15-20 heap objects; on a host caching 22.6M metrics, they
// doubled the GC's mark work per cycle.
//
// Records are immutable: a chunk's bytes are written once, by appending, and
// a chunk is never reused once released. A reader may therefore keep a record
// (see decodeStateRecord) after unlocking the shard; the slice keeps its chunk
// alive while compaction moves the live records elsewhere.

// stateSlot locates one name's record; it holds no pointers. A free slot has
// n == 0 and links the free list through next.
type stateSlot struct {
	hash       uint64
	epoch      uint64
	chunk, off uint32
	n          uint32
	// prev and next link the shard's LRU list, most recently used first.
	prev, next int32
}

// stateSlotOverhead is the memory of a slot and its share of the index
// (4 bytes per bucket at a load of 3/8 to 3/4) beyond its record.
const stateSlotOverhead = int(unsafe.Sizeof(stateSlot{})) + 8

// stateArena stores a shard's records in chunks. New records go to the head
// chunk; a record larger than a chunk gets a chunk of its own. A replaced or
// evicted record leaves dead bytes in its chunk until the chunk holds no live
// record, when it is released, or compaction moves its live records out.
type stateArena struct {
	// chunks holds each chunk's written bytes in its length; nil once released.
	chunks [][]byte
	live   []uint32
	// spare lists released chunk numbers for reuse (the number, not the memory).
	spare []uint32
	head  int32
	// written, live and capacity bytes of the unreleased chunks.
	written, liveBytes, capacity int64
}

// stateChunkSize sizes arena chunks for a shard budget: about 64 per shard,
// from 4 KiB up to 64 KiB.
func stateChunkSize(shardBudget int64) int {
	size := 4 << 10
	for size < 64<<10 && int64(size)*64 < shardBudget {
		size <<= 1
	}
	return size
}

func (c *stateCache) newChunk(a *stateArena, size int) uint32 {
	var i uint32
	if n := len(a.spare); n > 0 {
		i, a.spare = a.spare[n-1], a.spare[:n-1]
	} else {
		i = uint32(len(a.chunks))
		a.chunks = append(a.chunks, nil)
		a.live = append(a.live, 0)
	}
	a.chunks[i] = make([]byte, 0, size)
	a.capacity += int64(size)
	c.arenaBytes.Add(int64(size))
	return i
}

func (c *stateCache) releaseChunk(a *stateArena, i uint32) {
	size := int64(cap(a.chunks[i]))
	a.capacity -= size
	a.written -= int64(len(a.chunks[i]))
	a.chunks[i], a.live[i] = nil, 0
	a.spare = append(a.spare, i)
	c.arenaBytes.Add(-size)
	if a.head == int32(i) {
		a.head = -1
	}
}

// reserve appends n bytes for a record and returns its chunk, offset and
// bytes to fill.
func (c *stateCache) reserve(a *stateArena, n int) (uint32, uint32, []byte) {
	var i uint32
	if n > c.chunk {
		i = c.newChunk(a, n)
	} else {
		if a.head < 0 || cap(a.chunks[a.head])-len(a.chunks[a.head]) < n {
			if a.head >= 0 && a.live[a.head] == 0 {
				c.releaseChunk(a, uint32(a.head))
			}
			a.head = int32(c.newChunk(a, c.chunk))
		}
		i = uint32(a.head)
	}
	chunk := a.chunks[i]
	off := len(chunk)
	a.chunks[i] = chunk[:off+n]
	a.live[i] += uint32(n)
	a.written += int64(n)
	a.liveBytes += int64(n)
	return i, uint32(off), chunk[off : off : off+n]
}

// free marks a record's bytes dead, releasing its chunk once nothing in it
// is live. The head chunk stays until it fills.
func (c *stateCache) free(a *stateArena, chunk, n uint32) {
	a.live[chunk] -= n
	a.liveBytes -= int64(n)
	if a.live[chunk] == 0 && int32(chunk) != a.head {
		c.releaseChunk(a, chunk)
	}
}

// compact moves live records out of the emptiest chunks once dead bytes
// exceed an eighth of the live ones (and two chunks), down to a sixteenth.
// Updates replace their records continuously; most chunks then die whole and
// are released without copying, and compaction bounds the rest.
func (c *stateCache) compact(sh *stateShard) {
	a := &sh.arena
	dead := a.written - a.liveBytes
	if dead <= a.liveBytes/8 || dead <= 2*int64(c.chunk) {
		return
	}
	goal := dead - a.liveBytes/16
	candidates := make([]uint32, 0, len(a.chunks))
	for i, chunk := range a.chunks {
		if chunk != nil && int32(i) != a.head && int(a.live[i]) < len(chunk) {
			candidates = append(candidates, uint32(i))
		}
	}
	// Least utilized first: the most dead bytes reclaimed per byte copied.
	slices.SortFunc(candidates, func(x, y uint32) int {
		ux := uint64(a.live[x]) * uint64(len(a.chunks[y]))
		uy := uint64(a.live[y]) * uint64(len(a.chunks[x]))
		switch {
		case ux < uy:
			return -1
		case ux > uy:
			return 1
		}
		return 0
	})
	victim := make([]bool, len(a.chunks))
	var reclaim int64
	for _, i := range candidates {
		if reclaim >= goal {
			break
		}
		victim[i] = true
		reclaim += int64(len(a.chunks[i])) - int64(a.live[i])
	}
	for s := range sh.slots {
		slot := &sh.slots[s]
		if slot.n == 0 || int(slot.chunk) >= len(victim) || !victim[slot.chunk] {
			continue
		}
		src := a.chunks[slot.chunk][slot.off : slot.off+slot.n]
		chunk, off, dst := c.reserve(a, int(slot.n))
		copy(dst[:slot.n], src)
		c.free(a, slot.chunk, slot.n)
		slot.chunk, slot.off = chunk, off
		c.compactedBytes.Add(uint64(slot.n))
	}
}

// record returns slot i's record, capacity-limited.
func (sh *stateShard) record(i int32) []byte {
	s := &sh.slots[i]
	return sh.arena.chunks[s.chunk][s.off : s.off+s.n : s.off+s.n]
}

// recordKey returns the name a record is cached under.
func recordKey(rec []byte) []byte {
	n, w := binary.Uvarint(rec)
	return rec[w : w+int(n)]
}

// find returns name's index position and slot, or -1.
func (sh *stateShard) find(hash uint64, name string) (pos int, slot int32) {
	if len(sh.table) == 0 {
		return -1, -1
	}
	mask := len(sh.table) - 1
	for i := int(hash) & mask; ; i = (i + 1) & mask {
		s := sh.table[i] - 1
		if s < 0 {
			return -1, -1
		}
		if sh.slots[s].hash == hash && string(recordKey(sh.record(s))) == name {
			return i, s
		}
	}
}

// position returns the index position of slot s.
func (sh *stateShard) position(s int32) int {
	mask := len(sh.table) - 1
	for i := int(sh.slots[s].hash) & mask; ; i = (i + 1) & mask {
		if sh.table[i] == s+1 {
			return i
		}
	}
}

// index adds slot s, growing the index to keep its load at most 3/4.
func (c *stateCache) index(sh *stateShard, s int32) {
	if (sh.used+1)*4 > len(sh.table)*3 {
		old := sh.table
		sh.table = make([]int32, max(16, 2*len(old)))
		c.arenaBytes.Add(4 * int64(len(sh.table)-len(old)))
		for _, t := range old {
			if t != 0 {
				sh.place(t - 1)
			}
		}
	}
	sh.place(s)
	sh.used++
}

func (sh *stateShard) place(s int32) {
	mask := len(sh.table) - 1
	i := int(sh.slots[s].hash) & mask
	for sh.table[i] != 0 {
		i = (i + 1) & mask
	}
	sh.table[i] = s + 1
}

// unindex removes the slot at position pos, shifting back the entries
// probed past it so that every lookup still reaches its slot.
func (sh *stateShard) unindex(pos int) {
	mask := len(sh.table) - 1
	i := pos
	sh.table[i] = 0
	for j := (i + 1) & mask; sh.table[j] != 0; j = (j + 1) & mask {
		home := int(sh.slots[sh.table[j]-1].hash) & mask
		// The entry at j stays when its home lies cyclically in (i, j].
		if i <= j && i < home && home <= j || i > j && (i < home || home <= j) {
			continue
		}
		sh.table[i], sh.table[j] = sh.table[j], 0
		i = j
	}
	sh.used--
}

func (c *stateCache) newSlot(sh *stateShard) int32 {
	if s := sh.freeSlot; s >= 0 {
		sh.freeSlot = sh.slots[s].next
		return s
	}
	before := cap(sh.slots)
	sh.slots = append(sh.slots, stateSlot{})
	c.arenaBytes.Add(int64(cap(sh.slots)-before) * int64(unsafe.Sizeof(stateSlot{})))
	return int32(len(sh.slots) - 1)
}

func (sh *stateShard) pushFront(s int32) {
	sh.slots[s].prev, sh.slots[s].next = -1, sh.lruHead
	if sh.lruHead >= 0 {
		sh.slots[sh.lruHead].prev = s
	} else {
		sh.lruTail = s
	}
	sh.lruHead = s
}

func (sh *stateShard) unlink(s int32) {
	prev, next := sh.slots[s].prev, sh.slots[s].next
	if prev >= 0 {
		sh.slots[prev].next = next
	} else {
		sh.lruHead = next
	}
	if next >= 0 {
		sh.slots[next].prev = prev
	} else {
		sh.lruTail = prev
	}
}

func (sh *stateShard) touch(s int32) {
	if sh.lruHead != s {
		sh.unlink(s)
		sh.pushFront(s)
	}
}

// put records st as name's state, installed at epoch, replacing any state
// the name has.
func (c *stateCache) put(sh *stateShard, hash uint64, name string, st *metricState, epoch uint64) {
	if st.keys != nil {
		panic("chunkstore: caching a state that is not sealed")
	}
	n := stateRecordLen(name, st)
	_, s := sh.find(hash, name)
	if s >= 0 {
		old := &sh.slots[s]
		c.free(&sh.arena, old.chunk, old.n)
		c.account(sh, -(int64(old.n) + int64(stateSlotOverhead)))
		sh.touch(s)
	} else {
		s = c.newSlot(sh)
		sh.slots[s] = stateSlot{hash: hash}
		c.index(sh, s)
		sh.pushFront(s)
	}
	chunk, off, dst := c.reserve(&sh.arena, n)
	if rec := appendStateRecord(dst, name, st); len(rec) != n {
		panic("chunkstore: state record length mismatch")
	}
	slot := &sh.slots[s]
	slot.chunk, slot.off, slot.n, slot.epoch = chunk, off, uint32(n), epoch
	c.account(sh, int64(n)+int64(stateSlotOverhead))
	sh.last, sh.lastSlot = st, s
}

// remove forgets slot s at index position pos.
func (c *stateCache) remove(sh *stateShard, pos int, s int32) {
	sh.unindex(pos)
	sh.unlink(s)
	slot := &sh.slots[s]
	c.free(&sh.arena, slot.chunk, slot.n)
	c.account(sh, -(int64(slot.n) + int64(stateSlotOverhead)))
	*slot = stateSlot{next: sh.freeSlot, prev: -1}
	sh.freeSlot = s
	if sh.lastSlot == s {
		sh.last, sh.lastSlot = nil, -1
	}
}

func (c *stateCache) account(sh *stateShard, delta int64) {
	sh.bytes += delta
	c.bytes.Add(delta)
}

// stateAt returns slot s's state: the one put there when it is still at
// hand, else its record for decodeStateRecord.
func (sh *stateShard) stateAt(s int32) (*metricState, []byte) {
	if s == sh.lastSlot && sh.last != nil {
		return sh.last, nil
	}
	return nil, sh.record(s)
}

// A state record holds the name it is cached under, the metadata and the
// state keys (see encodeStateKeys):
//
//	uvarint name length, name
//	ID, Generation, Revision, LastUpdate (Unix nanoseconds, 0 when zero): 8 bytes each
//	AggregationMethod, XFilesFactor bits: 4 bytes each
//	FileMigration, flags (stateRecordContinuation, stateRecordOwnName): 1 byte each
//	with stateRecordOwnName: uvarint length and Metadata.Name, when it is not the cached name
//	uvarint retention count, then each Step and Count as varints
//	the state keys, optionally Snappy-packed (stateRecordPackedKeys)
//
// Integers are little endian. LastUpdate is kept as the store decodes it
// (decodeRevision): nanoseconds, in the local zone, no monotonic reading.
const (
	stateRecordFixed        = 42
	stateRecordContinuation = 1
	stateRecordOwnName      = 2
	stateRecordPackedKeys   = 4
)

func uvarintLen(x uint64) int { return (bits.Len64(x|1) + 6) / 7 }

func varintLen(x int64) int {
	u := uint64(x) << 1
	if x < 0 {
		u = ^u
	}
	return uvarintLen(u)
}

// stateRecordLen is the length of name's record of st.
func stateRecordLen(name string, st *metricState) int {
	m := &st.meta
	n := uvarintLen(uint64(len(name))) + len(name) + stateRecordFixed + uvarintLen(uint64(len(m.Retentions))) + len(st.recordKeys())
	if m.Name != name {
		n += uvarintLen(uint64(len(m.Name))) + len(m.Name)
	}
	for _, r := range m.Retentions {
		n += varintLen(int64(r.Step)) + varintLen(int64(r.Count))
	}
	return n
}

// appendStateRecord appends name's record of st, which must be sealed.
func appendStateRecord(dst []byte, name string, st *metricState) []byte {
	m := &st.meta
	dst = binary.AppendUvarint(dst, uint64(len(name)))
	dst = append(dst, name...)
	var nanos int64
	if !m.LastUpdate.IsZero() {
		nanos = m.LastUpdate.UnixNano()
	}
	dst = binary.LittleEndian.AppendUint64(dst, m.ID)
	dst = binary.LittleEndian.AppendUint64(dst, m.Generation)
	dst = binary.LittleEndian.AppendUint64(dst, m.Revision)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(nanos))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(m.AggregationMethod))
	dst = binary.LittleEndian.AppendUint32(dst, math.Float32bits(m.XFilesFactor))
	var flags byte
	if m.Continuation {
		flags |= stateRecordContinuation
	}
	if m.Name != name {
		flags |= stateRecordOwnName
	}
	if st.packed != nil {
		flags |= stateRecordPackedKeys
	}
	dst = append(dst, byte(m.FileMigration), flags)
	if m.Name != name {
		dst = binary.AppendUvarint(dst, uint64(len(m.Name)))
		dst = append(dst, m.Name...)
	}
	dst = binary.AppendUvarint(dst, uint64(len(m.Retentions)))
	for _, r := range m.Retentions {
		dst = binary.AppendVarint(dst, int64(r.Step))
		dst = binary.AppendVarint(dst, int64(r.Count))
	}
	return append(dst, st.recordKeys()...)
}

func (st *metricState) recordKeys() []byte {
	if st.packed != nil {
		return st.packed
	}
	return st.kenc
}

// Most states fit the stack buffer. Keep only the packed bytes, rather than
// allocating Snappy's maximum output size on every mutation (even a refusal).
func packStateKeys(keys []byte) []byte {
	var scratch [4096]byte
	if snappy.MaxEncodedLen(len(keys)) <= len(scratch) {
		packed := snappy.Encode(scratch[:], keys)
		if len(packed) < len(keys)-len(keys)/8 {
			return bytes.Clone(packed)
		}
		return nil
	}
	packed := snappy.Encode(nil, keys)
	if len(packed) < len(keys)-len(keys)/8 {
		return packed
	}
	return nil
}

// decodeStateRecord returns the sealed state a record holds. Its state keys
// stay in rec, which must never change, or in their own unpacked allocation;
// its metadata is the caller's. Metadata checkpoints version this encoding
// with checkpointMagic; change that version when the record layout changes.
func decodeStateRecord(rec []byte) *metricState {
	key := recordKey(rec)
	p := uvarintLen(uint64(len(key))) + len(key)
	st := &metricState{size: int64(len(rec) + stateSlotOverhead)}
	m := &st.meta
	le := binary.LittleEndian
	m.ID = le.Uint64(rec[p:])
	m.Generation = le.Uint64(rec[p+8:])
	m.Revision = le.Uint64(rec[p+16:])
	if nanos := int64(le.Uint64(rec[p+24:])); nanos != 0 {
		m.LastUpdate = time.Unix(0, nanos)
	}
	m.AggregationMethod = AggregationMethod(le.Uint32(rec[p+32:]))
	m.XFilesFactor = math.Float32frombits(le.Uint32(rec[p+36:]))
	m.FileMigration = FileMigrationState(rec[p+40])
	flags := rec[p+41]
	m.Continuation = flags&stateRecordContinuation != 0
	p += stateRecordFixed
	if flags&stateRecordOwnName != 0 {
		n, w := binary.Uvarint(rec[p:])
		m.Name = string(rec[p+w : p+w+int(n)])
		p += w + int(n)
	} else {
		m.Name = string(key)
	}
	count, w := binary.Uvarint(rec[p:])
	p += w
	if count > 0 {
		m.Retentions = make([]Retention, count)
		for i := range m.Retentions {
			step, w := binary.Varint(rec[p:])
			p += w
			points, w := binary.Varint(rec[p:])
			p += w
			m.Retentions[i] = Retention{Step: int(step), Count: int(points)}
		}
	}
	st.kenc = rec[p:len(rec):len(rec)]
	if flags&stateRecordPackedKeys != 0 {
		st.packed = st.kenc
		var err error
		st.kenc, err = snappy.Decode(nil, st.packed)
		if err != nil {
			panic("chunkstore: invalid packed state record")
		}
	}
	return st
}

// A sealed state's keys (metricState.kenc):
//
//	[0:4]      key count n
//	[4:8]      offset d of the key entries
//	[8:8+4n]   each key entry's offset from d, in key order
//	[8+4n:d]   complete prefixes: uvarint length, bytes
//	[d:]       key entries: uvarint key length, key, uvarint value tag (0 when
//	           the key is known absent, else 1 + the value length), value
//
// Integers are little endian. Keys sort bytewise, so a lookup is a binary
// search and a walk of a prefix one scan.
var emptyStateKeys = []byte{0, 0, 0, 0, 8, 0, 0, 0}

func encodeStateKeys(keys map[string][]byte, complete map[string]bool) []byte {
	names := make([]string, 0, len(keys)+len(complete))
	for k := range keys {
		names = append(names, k)
	}
	slices.Sort(names)
	prefixes := names[len(names):]
	for p := range complete {
		prefixes = append(prefixes, p)
	}
	slices.Sort(prefixes)
	data := 8 + 4*len(names)
	for _, p := range prefixes {
		data += uvarintLen(uint64(len(p))) + len(p)
	}
	size := data
	for _, k := range names {
		v := keys[k]
		size += uvarintLen(uint64(len(k))) + len(k) + len(v) + uvarintLen(stateValueTag(v))
	}
	b := make([]byte, data, size)
	le := binary.LittleEndian
	le.PutUint32(b[0:], uint32(len(names)))
	le.PutUint32(b[4:], uint32(data))
	p := b[:8+4*len(names)]
	for _, prefix := range prefixes {
		p = binary.AppendUvarint(p, uint64(len(prefix)))
		p = append(p, prefix...)
	}
	for i, k := range names {
		le.PutUint32(b[8+4*i:], uint32(len(b)-data))
		v := keys[k]
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
		b = binary.AppendUvarint(b, stateValueTag(v))
		b = append(b, v...)
	}
	return b
}

func stateValueTag(v []byte) uint64 {
	if v == nil {
		return 0
	}
	return uint64(len(v)) + 1
}

func stateKeyCount(kenc []byte) int { return int(binary.LittleEndian.Uint32(kenc)) }

// stateKeyAt returns key i and the offset of its value tag.
func stateKeyAt(kenc []byte, i int) ([]byte, int) {
	p := int(binary.LittleEndian.Uint32(kenc[4:])) + int(binary.LittleEndian.Uint32(kenc[8+4*i:]))
	n, w := binary.Uvarint(kenc[p:])
	p += w
	return kenc[p : p+int(n)], p + int(n)
}

// stateValueAt returns the value at a tag offset; nil records the key absent.
func stateValueAt(kenc []byte, p int) []byte {
	tag, w := binary.Uvarint(kenc[p:])
	if tag == 0 {
		return nil
	}
	p += w
	end := p + int(tag) - 1
	return kenc[p:end:end]
}

// stateSearch returns the first key index not below key.
func stateSearch(kenc []byte, key []byte) int {
	lo, hi := 0, stateKeyCount(kenc)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if k, _ := stateKeyAt(kenc, mid); bytes.Compare(k, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// statePrefixes returns the offsets of the complete prefixes; nextStatePrefix
// reads the one at p and returns the offset of the next.
func statePrefixes(kenc []byte) (p, end int) {
	return 8 + 4*stateKeyCount(kenc), int(binary.LittleEndian.Uint32(kenc[4:]))
}

func nextStatePrefix(kenc []byte, p int) ([]byte, int) {
	n, w := binary.Uvarint(kenc[p:])
	p += w
	return kenc[p : p+int(n)], p + int(n)
}

// unsafeString views immutable bytes as a string.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
