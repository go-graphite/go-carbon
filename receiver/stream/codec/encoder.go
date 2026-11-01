package codec

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/klauspost/compress/zstd"
)

type point struct {
	ts    int64
	bits  uint64
	valid bool
}
type row struct {
	id   uint32
	ts   int64
	bits uint64
}

// Encoder keeps a connection dictionary and reusable column buffers. Flush
// before sending data, and send AppendPreamble once when opening the connection.
// Add may queue complete frames internally; callers should Flush when Ready.
type Encoder struct {
	unshared         uint64
	opts             Options
	table            *NameTable
	ownsTable        bool
	index            wireIndex
	states           wireStates
	definitions      []nameRef
	arena            nameArena
	decodedEntries   int
	decodedBlocks    int
	decodedUsed      int
	decodedMaxBlocks int
	defined          int
	rows, tmp        []row
	cols             [3][]byte
	raw, pending     []byte
	estimate         int
	z                *zstd.Encoder
}

func NewEncoder(o Options) (*Encoder, error) {
	o, err := options(o)
	if err != nil {
		return nil, err
	}
	nameBudget := o.DictionaryBytes - o.DictionaryBytes/4
	if o.DictionaryBytes < 16<<10 {
		nameBudget = o.DictionaryBytes / 2
	}
	table, err := NewNameTable(nameBudget, true)
	if err != nil {
		return nil, err
	}
	e, err := NewSharedEncoder(o, table)
	if e != nil {
		e.ownsTable = true
		e.opts.DictionaryBytes = o.DictionaryBytes - nameBudget
	}
	return e, err
}

// NewSharedEncoder retains only global-to-wire ids and predictors. Name
// definitions are copied into frame scratch, then released on Flush.
func NewSharedEncoder(o Options, table *NameTable) (*Encoder, error) {
	o, err := options(o)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, ErrLimit
	}
	e := &Encoder{opts: o, table: table, arena: nameArena{quantum: min(64<<10, o.MaxFrameSize)}}
	if o.Flags&FlagZstd != 0 {
		e.z, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	}
	return e, err
}

// Close releases compression resources; it does not flush buffered points.
func (e *Encoder) Close() {
	for i := 0; i < e.states.n; i++ {
		e.table.ReleaseKey(e.states.at(uint32(i)).global())
	}
	e.states = wireStates{}
	e.index = nil
	if e.z != nil {
		_ = e.z.Close()
	}
}

// Add copies name only on its first occurrence. Known names allocate nothing in
// steady state. Reaching MaxNames automatically flushes and resets the dictionary
// before assigning a new id. Empty and oversized names are rejected.
func (e *Encoder) Add(name []byte, value float64, ts int64) error {
	if len(name) == 0 || len(name) > e.opts.MaxNameSize {
		return ErrLimit
	}
	_, _, err := e.AddHashed(name, nameHash(name), value, ts)
	return err
}

// AddHashed reuses a stable routing hash. Identity follows the NameTable
// policy: exact bytes or a pair of independent fingerprints. The connection
// byte limit includes allocated id-map and predictor capacity.
func (e *Encoder) AddHashed(name []byte, hash uint64, value float64, ts int64) (defined, reset bool, err error) {
	if len(name) == 0 || len(name) > e.opts.MaxNameSize {
		return false, false, ErrLimit
	}
	token := e.table.Resolve(name, hash)
	return e.AddGlobal(name, hash, token, value, ts)
}

// AddGlobal accepts a cached global token. The generation check rejects stale
// tokens after queueing, expiry, reloads or reuse. Known IDs need no shared lock.
func (e *Encoder) AddGlobal(name []byte, hash, token uint64, value float64, ts int64) (defined, reset bool, err error) {
	if len(name) == 0 || len(name) > e.opts.MaxNameSize {
		return false, false, ErrLimit
	}
	key := uint32(token) & 0xffffff
	id, ok := e.index.find(key, &e.states)
	ok = ok && e.states.at(id).nameToken() == token
	if !ok && !e.table.Valid(token, hash) {
		token = e.table.Resolve(name, hash)
		key = uint32(token) & 0xffffff
		id, ok = e.index.find(key, &e.states)
		ok = ok && e.states.at(id).nameToken() == token
	}
	cost := 24
	if !ok {
		cost += len(name) + 10
	}
	if cost+32 > e.opts.MaxFrameSize {
		return false, false, ErrLimit
	}
	if len(e.rows) > 0 && (len(e.rows) >= e.opts.MaxRows || e.estimate+cost+32 > e.opts.MaxFrameSize) {
		if err := e.flushPending(); err != nil {
			return false, false, err
		}
	}
	if !ok && (e.states.n >= e.opts.MaxNames || e.states.n >= wireIDMask || e.projectedBytes() > e.opts.DictionaryBytes || e.projectedDecoded(len(name)) > DefaultDictionaryBytes) {
		if err := e.Reset(); err != nil {
			return false, false, err
		}
		reset = true
	}
	if !ok {
		if !e.table.Acquire(token, hash) {
			token = e.table.Resolve(name, hash)
			if !e.table.Acquire(token, hash) {
				token = 0
			}
		}
		e.index.grow(e.states.n+1, &e.states)
		key = uint32(token) & 0xffffff
		id = e.states.add(token)
		if token != 0 {
			e.index.insert(key, id)
		} else {
			e.unshared++
		}
		e.definitions = append(e.definitions, e.arena.add(name))
		e.decodedEntries = nextCapacity(e.states.n, e.decodedEntries)
		if e.decodedBlocks == 0 || e.decodedUsed+len(name) > 64<<10 {
			e.decodedBlocks++
			e.decodedUsed = 0
		}
		e.decodedUsed += len(name)
		e.decodedMaxBlocks = max(e.decodedMaxBlocks, e.decodedBlocks)
	}
	e.rows = append(e.rows, row{id: id, ts: ts, bits: math.Float64bits(value)})
	e.estimate += cost
	return !ok, reset, nil
}
func (e *Encoder) projectedBytes() int {
	return e.states.projectedBytes() + indexCapacity(e.states.n+1, len(e.index))*4
}
func (e *Encoder) RetainedBytes() int {
	n := e.states.bytes() + len(e.index)*4
	if e.ownsTable {
		n += e.table.Stats().Bytes
	}
	return n
}
func (e *Encoder) NameCount() int { return e.states.n }

// Ready reports that the target rows/bytes were reached or a frame was queued.
// A timer may also call Flush to bound latency on quiet connections.
func (e *Encoder) Ready() bool {
	return len(e.pending) > 0 || len(e.rows) >= e.opts.MaxRows || e.estimate >= e.opts.FrameSize
}

// Flush appends all pending frames to caller-owned dst. It emits nothing for an
// empty batch, preserves dictionary state, and reuses encoder scratch memory.
func (e *Encoder) Flush(dst []byte) ([]byte, error) {
	dst = append(dst, e.pending...)
	e.pending = e.pending[:0]
	return e.flush(dst)
}
func (e *Encoder) flushPending() error {
	var err error
	e.pending, err = e.flush(e.pending)
	return err
}

// Reset queues buffered data followed by a reset frame, clearing every id.
func (e *Encoder) Reset() error {
	if err := e.flushPending(); err != nil {
		return err
	}
	var err error
	e.pending, err = appendFrame(e.pending, nil, ResetFrame, 0, nil, e.opts.MaxFrameSize)
	if err != nil {
		return err
	}
	for i := 0; i < e.states.n; i++ {
		e.table.ReleaseKey(e.states.at(uint32(i)).global())
	}
	clear(e.index)
	e.states.n = 0
	e.definitions = e.definitions[:0]
	e.arena.reset()

	e.defined = 0
	e.decodedBlocks = 0
	e.decodedUsed = 0
	return nil
}

func (e *Encoder) flush(dst []byte) ([]byte, error) {
	if len(e.rows) == 0 {
		return dst, nil
	}
	e.rows, e.tmp = sortRows(e.rows, e.tmp, uint32(e.states.n-1))
	raw := e.raw[:0]
	raw = binary.AppendUvarint(raw, uint64(e.defined))
	raw = binary.AppendUvarint(raw, uint64(e.states.n-e.defined))
	for _, name := range e.definitions {
		raw = binary.AppendUvarint(raw, uint64(name.size))
		raw = append(raw, e.arena.name(name)...)
	}
	ids, stamps, values := e.cols[0][:0], e.cols[1][:0], e.cols[2][:0]
	prev := row{}
	for _, r := range e.rows {
		ids = binary.AppendUvarint(ids, uint64(r.id-prev.id))
		want := *e.states.at(r.id)
		if !want.valid() {
			want.ts = prev.ts
		}
		stamps = binary.AppendVarint(stamps, r.ts-want.ts)
		values = appendXor(values, r.bits^want.bits)
		state := e.states.at(r.id)
		state.ts = r.ts
		state.bits = r.bits
		state.token |= predictorValid
		prev = r
	}
	raw = binary.AppendUvarint(raw, uint64(len(ids)))
	raw = binary.AppendUvarint(raw, uint64(len(stamps)))
	raw = append(raw, ids...)
	raw = append(raw, stamps...)
	raw = append(raw, values...)
	e.raw = raw
	e.cols[0], e.cols[1], e.cols[2] = ids, stamps, values
	var err error
	dst, err = appendFrame(dst, raw, DataFrame, len(e.rows), e.z, e.opts.MaxFrameSize)
	if err != nil {
		return dst, err
	}
	e.defined = e.states.n
	e.definitions = e.definitions[:0]
	e.arena.reset()
	e.rows = e.rows[:0]
	e.estimate = 0
	return dst, nil
}

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

// Stable radix ordering keeps repeated ids in their original arrival order.
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
			tmp = make([]row, len(rows))
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

func (e *Encoder) MapBytes() int { return len(e.index) * 4 }

func (e *Encoder) projectedDecoded(n int) int {
	blocks := e.decodedBlocks
	if blocks == 0 || e.decodedUsed+n > 64<<10 {
		blocks++
	}
	return nextCapacity(e.states.n+1, e.decodedEntries)*104 + max(blocks, e.decodedMaxBlocks)*((64<<10)+48)
}

func (e *Encoder) UnsharedTotal() uint64 { return e.unshared }
