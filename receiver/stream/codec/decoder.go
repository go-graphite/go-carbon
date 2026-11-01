package codec

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
)

type entry struct {
	last point
	slot any
}

// Visit receives one validated row. name belongs to the decoder, remains valid
// until reset, and must not be modified. slot is private to this id and caller;
// it survives frames but is cleared on reset and omitted from snapshots. Do not
// retain the slot pointer across calls (the dictionary may grow).
type Visit func(id uint32, name []byte, value float64, timestamp int64, slot *any)

// Decoder owns one connection's dictionary. After a protocol error, discard it.
type Decoder struct {
	opts        Options
	entries     []entry
	names       []nameRef
	definitions [][]byte
	resolve     func([]byte) any
	nameBytes   int
	arena       nameArena
	rows        []row
	z           *zstd.Decoder
	raw, wire   []byte
	readN       int
	broken      bool
}

func NewDecoder(o Options) (*Decoder, error) {
	o, err := options(o)
	if err != nil {
		return nil, err
	}
	d := &Decoder{opts: o, arena: nameArena{quantum: min(64<<10, o.DictionaryBytes/8)}}
	if o.Flags&FlagZstd != 0 {
		d.z, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(o.MaxFrameSize)), zstd.WithDecoderMaxWindow(uint64(max(1<<10, o.MaxFrameSize))), zstd.WithDecodeAllCapLimit(true))
	}
	return d, err
}

func (d *Decoder) Close() {
	if d.z != nil {
		d.z.Close()
	}
}

// Pending returns bytes consumed from an incomplete frame by ReadFrame. Append
// them after Snapshot and before any reader-buffered bytes during handoff.
func (d *Decoder) Pending() []byte { return d.wire[:d.readN] }

// ReadFrame reads and decodes one frame. Transport errors preserve Pending and
// can be resumed; protocol errors are terminal. io.EOF means a frame boundary,
// io.ErrUnexpectedEOF means a truncated frame. It never reads into the next frame.
func (d *Decoder) ReadFrame(r io.Reader, visit Visit) (FrameInfo, error) {
	if d.broken {
		return FrameInfo{}, ErrBroken
	}
	if len(d.wire) < HeaderSize {
		d.wire = append(d.wire[:0], make([]byte, HeaderSize)...)
	}
	if d.readN < HeaderSize {
		if err := readMore(r, d.wire[:HeaderSize], &d.readN); err != nil {
			return FrameInfo{}, err
		}
	}
	info, err := d.header(d.wire[:HeaderSize])
	if err != nil {
		d.broken = true
		return info, err
	}
	size := HeaderSize + int(info.PayloadBytes)
	if cap(d.wire) < size {
		b := make([]byte, size)
		copy(b, d.wire[:d.readN])
		d.wire = b
	} else {
		d.wire = d.wire[:size]
	}
	if d.readN < size {
		if err := readMore(r, d.wire, &d.readN); err != nil {
			if err == io.EOF { //nolint:errorlint // Only ReadFull's direct EOF needs upgrading after a consumed header.
				err = io.ErrUnexpectedEOF
			} // A header was already consumed.
			return info, err
		}
	}
	info, err = d.Decode(d.wire, visit)
	if err == nil {
		d.readN = 0
		d.wire = d.wire[:HeaderSize]
	}
	return info, err
}

func (d *Decoder) header(h []byte) (FrameInfo, error) {
	if len(h) < HeaderSize {
		return FrameInfo{}, ErrFrame
	}
	info := FrameInfo{Kind: h[0], PayloadBytes: binary.BigEndian.Uint32(h[4:8]), RawBytes: binary.BigEndian.Uint32(h[8:12]), Rows: binary.BigEndian.Uint32(h[12:16])}
	if h[1]|h[2]|h[3] != 0 || info.Kind > StateFrame {
		return info, ErrFrame
	}
	if uint64(info.PayloadBytes) > uint64(d.opts.MaxFrameSize) || uint64(info.RawBytes) > uint64(d.opts.MaxFrameSize) || uint64(info.Rows) > uint64(d.opts.MaxRows) {
		return info, ErrLimit
	}
	if info.Kind == ResetFrame {
		if info.PayloadBytes|info.RawBytes|info.Rows != 0 {
			return info, ErrFrame
		}
	} else if info.PayloadBytes == 0 || info.RawBytes == 0 || (info.Kind == StateFrame && info.Rows != 0) || (info.Kind == DataFrame && info.Rows == 0) {
		return info, ErrFrame
	}
	if d.z == nil && info.PayloadBytes != info.RawBytes {
		return info, ErrFrame
	}
	return info, nil
}

// Decode accepts exactly one complete frame (header included). All columns are
// validated before visiting rows. A rejected frame never calls visit.
func (d *Decoder) Decode(frame []byte, visit Visit) (info FrameInfo, err error) {
	if d.broken {
		return info, ErrBroken
	}
	defer func() {
		if err != nil {
			d.broken = true
		}
	}()
	info, err = d.header(frame)
	if err != nil {
		return info, err
	}
	if uint64(len(frame)) != uint64(HeaderSize)+uint64(info.PayloadBytes) {
		return info, ErrFrame
	}
	raw := frame[HeaderSize:]
	if crc32.Checksum(raw, castagnoli) != binary.BigEndian.Uint32(frame[16:20]) {
		return info, ErrFrame
	}
	if info.Kind == ResetFrame {
		clear(d.entries)
		d.entries = d.entries[:0]
		d.arena.reset()
		d.names = d.names[:0]
		d.nameBytes = 0
		return info, nil
	}
	if d.z != nil {
		if cap(d.raw) < int(info.RawBytes) {
			d.raw = make([]byte, 0, int(info.RawBytes))
		}
		raw, err = d.z.DecodeAll(raw, d.raw[:0:int(info.RawBytes)])
		if err != nil {
			return info, ErrFrame
		}
		d.raw = raw
	}
	if len(raw) != int(info.RawBytes) {
		return info, ErrFrame
	}
	r := reader{data: raw}
	first, count := r.uint(), r.uint()
	if r.err != nil || first != uint64(len(d.entries)) {
		return info, ErrFrame
	}
	if count > uint64(d.opts.MaxNames)-uint64(len(d.entries)) || count > uint64(len(r.data)) {
		return info, ErrLimit
	}
	info.Names = uint32(count)
	d.definitions = d.definitions[:0]
	for range int(count) {
		n := r.uint()
		if n == 0 || n > uint64(d.opts.MaxNameSize) {
			return info, ErrLimit
		}
		name := r.bytes(n)
		if r.err != nil {
			return info, ErrFrame
		}
		size := nextCapacity(len(d.entries)+1, cap(d.entries))
		charge := d.arena.bytes + size*84 + d.arena.extra(len(name))
		if d.resolve != nil {
			charge = size*40 + d.nameBytes + len(name) + (len(d.entries)+1)*64
		}
		if charge > d.opts.DictionaryBytes {
			return info, ErrLimit
		}
		if size > cap(d.entries) {
			entries := make([]entry, len(d.entries), size)
			copy(entries, d.entries)
			d.entries = entries
		}
		e := entry{}
		d.nameBytes += len(name)
		d.definitions = append(d.definitions, name)
		if d.resolve == nil {
			if size > cap(d.names) {
				refs := make([]nameRef, len(d.names), size)
				copy(refs, d.names)
				d.names = refs
			}
			d.names = append(d.names, d.arena.add(name))
		}

		if info.Kind == StateFrame {
			state := r.bytes(1)
			if len(state) == 0 || state[0] > 1 {
				return info, ErrFrame
			}
			if state[0] == 1 {
				b := r.bytes(16)
				if r.err != nil {
					return info, ErrFrame
				}
				e.last = point{ts: int64(binary.BigEndian.Uint64(b[:8])), bits: binary.BigEndian.Uint64(b[8:]), valid: true}
			}
		}
		d.entries = append(d.entries, e)
	}
	if info.Kind == StateFrame {
		if r.err != nil || len(r.data) != 0 || count == 0 {
			return info, ErrFrame
		}
		d.bind(int(first))
		return info, nil
	}
	idsLen, stampsLen := r.uint(), r.uint()
	ids := reader{data: r.bytes(idsLen)}
	stamps := reader{data: r.bytes(stampsLen)}
	values := r
	if r.err != nil || uint64(info.Rows) > uint64(len(ids.data)) || uint64(info.Rows) > uint64(len(stamps.data)) || uint64(info.Rows) > uint64(len(values.data)) {
		return info, ErrFrame
	}
	d.rows = d.rows[:0]
	var id uint64
	var prevTS int64
	for range int(info.Rows) {
		delta := ids.uint()
		if delta > uint64(len(d.entries)) || id+delta >= uint64(len(d.entries)) {
			return info, ErrFrame
		}
		id += delta
		want := d.entries[id].last
		if !want.valid {
			want.ts = prevTS
		}
		p := point{ts: want.ts + stamps.sint(), bits: want.bits ^ values.xor(), valid: true}
		d.entries[id].last = p
		d.rows = append(d.rows, row{id: uint32(id), ts: p.ts, bits: p.bits})
		prevTS = p.ts
	}
	if ids.err != nil || stamps.err != nil || values.err != nil || len(ids.data)+len(stamps.data)+len(values.data) != 0 {
		return info, ErrFrame
	}
	d.bind(int(first))
	if visit != nil {
		for _, p := range d.rows {
			e := &d.entries[p.id]
			visit(p.id, d.name(int(p.id)), math.Float64frombits(p.bits), p.ts, &e.slot)
		}
	}
	return info, nil
}

// Snapshot appends a reset followed by bounded state frames. It restores all
// predictors on a fresh decoder without replaying points or caller slots. Use
// the same options/flags for the successor; append Pending after this snapshot.
func (d *Decoder) Snapshot(dst []byte) ([]byte, error) {
	if d.broken {
		return dst, ErrBroken
	}
	var z *zstd.Encoder
	if d.opts.Flags&FlagZstd != 0 {
		var err error
		z, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
		if err != nil {
			return dst, err
		}
		defer func() { _ = z.Close() }()
	}
	var err error
	dst, err = appendFrame(dst, nil, ResetFrame, 0, nil, d.opts.MaxFrameSize)
	if err != nil {
		return dst, err
	}
	var raw []byte
	for first := 0; first < len(d.entries); {
		end, size := first, 20
		for end < len(d.entries) {
			n := len(d.name(end)) + 27
			if end > first && size+n > d.opts.FrameSize {
				break
			}
			size += n
			end++
		}
		raw = raw[:0]
		raw = binary.AppendUvarint(raw, uint64(first))
		raw = binary.AppendUvarint(raw, uint64(end-first))
		for i, e := range d.entries[first:end] {
			name := d.name(first + i)
			raw = binary.AppendUvarint(raw, uint64(len(name)))
			raw = append(raw, name...)
			if e.last.valid {
				raw = append(raw, 1)
				raw = binary.BigEndian.AppendUint64(raw, uint64(e.last.ts))
				raw = binary.BigEndian.AppendUint64(raw, e.last.bits)
			} else {
				raw = append(raw, 0)
			}
		}
		dst, err = appendFrame(dst, raw, StateFrame, 0, z, d.opts.MaxFrameSize)
		if err != nil {
			return dst, err
		}
		first = end
	}
	return dst, nil
}

// NameCount reports names whose ids are currently valid.
func (d *Decoder) NameCount() int { return len(d.entries) }

// RetainedBytes includes dictionary capacities and the reserved caller slots.
func (d *Decoder) RetainedBytes() int {
	n := cap(d.entries) * 40
	if d.resolve == nil {
		n += d.arena.bytes + cap(d.names)*12 + cap(d.entries)*32
	}
	return n
}

// NameReference provides immutable bytes owned by an external series index.
// The reference is held by each connection's slot; wire ids remain independent.
type NameReference interface{ StreamName() []byte }

// SetNameResolver binds validated definitions to external series identities.
// Call between frames. Existing ids are resolved once; no point is emitted.
// Returned values must implement NameReference and live until their last slot
// is released. Visit must preserve the bound reference in *slot.
// The resolver is never called for a rejected frame.
func (d *Decoder) SetNameResolver(resolve func([]byte) any) {
	if resolve == nil || d.resolve != nil {
		return
	}
	for i := range d.entries {
		d.entries[i].slot = resolve(d.name(i))
	}
	d.resolve = resolve
	d.names = nil
	d.arena = nameArena{quantum: d.arena.quantum}
}
func (d *Decoder) name(i int) []byte {
	if d.resolve != nil {
		return d.entries[i].slot.(NameReference).StreamName()
	}
	return d.arena.name(d.names[i])
}
func (d *Decoder) bind(first int) {
	if d.resolve != nil {
		for i, name := range d.definitions {
			d.entries[first+i].slot = d.resolve(name)
		}
	}
	clear(d.definitions)
	d.definitions = d.definitions[:0]
}
