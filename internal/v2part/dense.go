package v2part

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// DenseEncoding is an opt-in value encoding. GCP3 keys retain their identity;
// the key is the first physical slot of a run, not an event-time partition.
const DenseEncoding = "core-rollup-dense-v1"
const MaxDenseSlots = 4096

var denseMagic = []byte{'G', 'C', 'D', 1}

// headerMagic marks a dense run that carries its metric's envelope, like one
// block header per series per part: magic | uvarint(len) | envelope | run. The
// envelope sorts after every run of its metric, so a name's last run holds it
// and the name pays no separate envelope KV. Parts written before this form
// keep the envelope as its own KV at MetadataArchive; both read the same.
var headerMagic = []byte{'G', 'C', 'P', 1}

// LegacyEnvelopeKV makes new dense writers keep the separate envelope KV, the
// form older readers understand. It exists for compatibility tests and for a
// rollback to a reader without header-in-run support.
var LegacyEnvelopeKV atomic.Bool

// SplitHeader returns a dense value's folded envelope and its run. A value
// without a header returns a nil envelope and the value itself.
func SplitHeader(v []byte) (envelope, run []byte, err error) {
	if !bytes.HasPrefix(v, headerMagic) {
		return nil, v, nil
	}
	n, k := binary.Uvarint(v[len(headerMagic):])
	if k <= 0 || n == 0 || n >= uint64(len(v)-len(headerMagic)-k) {
		return nil, nil, errors.New("invalid dense run header")
	}
	p := v[len(headerMagic)+k:]
	return p[:n], p[n:], nil
}

// EnvelopeKey is the key a metric's envelope has in the logical record stream.
func EnvelopeKey(k Key) Key {
	k.Archive, k.BlockStart, k.Ordinal = MetadataArchive, 0, 0
	return k
}

// EncodeSlots preserves every occupied slot, timestamp, value bit, replacement
// order and correction flag. Missing slots cost only a delta, never padding.
// Runs are bounded and strictly increasing in physical slot, including across
// an event-time ring wrap. No arithmetic on float values is used unless exact
// integer reconstruction has been checked bit for bit.
func EncodeSlots(records []sealrollup.Record) ([]byte, error) {
	return appendSlots(nil, records)
}

// Mode bit 0x04 codes the order column as zigzag delta-of-delta (first order,
// first delta, then delta changes) in place of the XOR chain: regular arrival
// orders cost one byte per slot. Decoding is exact. Unknown mode bits are
// rejected; 0x02 is reserved for the slot-time code.
const denseModeOrderDoD = 4

func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func uvarintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

// dodSmaller reports whether delta-of-delta codes the orders in fewer bytes.
func dodSmaller(records []sealrollup.Record) bool {
	var x, d int
	var prev uint64
	var pd int64
	for i, r := range records {
		delta := int64(r.Order - prev)
		x += uvarintLen(r.Order ^ prev)
		d += uvarintLen(zigzag(delta - pd))
		if prev = r.Order; i > 0 {
			pd = delta
		}
	}
	return d < x
}

func appendSlots(out []byte, records []sealrollup.Record) ([]byte, error) {
	return appendSlotsOrder(out, records, false)
}

// appendSlotsOrder with deltas codes orders as delta-of-delta when smaller.
func appendSlotsOrder(out []byte, records []sealrollup.Record, deltas bool) ([]byte, error) {
	if len(records) == 0 || len(records) > MaxDenseSlots {
		return nil, errors.New("invalid dense slot count")
	}
	integer := true
	for i, r := range records {
		if !validSlotRecord(r) {
			return nil, errors.New("invalid sealed archive record clock/order/slot")
		}
		if i > 0 && (r.Archive != records[0].Archive || r.Slot <= records[i-1].Slot) {
			return nil, errors.New("unordered dense slots")
		}
		f := math.Float64frombits(r.Sample.Bits)
		if !(f >= -(1<<53) && f <= 1<<53) || math.Float64bits(float64(int64(f))) != r.Sample.Bits {
			integer = false
		}
	}
	out = append(out, denseMagic...)
	mode := byte(0)
	if integer {
		mode = 1
	}
	dod := deltas && dodSmaller(records)
	if dod {
		mode |= denseModeOrderDoD
	}
	out = append(out, mode)
	out = binary.AppendUvarint(out, uint64(len(records)))
	var slot, ts, dt int64
	for _, r := range records {
		out = binary.AppendUvarint(out, uint64(r.Slot-slot))
		slot = r.Slot
	}
	for _, r := range records {
		delta := r.Sample.Time - ts
		out = binary.AppendVarint(out, delta-dt)
		ts = r.Sample.Time
		dt = delta
	}
	var prev uint64
	var pd int64
	for i, r := range records {
		if dod {
			delta := int64(r.Order - prev)
			out = binary.AppendUvarint(out, zigzag(delta-pd))
			if i > 0 {
				pd = delta
			}
		} else {
			out = binary.AppendUvarint(out, r.Order^prev)
		}
		prev = r.Order
	}
	flags := len(out)
	out = append(out, make([]byte, (len(records)+7)/8)...)
	for i, r := range records {
		if r.Correction {
			out[flags+i/8] |= 1 << uint(i%8)
		}
	}
	if integer {
		var prev int64
		for _, r := range records {
			v := int64(math.Float64frombits(r.Sample.Bits))
			out = binary.AppendVarint(out, v-prev)
			prev = v
		}
	} else {
		for plane := uint(0); plane < 8; plane++ {
			for _, r := range records {
				out = append(out, byte(r.Sample.Bits>>(plane*8)))
			}
		}
	}
	return out, nil
}

// DecodeSlots also accepts the published singleton codec. It bounds allocations
// before decoding, rejects overflow/trailing bytes and never treats absence as
// a NaN sentinel. Returned records own all their scalar state.
func DecodeSlots(archive uint32, firstSlot int64, data []byte) ([]sealrollup.Record, error) {
	return DecodeSlotsInto(nil, archive, firstSlot, data)
}

// DecodeSlotsInto is DecodeSlots with caller-owned reusable scalar storage.
func DecodeSlotsInto(dst []sealrollup.Record, archive uint32, firstSlot int64, data []byte) ([]sealrollup.Record, error) {
	if !bytes.HasPrefix(data, denseMagic) {
		r, e := sealrollup.DecodeRecord(int(archive), firstSlot, data)
		if e != nil {
			return nil, e
		}
		return append(dst[:0], r), nil
	}
	bad := errors.New("invalid dense archive run")
	if len(data) < 6 || data[4]&^(1|denseModeOrderDoD) != 0 || archive == MetadataArchive || firstSlot < 0 {
		return nil, bad
	}
	integer := data[4]&1 == 1
	p := data[5:]
	take := func() (uint64, bool) {
		v, n := binary.Uvarint(p)
		if n <= 0 {
			return 0, false
		}
		p = p[n:]
		return v, true
	}
	count, ok := take()
	if !ok || count == 0 || count > MaxDenseSlots || count > uint64(len(p)) {
		return nil, bad
	}
	dod := data[4]&denseModeOrderDoD != 0
	var rs []sealrollup.Record
	if cap(dst) >= int(count) {
		rs = dst[:int(count)]
		clear(rs)
	} else {
		rs = make([]sealrollup.Record, int(count))
	}
	var slot int64
	for i := range rs {
		v, ok := take()
		if !ok || v > math.MaxInt64 || slot > math.MaxInt64-int64(v) || (i > 0 && v == 0) {
			return nil, bad
		}
		slot += int64(v)
		rs[i].Archive = int(archive)
		rs[i].Slot = slot
	}
	if rs[0].Slot != firstSlot {
		return nil, bad
	}
	var ts, dt int64
	for i := range rs {
		v, n := binary.Varint(p)
		if n <= 0 {
			return nil, bad
		}
		p = p[n:]
		if (v > 0 && dt > math.MaxInt64-v) || (v < 0 && dt < math.MinInt64-v) {
			return nil, bad
		}
		dt += v
		if dt < -math.MaxUint32 || dt > math.MaxUint32 {
			return nil, bad
		}
		ts += dt
		if ts <= 0 || ts > math.MaxUint32 {
			return nil, bad
		}
		rs[i].Sample.Time = ts
	}
	var order uint64
	var pd int64
	for i := range rs {
		v, ok := take()
		if !ok {
			return nil, bad
		}
		if dod {
			delta := pd + (int64(v>>1) ^ -int64(v&1))
			if order += uint64(delta); i > 0 {
				pd = delta
			}
		} else {
			order ^= v
		}
		if order == 0 {
			return nil, bad
		}
		rs[i].Order = order
		rs[i].Sample.Sequence = order
	}
	nflags := (len(rs) + 7) / 8
	if len(p) < nflags {
		return nil, bad
	}
	flags := p[:nflags]
	p = p[nflags:]
	if n := uint(len(rs) % 8); n != 0 && flags[len(flags)-1]>>n != 0 {
		return nil, bad
	}
	for i := range rs {
		rs[i].Correction = flags[i/8]&(1<<uint(i%8)) != 0
	}
	if integer {
		var prev int64
		for i := range rs {
			v, n := binary.Varint(p)
			if n <= 0 || (v > 0 && prev > math.MaxInt64-v) || (v < 0 && prev < math.MinInt64-v) {
				return nil, bad
			}
			p = p[n:]
			prev += v
			if prev < -(1<<53) || prev > 1<<53 {
				return nil, bad
			}
			rs[i].Sample.Bits = math.Float64bits(float64(prev))
		}
	} else {
		if len(p) != 8*len(rs) {
			return nil, bad
		}
		for plane := uint(0); plane < 8; plane++ {
			for i := range rs {
				rs[i].Sample.Bits |= uint64(p[int(plane)*len(rs)+i]) << (plane * 8)
			}
		}
		p = p[8*len(rs):]
	}
	if len(p) != 0 {
		return nil, bad
	}
	return rs, nil
}

// DenseWriter groups a sorted singleton stream into bounded runs. It reuses
// scalar packing buffers across files; scope and archive policy validation
// remain the shared Writer's responsibility.
type DenseWriter struct {
	writer *Writer
	*denseWorkspace
	err    error
	closed bool
	// dod: runs code orders as delta-of-delta when smaller (OrderDeltas).
	dod bool
}

// OrderDeltas codes each run's orders as delta-of-delta when that is smaller
// than the XOR chain, including copied runs. Decoding is exact. Call before
// the first Set.
func (w *DenseWriter) OrderDeltas() { w.dod = true }

// LimitBytes bounds compressed staging output, including SST indexes/footer.
// Call before the first Set. Exceeding it aborts this proposal, never inputs.
func (w *DenseWriter) LimitBytes(n uint64) { w.writer.out.limit = n }

// SealCompression writes this part's blocks at the fast seal level. Use it
// only for parts a background merge rewrites; call before the first Set.
func (w *DenseWriter) SealCompression() { w.writer.sst.(*denseSSTWriter).level = denseSealLevel }

// SetExpiry records expiry bounds measured while streaming. Call before Close.
func (w *DenseWriter) SetExpiry(minExpiry, maxExpiry int64) {
	w.writer.spec.MinExpiry, w.writer.spec.MaxExpiry = minExpiry, maxExpiry
}

type denseWorkspace struct {
	decoder keyDecoder
	first   []byte
	encoded []byte
	key     Key
	records []sealrollup.Record
	// raw is a copied run awaiting its metric's envelope (rawPending).
	raw        []byte
	rawPending bool
	fold       bool
	last       []byte
	// Planned input: the previous tuple, its planner rank and the scope's
	// group key prefix.
	prev     Key
	prevRank uint32
	havePrev bool
	group    []byte
}

var denseWorkspaces sync.Pool

func (w *DenseWriter) releaseWorkspace() {
	b := w.denseWorkspace
	if b == nil {
		return
	}
	b.decoder.key, b.decoder.name, b.key = Key{}, "", Key{}
	b.decoder.prefix = b.decoder.prefix[:0]
	b.first, b.encoded, b.records, b.last = b.first[:0], b.encoded[:0], b.records[:0], b.last[:0]
	b.raw, b.rawPending = b.raw[:0], false
	b.prev, b.prevRank, b.havePrev, b.group = Key{}, 0, false, b.group[:0]
	w.denseWorkspace = nil
	denseWorkspaces.Put(b)
}

func NewDenseWriter(path string, spec Spec) (*DenseWriter, error) {
	spec.Encoding = DenseEncoding
	w, e := NewWriter(path, spec)
	if e != nil {
		return nil, e
	}
	b, _ := denseWorkspaces.Get().(*denseWorkspace)
	if b == nil {
		b = &denseWorkspace{}
	}
	b.fold = !LegacyEnvelopeKV.Load()
	return &DenseWriter{writer: w, denseWorkspace: b}, nil
}
func (w *DenseWriter) flush() error { return w.flushWith(nil) }

// flushWith writes the pending run, prefixed by envelope when it is non-nil.
func (w *DenseWriter) flushWith(envelope []byte) error {
	if len(w.records) == 0 && !w.rawPending {
		return nil
	}
	b := w.encoded[:0]
	if envelope != nil {
		b = append(b, headerMagic...)
		b = binary.AppendUvarint(b, uint64(len(envelope)))
		b = append(b, envelope...)
	}
	var e error
	if w.rawPending {
		b = append(b, w.raw...)
	} else {
		b, e = appendSlotsOrder(b, w.records, w.dod)
	}
	w.encoded = b
	if e == nil {
		e = w.writer.setDecoded(w.first, w.key, b)
	}
	w.records, w.rawPending = w.records[:0], false
	return e
}

// setEnvelope writes metric's envelope k: folded into the pending run when
// that run is the metric's last, else as its own KV at key.
func (w *DenseWriter) setEnvelope(key []byte, k Key, value []byte) error {
	if w.fold && len(value) > 0 && (len(w.records) > 0 || w.rawPending) && w.key.Metric == k.Metric {
		return w.flushWith(value)
	}
	if e := w.flush(); e != nil {
		return e
	}
	return w.writer.setDecoded(key, k, value)
}

// NameHint names the metric of the following keys for a caller that already
// holds that string: a matching key shares it instead of copying its name.
// Every key is still fully validated; a stale hint only costs the copy.
func (w *DenseWriter) NameHint(metric ID) { w.decoder.name = metric }

func (w *DenseWriter) SetEncoded(key, value []byte) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	w.err = w.set(key, value)
	return w.err
}

// SetRecordEncoded accepts a planner's exact scalar record without framing it as
// a singleton value first. The untrusted key, scalar bounds, archive and physical
// slot are validated exactly as in SetEncoded; no caller buffers are retained.
func (w *DenseWriter) SetRecordEncoded(key []byte, record sealrollup.Record) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	k, err := w.decodeInput(key)
	if err == nil && (k.Archive == MetadataArchive || k.Ordinal != 0 || record.Archive < 0 || uint64(record.Archive) != uint64(k.Archive) || record.Slot != k.BlockStart || !validSlotRecord(record)) {
		err = errors.New("dense scalar record does not match valid archive key")
	}
	if err == nil {
		err = w.appendRecord(key, k, record)
	}
	w.err = err
	return err
}

// SetSlotsEncoded copies a complete non-overlapping dense run without
// repacking its values. Validation uses the writer's reusable scalar buffer;
// pending singleton output is flushed first. Caller buffers are not retained.
func (w *DenseWriter) SetSlotsEncoded(key, value []byte) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	k, err := w.decodeInput(key)
	if err == nil && (k.Archive == MetadataArchive || k.Ordinal != 0) {
		err = errors.New("dense run requires archive and ordinal zero")
	}
	if err == nil {
		err = w.flush()
	}
	if err == nil {
		w.records, err = DecodeSlotsInto(w.records[:0], k.Archive, k.BlockStart, value)
	}
	if err == nil {
		// Future inputs must start after this run's last physical slot.
		binary.BigEndian.PutUint64(w.last[len(w.last)-12:], uint64(w.records[len(w.records)-1].Slot)^(1<<63))
		if w.dod && value[4]&denseModeOrderDoD == 0 && dodSmaller(w.records) {
			value, err = appendSlotsOrder(w.encoded[:0], w.records, true)
			w.encoded = value
		}
	}
	if err == nil {
		if w.fold {
			// Hold the copy: if it is the metric's last run, it carries the envelope.
			w.first, w.key, w.raw, w.rawPending = append(w.first[:0], key...), k, append(w.raw[:0], value...), true
		} else {
			err = w.writer.setDecoded(key, k, value)
		}
	}
	w.records = w.records[:0]
	w.err = err
	return err
}

// AppendSlots appends a decoded non-overlapping dense run (records, encoded as
// value) after the pending output. A run that cannot grow, MaxDenseSlots long
// with no pending run of its metric and archive, keeps its exact encoding as in
// SetSlotsEncoded. Any shorter run joins the pending run, which re-encodes in
// runs of up to MaxDenseSlots, so merges repack short runs instead of freezing
// them. key is the first record's key. Every record is validated as
// SetRecordEncoded validates it; caller buffers are not retained.
func (w *DenseWriter) AppendSlots(key, value []byte, records []sealrollup.Record) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	if len(records) == MaxDenseSlots && (len(w.records) == 0 || len(w.records) == MaxDenseSlots || w.records[0].Archive != records[0].Archive || !bytes.Equal(w.first[:len(w.first)-16], key[:max(len(key)-16, 0)])) {
		return w.SetSlotsEncoded(key, value)
	}
	k, err := w.decodeInput(key)
	if err == nil && (len(records) == 0 || k.Archive == MetadataArchive || k.Ordinal != 0 || records[0].Archive < 0 || uint64(records[0].Archive) != uint64(k.Archive) || records[0].Slot != k.BlockStart) {
		err = errors.New("dense run does not match valid archive key")
	}
	if err == nil && w.rawPending {
		// A held copy is complete: write it before this run joins w.records.
		err = w.flush()
	}
	for i := 0; err == nil && i < len(records); i++ {
		r := records[i]
		if r.Archive != records[0].Archive || !validSlotRecord(r) || (i > 0 && r.Slot <= records[i-1].Slot) {
			err = errors.New("invalid dense run record")
			break
		}
		if len(w.records) > 0 && (k.Metric != w.key.Metric || k.Archive != w.key.Archive || len(w.records) == MaxDenseSlots) {
			if err = w.flush(); err != nil {
				break
			}
		}
		if len(w.records) == 0 {
			k.BlockStart = r.Slot
			w.key = k
			w.first = append(w.first[:0], key...)
			binary.BigEndian.PutUint64(w.first[len(w.first)-12:], uint64(r.Slot)^(1<<63))
		}
		w.records = append(w.records, r)
	}
	if err == nil {
		// Future inputs must start after this run's last physical slot.
		binary.BigEndian.PutUint64(w.last[len(w.last)-12:], uint64(records[len(records)-1].Slot)^(1<<63))
	}
	w.err = err
	return err
}

func validSlotRecord(r sealrollup.Record) bool {
	return r.Archive >= 0 && r.Slot >= 0 && r.Order != 0 && r.Sample.Time > 0 && r.Sample.Time <= math.MaxUint32
}
func (w *DenseWriter) decodeInput(key []byte) (Key, error) {
	k, err := w.decoder.decode(key)
	if err != nil {
		return Key{}, err
	}
	if !w.writer.spec.Scope.contains(k) || (len(w.last) > 0 && bytes.Compare(w.last, key) >= 0) {
		return Key{}, errors.New("unordered or foreign dense input")
	}
	w.last = append(w.last[:0], key...)
	return k, nil
}
func (w *DenseWriter) set(key, value []byte) error {
	k, e := w.decodeInput(key)
	if e != nil {
		return e
	}
	if k.Archive == MetadataArchive {
		return w.setEnvelope(key, k, value)
	}
	if k.Ordinal != 0 {
		return errors.New("dense input requires ordinal zero")
	}
	r, e := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, value)
	if e != nil {
		return e
	}
	return w.appendRecord(key, k, r)
}

// appendRecord starts a run at key, or at k's formatted key when key is nil.
func (w *DenseWriter) appendRecord(key []byte, k Key, r sealrollup.Record) error {
	if w.rawPending {
		if e := w.flush(); e != nil {
			return e
		}
	}
	if len(w.records) > 0 && (k.Metric != w.key.Metric || k.Archive != w.key.Archive || len(w.records) == MaxDenseSlots) {
		if e := w.flush(); e != nil {
			return e
		}
	}
	if len(w.records) == 0 {
		w.key = k
		if key == nil {
			w.first = w.appendKey(w.first[:0], k)
		} else {
			w.first = append(w.first[:0], key...)
		}
	}
	w.records = append(w.records, r)
	return nil
}

// SetPlannedRecord packs a planner record of metric, routed to vshard, without
// an encoded input key. Group, partition and part come from the writer scope,
// and key bytes are formatted only where a dense run starts. Order, routing,
// archive and scalar bounds are validated as SetRecordEncoded validates them.
// Planned and encoded input cannot be mixed in one part.
//
// rank is the metric's 1-based position in the planner's canonical
// (group, vshard, length, name) order, or 0 when unranked. Between two ranked
// tuples the writer checks order by rank and never reads the names; the
// caller must pass ranks of one planner epoch only.
func (w *DenseWriter) SetPlannedRecord(vshard uint16, rank uint32, metric ID, r *sealrollup.Record) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	var k Key
	var err error
	if r.Archive >= 0 && int64(r.Archive) < int64(MetadataArchive) && validSlotRecord(*r) {
		k, err = w.plannedKey(vshard, rank, metric, uint32(r.Archive), r.Slot)
	} else {
		err = errors.New("dense scalar record does not match valid archive key")
	}
	if err == nil {
		err = w.appendRecord(nil, k, *r)
	}
	w.err = err
	return err
}

// SetPlannedMetadata writes metric's envelope value after its archive records.
// The caller's value is not retained.
func (w *DenseWriter) SetPlannedMetadata(vshard uint16, rank uint32, metric ID, value []byte) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	k, err := w.plannedKey(vshard, rank, metric, MetadataArchive, 0)
	if err == nil && w.fold && len(value) > 0 && len(w.records) > 0 && w.key.Metric == metric {
		err = w.flushWith(value)
	} else if err == nil {
		err = w.flush()
		if err == nil {
			w.first = w.appendKey(w.first[:0], k)
			err = w.writer.setDecoded(w.first, k, value)
		}
	}
	w.err = err
	return err
}

// plannedKey checks that a tuple sorts strictly after the previous one in GCP3
// key order (vshard, name length, name, archive, slot) and validates each new
// name's length and route once. Two ranked tuples compare by rank, which
// follows that name order, so the check reads no name bytes.
func (w *DenseWriter) plannedKey(vshard uint16, rank uint32, metric ID, archive uint32, slot int64) (Key, error) {
	if len(w.last) > 0 {
		return Key{}, errors.New("mixed encoded and planned dense input")
	}
	p := &w.prev
	if w.havePrev {
		c := cmp.Compare(vshard, p.VShard)
		if c == 0 && rank != 0 && w.prevRank != 0 {
			c = cmp.Compare(rank, w.prevRank)
		} else if c == 0 {
			if c = cmp.Compare(len(metric), len(p.Metric)); c == 0 {
				c = strings.Compare(string(metric), string(p.Metric))
			}
		}
		if c == 0 {
			if c = cmp.Compare(archive, p.Archive); c == 0 {
				c = cmp.Compare(slot, p.BlockStart)
			}
			if c <= 0 {
				return Key{}, errors.New("unordered planned dense input")
			}
			p.Archive, p.BlockStart = archive, slot
			return *p, nil
		}
		if c < 0 {
			return Key{}, errors.New("unordered planned dense input")
		}
	}
	s := w.writer.spec.Scope
	if len(metric) == 0 || len(metric) > MaxNameBytes || vshard >= VirtualShards || Route(metric) != vshard || (s.VShard != GroupVShard && s.VShard != vshard) {
		return Key{}, errors.New("invalid or foreign planned dense metric")
	}
	if len(w.group) == 0 {
		group, _, err := GroupRange(s.Group)
		if err != nil {
			return Key{}, err
		}
		w.group = append(w.group[:0], group...)
	}
	*p = Key{Group: s.Group, VShard: vshard, Partition: s.Partition, Part: s.Part, Metric: metric, Archive: archive, BlockStart: slot}
	w.prevRank, w.havePrev = rank, true
	return *p, nil
}

// appendKey formats k exactly as Encode does, from the cached group prefix.
func (w *DenseWriter) appendKey(dst []byte, k Key) []byte {
	dst = append(dst, w.group...)
	dst = binary.BigEndian.AppendUint16(dst, k.VShard)
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Partition)^(1<<63))
	dst = append(dst, k.Part[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(k.Metric)))
	dst = append(dst, k.Metric...)
	dst = binary.BigEndian.AppendUint32(dst, k.Archive)
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.BlockStart)^(1<<63))
	return binary.BigEndian.AppendUint32(dst, k.Ordinal)
}
func (w *DenseWriter) Abort() error {
	w.closed = true
	defer w.releaseWorkspace()
	return w.writer.Abort()
}
func (w *DenseWriter) Close() (Descriptor, error) {
	if w.closed {
		return Descriptor{}, io.ErrClosedPipe
	}
	w.closed = true
	defer w.releaseWorkspace()
	if w.err == nil {
		w.err = w.flush()
	}
	if w.err != nil {
		return Descriptor{}, errors.Join(w.err, w.Abort())
	}
	return w.writer.Close()
}

// ReadExpanded reconstructs the exact published singleton keys/values for
// converter verification. Descriptor coverage counts physical SST records.
func ReadExpanded(ctx context.Context, path string, d *Descriptor, visit func(Key, []byte) error) error {
	var last []byte
	return Read(ctx, path, d, func(k Key, b []byte) error {
		emit := func(key Key, value []byte) error {
			enc, e := Encode(key)
			if e != nil {
				return e
			}
			if len(last) > 0 && bytes.Compare(last, enc) >= 0 {
				return errors.New("overlapping dense runs")
			}
			last = append(last[:0], enc...)
			if visit != nil {
				return visit(key, value)
			}
			return nil
		}
		if k.Archive == MetadataArchive {
			return emit(k, b)
		}
		if k.Ordinal != 0 {
			return errors.New("unexpected dense ordinal")
		}
		rs, e := DecodeSlots(k.Archive, k.BlockStart, b)
		if e != nil {
			return e
		}
		for _, r := range rs {
			k.BlockStart = r.Slot
			if e = emit(k, sealrollup.EncodeRecord(r)); e != nil {
				return e
			}
		}
		return nil
	})
}
