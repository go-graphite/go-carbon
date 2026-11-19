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

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// DenseEncoding is an opt-in value encoding. GCP3 keys retain their identity;
// the key is the first physical slot of a run, not an event-time partition.
const DenseEncoding = "core-rollup-dense-v1"
const MaxDenseSlots = 4096

var denseMagic = []byte{'G', 'C', 'D', 1}

// EncodeSlots preserves every occupied slot, timestamp, value bit, replacement
// order and correction flag. Missing slots cost only a delta, never padding.
// Runs are bounded and strictly increasing in physical slot, including across
// an event-time ring wrap. No arithmetic on float values is used unless exact
// integer reconstruction has been checked bit for bit.
func EncodeSlots(records []sealrollup.Record) ([]byte, error) {
	return appendSlots(nil, records)
}

func appendSlots(out []byte, records []sealrollup.Record) ([]byte, error) {
	if len(records) == 0 || len(records) > MaxDenseSlots {
		return nil, errors.New("invalid dense slot count")
	}
	for i, r := range records {
		if !validSlotRecord(r) {
			return nil, errors.New("invalid sealed archive record clock/order/slot")
		}
		if i > 0 && (r.Archive != records[0].Archive || r.Slot <= records[i-1].Slot) {
			return nil, errors.New("unordered dense slots")
		}
	}
	out = append(out, denseMagic...)
	out = append(out, denseTagged)
	out = binary.AppendUvarint(out, uint64(len(records)))
	var slot, ts, dt int64
	var order uint64
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
	for _, r := range records {
		out = binary.AppendUvarint(out, r.Order^order)
		order = r.Order
	}
	flags := len(out)
	out = append(out, make([]byte, (len(records)+7)/8)...)
	for i, r := range records {
		if r.Correction {
			out[flags+i/8] |= 1 << uint(i%8)
		}
	}
	return appendValues(out, records), nil
}

// Value column codecs. A run's tag byte (mode bit denseTagged) names one codec
// for all its values: decimal runs store v = m / 10^e with one exponent e per
// run and integer mantissas m as zigzag first or second differences, whichever
// packs smaller; other runs, and decimal runs whose mantissa differences average
// more than decimalMaxCost bytes, store eight raw byte planes. Every decimal value's
// float64 bits are checked at encode, so NaN payloads, -0 and infinities always
// take the byte planes.
const (
	denseTagged   = 0x80
	valuePlanes   = 0 << 5
	valueDelta1   = 1 << 5
	valueDelta2   = 2 << 5
	maxDecimalExp = 18
	// Above this mean varint size per value, mantissas are wide and noisy:
	// the byte planes' constant exponent and zero low bytes compress better.
	decimalMaxCost = 4
)

var pow10 = [maxDecimalExp + 1]float64{1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18}

// decimalMantissa returns m with float64(m)/10^e bit-identical to bits.
func decimalMantissa(bits uint64, e int) (int64, bool) {
	s := math.Float64frombits(bits) * pow10[e]
	if !(s >= -(1<<53) && s <= 1<<53) {
		return 0, false
	}
	m := int64(math.Round(s))
	return m, math.Float64bits(float64(m)/pow10[e]) == bits
}

func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func varintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

func appendValues(out []byte, records []sealrollup.Record) []byte {
	e := 0
	for _, r := range records {
		for ; e <= maxDecimalExp; e++ {
			if _, ok := decimalMantissa(r.Sample.Bits, e); ok {
				break
			}
		}
		if e > maxDecimalExp {
			break
		}
	}
	if e <= maxDecimalExp {
		var prev, prevDelta int64
		cost1, cost2 := 0, 0
		ok := true
		for _, r := range records {
			m, exact := decimalMantissa(r.Sample.Bits, e)
			if !exact {
				ok = false
				break
			}
			d := m - prev
			cost1 += varintLen(zigzag(d))
			cost2 += varintLen(zigzag(d - prevDelta))
			prev, prevDelta = m, d
		}
		if ok && min(cost1, cost2) <= decimalMaxCost*len(records) {
			tag := byte(valueDelta1)
			if cost2 < cost1 {
				tag = valueDelta2
			}
			out = append(out, tag|byte(e))
			prev, prevDelta = 0, 0
			for _, r := range records {
				m, _ := decimalMantissa(r.Sample.Bits, e)
				d := m - prev
				if tag == valueDelta2 {
					out = binary.AppendUvarint(out, zigzag(d-prevDelta))
				} else {
					out = binary.AppendUvarint(out, zigzag(d))
				}
				prev, prevDelta = m, d
			}
			return out
		}
	}
	out = append(out, valuePlanes)
	for plane := uint(0); plane < 8; plane++ {
		for _, r := range records {
			out = append(out, byte(r.Sample.Bits>>(plane*8)))
		}
	}
	return out
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
	if len(data) < 6 || archive == MetadataArchive || firstSlot < 0 {
		return nil, bad
	}
	mode := data[4]
	if mode != 0 && mode != 1 && mode != denseTagged {
		return nil, bad
	}
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
	for i := range rs {
		v, ok := take()
		if !ok {
			return nil, bad
		}
		order ^= v
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
	tag := byte(valuePlanes)
	switch mode {
	case 1: // legacy integer run: first differences, plain varints, e = 0
		tag = valueDelta1
	case denseTagged:
		if len(p) == 0 {
			return nil, bad
		}
		tag, p = p[0], p[1:]
	}
	var err error
	if p, err = decodeValues(rs, tag, mode == 1, p); err != nil {
		return nil, bad
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
}

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
	last    []byte
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
	return &DenseWriter{writer: w, denseWorkspace: b}, nil
}
func (w *DenseWriter) flush() error {
	if len(w.records) == 0 {
		return nil
	}
	b, e := appendSlots(w.encoded[:0], w.records)
	w.encoded = b
	if e == nil {
		e = w.writer.setDecoded(w.first, w.key, b)
	}
	w.records = w.records[:0]
	return e
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
		err = w.writer.setDecoded(key, k, value)
	}
	w.records = w.records[:0]
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
		if e = w.flush(); e != nil {
			return e
		}
		return w.writer.setDecoded(key, k, value)
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
	if err == nil {
		err = w.flush()
	}
	if err == nil {
		w.first = w.appendKey(w.first[:0], k)
		err = w.writer.setDecoded(w.first, k, value)
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

// decodeValues fills rs' value bits from a value column coded as tag. legacy
// selects the untagged integer run's signed varints.
func decodeValues(rs []sealrollup.Record, tag byte, legacy bool, p []byte) ([]byte, error) {
	bad := errors.New("invalid dense value column")
	kind, e := tag&^31, int(tag&31)
	if kind == valuePlanes {
		if e != 0 || len(p) < 8*len(rs) {
			return nil, bad
		}
		for plane := uint(0); plane < 8; plane++ {
			for i := range rs {
				rs[i].Sample.Bits |= uint64(p[int(plane)*len(rs)+i]) << (plane * 8)
			}
		}
		return p[8*len(rs):], nil
	}
	if (kind != valueDelta1 && kind != valueDelta2) || e > maxDecimalExp {
		return nil, bad
	}
	var prev, delta int64
	for i := range rs {
		var v int64
		if legacy {
			x, n := binary.Varint(p)
			if n <= 0 {
				return nil, bad
			}
			v, p = x, p[n:]
		} else {
			u, n := binary.Uvarint(p)
			if n <= 0 {
				return nil, bad
			}
			v, p = int64(u>>1)^-int64(u&1), p[n:]
		}
		if kind == valueDelta2 {
			if (v > 0 && delta > math.MaxInt64-v) || (v < 0 && delta < math.MinInt64-v) {
				return nil, bad
			}
			v += delta
		}
		if (v > 0 && prev > math.MaxInt64-v) || (v < 0 && prev < math.MinInt64-v) {
			return nil, bad
		}
		delta = v
		prev += v
		if prev < -(1<<53) || prev > 1<<53 {
			return nil, bad
		}
		rs[i].Sample.Bits = math.Float64bits(float64(prev) / pow10[e])
	}
	return p, nil
}
