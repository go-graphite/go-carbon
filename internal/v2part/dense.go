package v2part

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"

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
	out = append(out, mode)
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
	if !bytes.HasPrefix(data, denseMagic) {
		r, e := sealrollup.DecodeRecord(int(archive), firstSlot, data)
		if e != nil {
			return nil, e
		}
		return []sealrollup.Record{r}, nil
	}
	bad := errors.New("invalid dense archive run")
	if len(data) < 6 || data[4] > 1 || archive == MetadataArchive || firstSlot < 0 {
		return nil, bad
	}
	integer := data[4] == 1
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
	rs := make([]sealrollup.Record, int(count))
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

// DenseWriter groups a sorted singleton stream into bounded runs. It owns no
// cross-file buffers and never reads before writing. Scope and archive policy
// validation remain the shared Writer's responsibility.
type DenseWriter struct {
	writer  *Writer
	decoder keyDecoder
	first   []byte
	encoded []byte
	key     Key
	records []sealrollup.Record
	last    []byte
	err     error
	closed  bool
}

func NewDenseWriter(path string, spec Spec) (*DenseWriter, error) {
	spec.Encoding = DenseEncoding
	w, e := NewWriter(path, spec)
	if e != nil {
		return nil, e
	}
	return &DenseWriter{writer: w}, nil
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
func (w *DenseWriter) appendRecord(key []byte, k Key, r sealrollup.Record) error {
	if len(w.records) > 0 && (k.Metric != w.key.Metric || k.Archive != w.key.Archive || len(w.records) == MaxDenseSlots) {
		if e := w.flush(); e != nil {
			return e
		}
	}
	if len(w.records) == 0 {
		w.key = k
		w.first = append(w.first[:0], key...)
	}
	w.records = append(w.records, r)
	return nil
}
func (w *DenseWriter) Abort() error { w.closed = true; return w.writer.Abort() }
func (w *DenseWriter) Close() (Descriptor, error) {
	if w.closed {
		return Descriptor{}, io.ErrClosedPipe
	}
	w.closed = true
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
