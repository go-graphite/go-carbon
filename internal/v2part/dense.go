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
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// DenseEncoding is an opt-in value encoding. GCP3 keys retain their identity;
// the key is the first physical slot of a run, not an event-time partition.
const DenseEncoding = "core-rollup-dense-v1"
const MaxDenseSlots = 4096

var denseMagic = []byte{'G', 'C', 'D', 1}

// Run mode bits, independent flags; a reader rejects any other bit. An aligned
// run stores slots only: every timestamp is ts0 + (slot - slot0) * step, so the
// header carries ts0 and step instead of a timestamp column. Ring wrap,
// unaligned and single-slot runs keep the delta-of-delta timestamp column.
// Older merged runs use denseModeOrderDoD; new merges use denseModeOrderLinear.
// denseExtended marks a run with an extension byte right after the mode, the
// one place later run formats add columns: the mode byte has no free bit.
// Readers reject an extension byte of zero or with unknown bits.
//
// extKinds marks a run of record versions: a physical slot may repeat
// (oldest version first, never aligned), and after the correction bitmap come
// a 2-bit kind per record (sealrollup.Kind, four per byte, low bits first)
// and one uvarint upto per Mat or None record (0: a seal's fallback window
// that bounds no event walk). Runs of single Raw versions keep the older form.
// extOrderMaximum replaces the order column with one positive run maximum.
// It is valid only for owner-certified, unique-slot single-archive Raw output.
const (
	extKinds        = 1 << 0
	extOrderMaximum = 1 << 1
	extKnown        = extKinds | extOrderMaximum
)

const (
	denseInteger         = 1 << 0
	denseAligned         = 1 << 1
	denseModeOrderDoD    = 1 << 2
	denseExtended        = 1 << 3
	denseModeOrderLinear = 1 << 4
	denseModes           = denseInteger | denseAligned | denseModeOrderDoD | denseExtended | denseModeOrderLinear | denseTagged
)

// alignedStep returns the run's slot step in seconds, or 0 if any timestamp
// is not ts0 + (slot - slot0) * step. Callers have validated strictly
// increasing slots and timestamps in (0, MaxUint32].
func alignedStep(records []sealrollup.Record) int64 {
	if len(records) < 2 {
		return 0
	}
	t0, s0 := records[0].Sample.Time, records[0].Slot
	dt, ds := records[1].Sample.Time-t0, records[1].Slot-s0
	if dt <= 0 || dt%ds != 0 {
		return 0
	}
	step := dt / ds
	for _, r := range records[2:] {
		ds := r.Slot - s0
		if ds > (math.MaxUint32-t0)/step || r.Sample.Time != t0+ds*step {
			return 0
		}
	}
	return step
}

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

// LegacyKinds makes new writers store what per-slot readers resolve: each
// slot's last Raw or Mat record, without kinds, the form readers without the
// kinds column decode. It exists for compatibility tests and for a rollback
// to such a reader (set it, let merges rewrite the parts, then roll back).
var LegacyKinds atomic.Bool

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
	return appendSlots(nil, records, nil)
}

func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func uvarintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

// appendSlots reuses *table, when given, as the value codec's distinct-value
// table.
func appendSlots(out []byte, records []sealrollup.Record, table *[]uint64) ([]byte, error) {
	return appendSlotsOrder(out, records, table, ExactOrders)
}

// appendSlotsOrder predicts merged orders from the two endpoints and stores
// exact zigzag residuals in byte planes. Unsigned arithmetic preserves wraps.
func appendSlotsOrder(out []byte, records []sealrollup.Record, table *[]uint64, orders OrderEncoding) ([]byte, error) {
	if len(records) == 0 || len(records) > MaxDenseSlots {
		return nil, errors.New("invalid dense slot count")
	}
	kinds := false
	for i, r := range records {
		if !validSlotRecord(r) || r.Kind > sealrollup.KindNone || r.Upto != 0 && r.Kind != sealrollup.KindMat && r.Kind != sealrollup.KindNone {
			return nil, errors.New("invalid sealed archive record clock/order/slot")
		}
		if i > 0 && (r.Archive != records[0].Archive || r.Slot < records[i-1].Slot) {
			return nil, errors.New("unordered dense slots")
		}
		kinds = kinds || r.Kind != sealrollup.KindRaw || i > 0 && r.Slot == records[i-1].Slot
	}
	if orders == SummaryOrders && kinds {
		return nil, errors.New("order summary requires unique Raw slots")
	}
	// A singleton's exact order already is its maximum; omit the redundant tag.
	if orders == SummaryOrders && len(records) == 1 {
		orders = ExactOrders
	}
	out = append(out, denseMagic...)
	mode := byte(denseTagged)
	step := int64(0)
	if kinds {
		mode |= denseExtended
	} else if step = alignedStep(records); step != 0 {
		mode |= denseAligned
	}
	predict := orders == PredictedOrders && len(records) > 1
	if orders == SummaryOrders {
		mode |= denseExtended
	}
	if predict {
		mode |= denseModeOrderLinear
	}
	out = append(out, mode)
	if kinds {
		out = append(out, extKinds)
	} else if orders == SummaryOrders {
		out = append(out, extOrderMaximum)
	}
	out = binary.AppendUvarint(out, uint64(len(records)))
	var slot, ts, dt int64
	for _, r := range records {
		out = binary.AppendUvarint(out, uint64(r.Slot-slot))
		slot = r.Slot
	}
	if step != 0 {
		out = binary.AppendUvarint(out, uint64(records[0].Sample.Time))
		out = binary.AppendUvarint(out, uint64(step))
	} else {
		for _, r := range records {
			delta := r.Sample.Time - ts
			out = binary.AppendVarint(out, delta-dt)
			ts = r.Sample.Time
			dt = delta
		}
	}
	if orders == SummaryOrders {
		var maximum uint64
		for _, r := range records {
			maximum = max(maximum, r.Order)
		}
		out = binary.AppendUvarint(out, maximum)
	} else if predict {
		out = appendPredictedOrders(out, records)
	} else {
		var prev uint64
		for _, r := range records {
			out = binary.AppendUvarint(out, r.Order^prev)
			prev = r.Order
		}
	}
	flags := len(out)
	out = append(out, make([]byte, (len(records)+7)/8)...)
	for i, r := range records {
		if r.Correction {
			out[flags+i/8] |= 1 << uint(i%8)
		}
	}
	if kinds {
		at := len(out)
		out = append(out, make([]byte, (len(records)+3)/4)...)
		for i, r := range records {
			out[at+i/4] |= byte(r.Kind) << uint(2*(i%4))
		}
		for _, r := range records {
			if r.Kind == sealrollup.KindMat || r.Kind == sealrollup.KindNone {
				out = binary.AppendUvarint(out, r.Upto)
			}
		}
	}
	return appendValues(out, records, table), nil
}

func appendPredictedOrders(out []byte, records []sealrollup.Record) []byte {
	first := records[0].Order
	step := int64(records[len(records)-1].Order-first) / int64(len(records)-1)
	out = binary.AppendUvarint(out, first)
	out = binary.AppendVarint(out, step)
	var union uint64
	for i, r := range records[1:] {
		union |= zigzag(int64(r.Order - first - uint64(step)*uint64(i+1)))
	}
	width := (bits.Len64(union) + 7) / 8
	out = append(out, byte(width))
	for plane := 0; plane < width; plane++ {
		for i, r := range records[1:] {
			v := zigzag(int64(r.Order - first - uint64(step)*uint64(i+1)))
			out = append(out, byte(v>>uint(plane*8)))
		}
	}
	return out
}

// Value column codecs. A run's tag byte (mode bit denseTagged) names one codec
// for all its values: decimal runs store v = m / 10^e with one exponent e per
// run and integer mantissas m as zigzag first or second differences, whichever
// packs smaller. Other runs, and decimal runs whose mantissa differences average
// more than decimalMaxCost bytes, store their float64 bits verbatim: as raw
// little-endian words when the run has at least wordsMinRun values and at least
// one in twenty repeats an earlier one (zstd then matches whole 8-byte values),
// else as eight byte planes (zstd then packs the near-constant sign and
// exponent bytes). Every decimal
// value's float64 bits are checked at encode, so NaN payloads, -0 and
// infinities always keep their exact bits.
const (
	denseTagged   = 0x80
	valuePlanes   = 0 << 5
	valueDelta1   = 1 << 5
	valueDelta2   = 2 << 5
	valueWords    = 3 << 5
	maxDecimalExp = 18
	wordsMinRun   = 64
	// A run repeats values when fewer than wordsDistinctNum/wordsDistinctDen
	// of them are distinct.
	wordsDistinctNum = 19
	wordsDistinctDen = 20
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

// repeatsValues reports whether fewer than wordsDistinctNum/wordsDistinctDen of
// the run's values are distinct, counting them in an open-addressing table
// (reused from *table when given) and stopping at the limit.
func repeatsValues(records []sealrollup.Record, table *[]uint64) bool {
	shift := 64 - bits.Len(uint(2*len(records)-1))
	size := 1 << (64 - shift)
	var t []uint64
	if table != nil && cap(*table) >= size {
		t = (*table)[:size]
		clear(t)
	} else {
		t = make([]uint64, size)
		if table != nil {
			*table = t
		}
	}
	limit := len(records) * wordsDistinctNum / wordsDistinctDen
	distinct, zero := 0, false
	for _, r := range records {
		v := r.Sample.Bits
		if v == 0 { // 0 marks a free slot, so +0.0 is counted aside
			if !zero {
				zero, distinct = true, distinct+1
			}
		} else {
			i := (v * 0x9e3779b97f4a7c15) >> shift
			for t[i] != 0 && t[i] != v {
				i = (i + 1) & uint64(size-1)
			}
			if t[i] == 0 {
				t[i], distinct = v, distinct+1
			}
		}
		if distinct >= limit {
			return false
		}
	}
	return true
}

func appendValues(out []byte, records []sealrollup.Record, table *[]uint64) []byte {
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
			cost1 += uvarintLen(zigzag(d))
			cost2 += uvarintLen(zigzag(d - prevDelta))
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
	if len(records) >= wordsMinRun && repeatsValues(records, table) {
		out = append(out, valueWords)
		for _, r := range records {
			out = binary.LittleEndian.AppendUint64(out, r.Sample.Bits)
		}
		return out
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
// It returns what every per-slot reader resolves: of a run of versions, the
// last Raw record or seal-written Mat record (Upto 0) of each physical slot;
// Ghosts, None records, windows a merge wrote down and superseded versions
// are skipped, so slots strictly increase. Kinds are cleared: the view equals
// what a part written without kinds decodes to.
func DecodeSlotsInto(dst []sealrollup.Record, archive uint32, firstSlot int64, data []byte) ([]sealrollup.Record, error) {
	rs, err := decodeRun(dst, archive, firstSlot, data)
	if err != nil {
		return rs, err
	}
	live := rs[:0]
	for _, r := range rs {
		kind := r.Kind
		r.Kind = sealrollup.KindRaw
		switch {
		case kind != sealrollup.KindRaw && (kind != sealrollup.KindMat || r.Upto != 0):
		case len(live) > 0 && live[len(live)-1].Slot == r.Slot:
			live[len(live)-1] = r
		default:
			live = append(live, r)
		}
	}
	return live, nil
}

// PlainRunSpan reads a run's header and slot column only. ok reports a run
// without kinds: single Raw versions in strictly increasing slots (a
// singleton value has no kind either), whose last physical slot is last.
// A run of versions, or one whose header or slots do not parse, is not ok;
// decoding it reports any error.
func PlainRunSpan(firstSlot int64, data []byte) (last int64, ok bool) {
	if !bytes.HasPrefix(data, denseMagic) {
		return firstSlot, len(data) == 26
	}
	if len(data) < 6 {
		return 0, false
	}
	p := data[5:]
	if data[4]&denseExtended != 0 {
		if p[0]&extKinds != 0 {
			return 0, false
		}
		p = p[1:]
	}
	count, n := binary.Uvarint(p)
	if n <= 0 || count == 0 || count > MaxDenseSlots {
		return 0, false
	}
	p = p[n:]
	for ; count > 0; count-- {
		v, n := binary.Uvarint(p)
		if n <= 0 || v > math.MaxInt64 || last > math.MaxInt64-int64(v) {
			return 0, false
		}
		p, last = p[n:], last+int64(v)
	}
	return last, true
}

// DecodeRecordsInto returns every stored record version of a run, kinds and
// Upto included, oldest version of a slot first: what a merge rewrites.
func DecodeRecordsInto(dst []sealrollup.Record, archive uint32, firstSlot int64, data []byte) ([]sealrollup.Record, error) {
	return decodeRun(dst, archive, firstSlot, data)
}

// DecodeVersionsInto returns every stored version of a run, the input of
// sealrollup.Versions (see sealrollup.Record.Version). records is reusable
// scratch.
func DecodeVersionsInto(dst []sealrollup.Version, records []sealrollup.Record, archive uint32, firstSlot int64, data []byte) ([]sealrollup.Version, []sealrollup.Record, error) {
	records, err := decodeRun(records, archive, firstSlot, data)
	if err != nil {
		return dst, records, err
	}
	for _, r := range records {
		dst = append(dst, r.Version())
	}
	return dst, records, nil
}

// decodeRun decodes every record of a run, Upto included (0 for Raw, Ghost
// and runs without kinds).
func decodeRun(dst []sealrollup.Record, archive uint32, firstSlot int64, data []byte) ([]sealrollup.Record, error) {
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
	// Mode bits are independent flags; the value column takes one of
	// untagged planes, untagged integers or a tag.
	mode := data[4]
	if mode&^denseModes != 0 || mode&(denseInteger|denseTagged) == denseInteger|denseTagged || mode&(denseModeOrderDoD|denseModeOrderLinear) == denseModeOrderDoD|denseModeOrderLinear {
		return nil, bad
	}
	aligned, kinds, summary, p := mode&denseAligned != 0, false, false, data[5:]
	if mode&denseExtended != 0 {
		ext := p[0]
		if ext == 0 || ext&^extKnown != 0 || ext&extKinds != 0 && (aligned || ext&extOrderMaximum != 0) || ext&extOrderMaximum != 0 && mode&(denseModeOrderDoD|denseModeOrderLinear) != 0 {
			return nil, bad
		}
		kinds, summary, p = ext&extKinds != 0, ext&extOrderMaximum != 0, p[1:]
	}
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
		if !ok || v > math.MaxInt64 || slot > math.MaxInt64-int64(v) || (i > 0 && v == 0 && !kinds) {
			return nil, bad
		}
		slot += int64(v)
		rs[i].Archive = int(archive)
		rs[i].Slot = slot
	}
	if rs[0].Slot != firstSlot {
		return nil, bad
	}
	if aligned {
		t0, ok0 := take()
		step, ok1 := take()
		if !ok0 || !ok1 || len(rs) < 2 || t0 == 0 || t0 > math.MaxUint32 || step == 0 || step > math.MaxUint32 {
			return nil, bad
		}
		// The last slot bounds every offset; slots strictly increase.
		if uint64(rs[len(rs)-1].Slot-firstSlot) > (math.MaxUint32-t0)/step {
			return nil, bad
		}
		for i := range rs {
			rs[i].Sample.Time = int64(t0) + (rs[i].Slot-firstSlot)*int64(step)
		}
	} else {
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
	}
	var order uint64
	var pd int64
	if summary {
		order, ok = take()
		if !ok || order == 0 {
			return nil, bad
		}
	} else if mode&denseModeOrderLinear != 0 {
		first, ok0 := take()
		u, ok1 := take()
		if !ok0 || !ok1 || first == 0 || len(rs) < 2 || len(p) == 0 {
			return nil, bad
		}
		step := int64(u>>1) ^ -int64(u&1)
		width := int(p[0])
		p = p[1:]
		if width > 8 || len(p) < width*(len(rs)-1) {
			return nil, bad
		}
		for plane := 0; plane < width; plane++ {
			for i := 1; i < len(rs); i++ {
				rs[i].Order |= uint64(p[plane*(len(rs)-1)+i-1]) << uint(plane*8)
			}
		}
		p = p[width*(len(rs)-1):]
		for i := range rs {
			u := rs[i].Order
			rs[i].Order = first + uint64(step)*uint64(i) + uint64(int64(u>>1)^-int64(u&1))
			if rs[i].Order == 0 {
				return nil, bad
			}
			rs[i].Sample.Sequence = rs[i].Order
		}
	} else {
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
	if summary {
		// Fill the compatibility clocks with flags in the same record pass.
		for i := range rs {
			rs[i].Order, rs[i].Sample.Sequence = order, order
			rs[i].Correction = flags[i/8]&(1<<uint(i%8)) != 0
		}
	} else {
		for i := range rs {
			rs[i].Correction = flags[i/8]&(1<<uint(i%8)) != 0
		}
	}
	if kinds {
		nkinds := (len(rs) + 3) / 4
		if len(p) < nkinds {
			return nil, bad
		}
		if n := uint(len(rs) % 4); n != 0 && p[nkinds-1]>>(2*n) != 0 {
			return nil, bad
		}
		canonical := false // the writer sets the bit only for versions
		for i := range rs {
			rs[i].Kind = sealrollup.Kind(p[i/4] >> uint(2*(i%4)) & 3)
			canonical = canonical || rs[i].Kind != sealrollup.KindRaw || i > 0 && rs[i].Slot == rs[i-1].Slot
		}
		if !canonical {
			return nil, bad
		}
		p = p[nkinds:]
		for i := range rs {
			if rs[i].Kind != sealrollup.KindMat && rs[i].Kind != sealrollup.KindNone {
				continue
			}
			v, ok := take()
			if !ok {
				return nil, bad
			}
			rs[i].Upto = v
		}
	}
	tag := byte(valuePlanes)
	legacyInteger := mode&denseInteger != 0
	switch {
	case legacyInteger: // first differences, plain varints, e = 0
		tag = valueDelta1
	case mode&denseTagged != 0:
		if len(p) == 0 {
			return nil, bad
		}
		tag, p = p[0], p[1:]
	}
	var err error
	if p, err = decodeValues(rs, tag, legacyInteger, p); err != nil {
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
	err         error
	closed      bool
	orders      OrderEncoding
	legacyKinds bool
}

// PredictOrders replaces the seal order codec with endpoint-predicted byte planes,
// including copied runs. Decoding is exact. Call before
// the first Set.
func (w *DenseWriter) PredictOrders() { w.orders = PredictedOrders }

// OrderEncoding selects the exact seal, exact merge, or certified summary codec.
type OrderEncoding uint8

const (
	ExactOrders OrderEncoding = iota
	PredictedOrders
	SummaryOrders
)

// SetOrderEncoding flushes the preceding name before changing its order codec.
// SummaryOrders requires the owner certificate described in ROLLUPS.md.
func (w *DenseWriter) SetOrderEncoding(orders OrderEncoding) error {
	if w.orders != orders {
		if err := w.flush(); err != nil {
			return err
		}
		w.orders = orders
	}
	return nil
}

// HasOrderSummary queries the run header; decoding still validates its syntax.
func HasOrderSummary(data []byte) bool {
	return len(data) >= 6 && bytes.HasPrefix(data, denseMagic) && data[4]&denseExtended != 0 && data[5]&extOrderMaximum != 0
}

// LimitBytes bounds compressed staging output, including SST indexes/footer.
// Call before the first Set. Exceeding it aborts this proposal, never inputs.
func (w *DenseWriter) LimitBytes(n uint64) { w.writer.out.limit = n }

// SealCompression writes this part's blocks at the fast seal level. Use it
// only for parts a background merge rewrites; call before the first Set.
func (w *DenseWriter) SealCompression() { w.writer.sst.(*denseSSTWriter).level = denseSealLevel }

// IDKeyed makes this an ID-keyed part (GCI2 keys, descriptor schema 3) whose
// IDs resolve at name dictionary generation (nonzero). Input then takes ID
// keys or SetPlannedID* tuples. Requires IDKeys; call before the first Set.
func (w *DenseWriter) IDKeyed(generation uint64) error {
	if generation == 0 || !IDKeys.Load() || w.writer.records > 0 || w.havePrev || len(w.last) > 0 {
		return errors.New("ID-keyed parts are switched off, or the writer has input")
	}
	w.writer.generation = generation
	return nil
}

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
	slotKey    []byte // AppendVersions' per-record key
	// Planned input: the previous tuple, its planner rank and the scope's
	// group key prefix.
	prev     Key
	prevRank uint32
	havePrev bool
	group    []byte
	// distinct is the value codec's reusable distinct-value table.
	distinct []uint64
}

var denseWorkspaces sync.Pool

func (w *DenseWriter) releaseWorkspace() {
	b := w.denseWorkspace
	if b == nil {
		return
	}
	b.decoder.key, b.decoder.name, b.key = Key{}, "", Key{}
	b.decoder.prefix, b.decoder.tail = b.decoder.prefix[:0], 0
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
	return &DenseWriter{writer: w, denseWorkspace: b, legacyKinds: LegacyKinds.Load()}, nil
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
		b, e = appendSlotsOrder(b, w.records, &w.distinct, w.orders)
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
	if w.fold && len(value) > 0 && (len(w.records) > 0 || w.rawPending) && SameMetric(w.key, k) {
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
	legacy := w.legacyKinds && len(value) > 4 && value[4]&denseExtended != 0
	if err == nil && legacy {
		// LegacyKinds re-encodes the run's per-slot view: winners, no kinds.
		w.records, err = DecodeSlotsInto(w.records[:0], k.Archive, k.BlockStart, value)
	} else if err == nil {
		w.records, err = DecodeRecordsInto(w.records[:0], k.Archive, k.BlockStart, value)
	}
	if err == nil {
		// Future inputs must start after this run's last physical slot.
		if !setKeySlot(w.last, w.records[len(w.records)-1].Slot) {
			err = errors.New("dense run slot does not fit its key")
		}
	}
	if err == nil {
		// A re-encode for predicted orders keeps every version and kind.
		if legacy || w.orders == SummaryOrders || w.orders == PredictedOrders && value[4]&denseModeOrderLinear == 0 && len(w.records) > 1 {
			value, err = appendSlotsOrder(w.encoded[:0], w.records, &w.distinct, w.orders)
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

// AppendVersions appends a decoded run of record versions of one archive:
// slots never decrease (a slot's versions oldest first) and key is the first
// record's key. Like AppendSlots it joins the pending run of its metric and
// archive and re-encodes in runs of up to MaxDenseSlots, and a full run never
// splits one slot's versions. Every record is validated as SetRecordEncoded
// validates it; caller buffers are not retained.
func (w *DenseWriter) AppendVersions(key []byte, records []sealrollup.Record) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	k, err := w.decodeInput(key)
	if err == nil && (len(records) == 0 || k.Archive == MetadataArchive || k.Ordinal != 0 || records[0].Archive < 0 || uint64(records[0].Archive) != uint64(k.Archive) || records[0].Slot != k.BlockStart) {
		err = errors.New("dense versions do not match valid archive key")
	}
	w.slotKey = append(w.slotKey[:0], key...)
	for i := 0; err == nil && i < len(records); i++ {
		r := records[i]
		if r.Archive != records[0].Archive || !validSlotRecord(r) || i > 0 && r.Slot < records[i-1].Slot {
			err = errors.New("invalid dense version record")
			break
		}
		k.BlockStart = r.Slot
		if !setKeySlot(w.slotKey, r.Slot) {
			err = errors.New("dense version slot does not fit its key")
			break
		}
		err = w.appendRecord(w.slotKey, k, r)
	}
	if err == nil && !setKeySlot(w.last, records[len(records)-1].Slot) {
		// Future inputs must start after this run's last physical slot.
		err = errors.New("dense run slot does not fit its key")
	}
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
	t := keyTail(w.writer.generation != 0)
	if len(records) == MaxDenseSlots && (len(w.records) == 0 || len(w.records) == MaxDenseSlots || w.records[0].Archive != records[0].Archive || !bytes.Equal(w.first[:len(w.first)-t], key[:max(len(key)-t, 0)])) {
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
		if len(w.records) > 0 && (!SameMetric(k, w.key) || k.Archive != w.key.Archive || len(w.records) == MaxDenseSlots) {
			if err = w.flush(); err != nil {
				break
			}
		}
		if len(w.records) == 0 {
			k.BlockStart = r.Slot
			w.key = k
			w.first = append(w.first[:0], key...)
			if !setKeySlot(w.first, r.Slot) {
				err = errors.New("dense run slot does not fit its key")
				break
			}
		}
		w.records = append(w.records, r)
	}
	if err == nil {
		// Future inputs must start after this run's last physical slot.
		if !setKeySlot(w.last, records[len(records)-1].Slot) {
			err = errors.New("dense run slot does not fit its key")
		}
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
	if w.legacyKinds {
		if r.Kind != sealrollup.KindRaw && (r.Kind != sealrollup.KindMat || r.Upto != 0) {
			return nil
		}
		r.Kind = sealrollup.KindRaw
		if n := len(w.records); n > 0 && !w.rawPending && SameMetric(k, w.key) && k.Archive == w.key.Archive && w.records[n-1].Slot == r.Slot {
			w.records[n-1] = r
			return nil
		}
	}
	if w.rawPending {
		if e := w.flush(); e != nil {
			return e
		}
	}
	var carry []sealrollup.Record
	if len(w.records) > 0 && (!SameMetric(k, w.key) || k.Archive != w.key.Archive || len(w.records) == MaxDenseSlots) {
		// A full run never splits one slot's versions: they start the next run.
		if g := len(w.records); g == MaxDenseSlots && SameMetric(k, w.key) && k.Archive == w.key.Archive && w.records[g-1].Slot == r.Slot {
			for g > 0 && w.records[g-1].Slot == r.Slot {
				g--
			}
			if g == 0 {
				return errors.New("slot versions exceed a dense run")
			}
			carry = slices.Clone(w.records[g:])
			w.records = w.records[:g]
		}
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
	w.records = append(append(w.records, carry...), r)
	return nil
}

// SetPlannedRecord packs a planner record of metric, routed to vshard, without
// an encoded input key. Group, partition and part come from the writer scope,
// and key bytes are formatted only where a dense run starts. The caller must
// supply the route already derived by the planner. Order, scope, archive and
// scalar bounds are validated as SetRecordEncoded validates them.
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
// The caller supplies a planner-validated route, as for SetPlannedRecord.
// The caller's value is not retained.
func (w *DenseWriter) SetPlannedMetadata(vshard uint16, rank uint32, metric ID, value []byte) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	k, err := w.plannedKey(vshard, rank, metric, MetadataArchive, 0)
	return w.setPlannedMetadata(k, err, value)
}

func (w *DenseWriter) setPlannedMetadata(k Key, err error, value []byte) error {
	if err == nil && w.fold && len(value) > 0 && len(w.records) > 0 && SameMetric(w.key, k) {
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

// SetPlannedIDRecord is SetPlannedRecord for an ID-keyed writer (IDKeyed):
// the metric is its vshard-local dictionary ID, and tuples sort by
// (vshard, ID, archive, slot).
func (w *DenseWriter) SetPlannedIDRecord(vshard uint16, id uint32, r *sealrollup.Record) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	var k Key
	var err error
	if r.Archive >= 0 && int64(r.Archive) < int64(MetadataArchive) && validSlotRecord(*r) {
		k, err = w.plannedIDKey(vshard, id, uint32(r.Archive), r.Slot)
	} else {
		err = errors.New("dense scalar record does not match valid archive key")
	}
	if err == nil {
		err = w.appendRecord(nil, k, *r)
	}
	w.err = err
	return err
}

// SetPlannedIDMetadata is SetPlannedMetadata for an ID-keyed writer.
func (w *DenseWriter) SetPlannedIDMetadata(vshard uint16, id uint32, value []byte) error {
	if w.closed {
		return io.ErrClosedPipe
	}
	if w.err != nil {
		return w.err
	}
	k, err := w.plannedIDKey(vshard, id, MetadataArchive, 0)
	return w.setPlannedMetadata(k, err, value)
}

// plannedIDKey is plannedKey for ID keys: tuples sort by vshard, ID, archive
// and slot, and must fit GCI2 (archive below 255, slot below 2^32).
func (w *DenseWriter) plannedIDKey(vshard uint16, id uint32, archive uint32, slot int64) (Key, error) {
	s := w.writer.spec.Scope
	if len(w.last) > 0 || w.writer.generation == 0 {
		return Key{}, errors.New("planned ID input needs an ID-keyed writer without encoded input")
	}
	if _, err := partsformat.AppendIDTail(nil, archive, slot, 0); err != nil {
		return Key{}, err
	}
	p := &w.prev
	if w.havePrev {
		c := cmp.Compare(vshard, p.VShard)
		if c == 0 {
			c = cmp.Compare(id, p.Local)
		}
		if c == 0 {
			if c = cmp.Compare(archive, p.Archive); c == 0 {
				c = cmp.Compare(slot, p.BlockStart)
			}
			if c < 0 || c == 0 && archive == MetadataArchive {
				return Key{}, errors.New("unordered planned dense input")
			}
			p.Archive, p.BlockStart = archive, slot
			return *p, nil
		}
		if c < 0 {
			return Key{}, errors.New("unordered planned dense input")
		}
	}
	if vshard >= VirtualShards || (s.VShard != GroupVShard && s.VShard != vshard) {
		return Key{}, errors.New("invalid or foreign planned dense metric")
	}
	if len(w.group) == 0 {
		group, _, err := partsformat.IDGroupRange(s.Group)
		if err != nil {
			return Key{}, err
		}
		w.group = append(w.group[:0], group...)
	}
	*p = Key{Group: s.Group, VShard: vshard, Partition: s.Partition, Part: s.Part, ByID: true, Local: id, Archive: archive, BlockStart: slot}
	w.havePrev = true
	return *p, nil
}

// plannedKey checks that a tuple sorts strictly after the previous one in GCP3
// key order (vshard, name length, name, archive, slot), or repeats its archive
// slot (the next version of that slot), and validates each new name's length
// and scope once. Routing comes from the planner, without hashing
// the name again. Two ranked tuples compare by rank, which
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
			if c < 0 || c == 0 && archive == MetadataArchive {
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
	if len(metric) == 0 || len(metric) > MaxNameBytes || vshard >= VirtualShards || (s.VShard != GroupVShard && s.VShard != vshard) {
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
// An ID key's archive and slot were checked by plannedIDKey.
func (w *DenseWriter) appendKey(dst []byte, k Key) []byte {
	dst = append(dst, w.group...)
	dst = binary.BigEndian.AppendUint16(dst, k.VShard)
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Partition)^(1<<63))
	dst = append(dst, k.Part[:]...)
	if k.ByID {
		dst = binary.BigEndian.AppendUint32(dst, k.Local)
		dst, _ = partsformat.AppendIDTail(dst, k.Archive, k.BlockStart, 0)
		return dst
	}
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
	if kind == valueWords {
		if e != 0 || len(p) < 8*len(rs) {
			return nil, bad
		}
		for i := range rs {
			rs[i].Sample.Bits = binary.LittleEndian.Uint64(p[8*i:])
		}
		return p[8*len(rs):], nil
	}
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
