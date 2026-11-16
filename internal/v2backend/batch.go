package v2backend

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/points"
)

type resolvedMetric struct {
	name   string
	owner  int
	group  partsformat.Group
	policy daemonstore.Metric
	prefix []byte
}

// The first lookup borrows name bytes for hashing. Only a catalogue miss
// allocates a stable canonical string; receiver buffers can be reused at return.
func (s *Store) resolveBytes(name []byte) (*resolvedMetric, error) {
	// Load never retains its key. The map owns a different, copied string from
	// resolve on misses; this borrowed view dies before receiver memory is reused.
	key := unsafe.String(unsafe.SliceData(name), len(name))
	if x, ok := s.resolved.Load(key); ok {
		return x.(*resolvedMetric), nil
	}
	return s.resolve(string(name))
}
func (s *Store) resolve(name string) (*resolvedMetric, error) {
	if x, ok := s.resolved.Load(name); ok {
		return x.(*resolvedMetric), nil
	}
	p, e := s.opts.Policy(name)
	if e != nil {
		return nil, e
	}
	if _, e = rollupConfig(p); e != nil {
		return nil, e
	}
	if legacy := s.legacyReady.Load(); legacy != nil {
		if old, ok := legacy.Lookup(name); ok {
			if !reflect.DeepEqual(toDaemonMetric(old), p) {
				return nil, errors.New("legacy policy differs from live policy")
			}
			if len(p.Archives) > 1 {
				return nil, errors.New("legacy multiarchive writes require an exact physical-slot rollup seed")
			}
		}
	}
	g, e := partsformat.AssignGroup(partsformat.ID(name), nil)
	if e != nil {
		return nil, e
	}
	b, e := json.Marshal(p)
	if e != nil {
		return nil, e
	}
	prefix := []byte{'V', '2', 'B', 1}
	prefix = binary.BigEndian.AppendUint32(prefix, uint32(len(b)))
	prefix = binary.BigEndian.AppendUint32(prefix, uint32(len(g)))
	prefix = append(prefix, b...)
	prefix = append(prefix, []byte(g)...)
	r := &resolvedMetric{name: name, owner: s.route(name), group: g, policy: p, prefix: prefix}
	actual, _ := s.resolved.LoadOrStore(name, r)
	return actual.(*resolvedMetric), nil
}
func (s *Store) makeFrame(name string, data []points.Point, clock int64) (frame, error) {
	r, e := s.resolve(name)
	if e != nil {
		return frame{}, e
	}
	for _, p := range data {
		if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 {
			return frame{}, errors.New("point timestamp outside storage range")
		}
	}
	return frame{name: r.name, resolved: r, group: r.group, policy: r.policy, clock: clock, points: slices.Clone(data)}, nil
}

// AppendBatch borrows receiver buffers only until return. Every input point is
// still one original apply boundary; batching journal I/O never coalesces applies.
func (s *Store) AppendBatch(names [][]byte, data []points.Point) error {
	if e := s.status(); e != nil {
		return e
	}
	if len(names) != len(data) {
		return errors.New("batch lengths differ")
	}
	if len(data) == 0 {
		return nil
	}
	owned := slices.Clone(data)
	groups := make([][]frame, len(s.owners))
	frames := make([]frame, len(data))
	counts := make([]int, len(s.owners))
	resolved := make([]*resolvedMetric, len(owned))
	for i := range owned {
		r, e := s.resolveBytes(names[i])
		if e != nil {
			return e
		}
		resolved[i] = r
		counts[r.owner]++
	}
	offset := 0
	for i, n := range counts {
		groups[i] = frames[offset : offset : offset+n]
		offset += n
	}
	clock := s.now().Unix()
	for i, p := range owned {
		if p.Timestamp <= 0 || p.Timestamp > math.MaxUint32 {
			return errors.New("point timestamp outside storage range")
		}
		r := resolved[i]
		groups[r.owner] = append(groups[r.owner], frame{name: r.name, resolved: r, group: r.group, policy: r.policy, clock: clock, points: owned[i : i+1]})
	}
	ids := make([]int, 0, len(groups))
	batches := make([][]frame, 0, len(groups))
	for i, g := range groups {
		if len(g) > 0 {
			ids = append(ids, i)
			batches = append(batches, g)
		}
	}
	return s.admit(batches, ids)
}
func (s *Store) admit(batches [][]frame, ids []int) error {
	if e := s.status(); e != nil {
		return e
	}
	for _, fs := range batches {
		if encodedFramesSize(fs) > 8<<20 {
			return errors.New("receipt batch too large")
		}
	}
	acks := make([]chan answer, len(ids))
	sent := 0
	for i, id := range ids {
		o := s.owners[id]
		acks[i] = make(chan answer, 1)
		charge := frameCharge(batches[i])
		if charge > 256<<20 {
			return errors.New("receipt exceeds owner head budget")
		}
		if e := o.admission.Acquire(s.admissionContext, charge); e != nil {
			return e
		}
		select {
		case o.requests <- command{batch: batches[i], answer: acks[i]}:
			sent++
		case <-o.done:
			o.admission.Release(charge)
			return ErrClosed
		}
	}
	var e error
	for i := 0; i < sent; i++ {
		select {
		case a := <-acks[i]:
			e = errors.Join(e, a.err)
		case <-s.owners[ids[i]].done:
			e = errors.Join(e, ErrClosed)
		}
	}
	return e
}
func encodeFrames(fs []frame) ([]byte, error) {
	if len(fs) == 0 {
		return nil, errors.New("empty receipt batch")
	}
	size := 8
	for _, f := range fs {
		if f.resolved == nil {
			return nil, errors.New("unresolved receipt")
		}
		size += 12 + len(f.resolved.prefix) + 16 + len(f.name) + 16*len(f.points)
	}
	if size > 8<<20 {
		return nil, errors.New("receipt batch too large")
	}
	out := make([]byte, 0, size)
	out = append(out, 'V', '2', 'M', 1)
	out = binary.BigEndian.AppendUint32(out, uint32(len(fs)))
	for i, f := range fs {
		n := len(f.resolved.prefix) + 16 + len(f.name) + 16*len(f.points)
		out = binary.BigEndian.AppendUint64(out, uint64(f.clock))
		out = binary.BigEndian.AppendUint32(out, uint32(n))
		start := len(out)
		out = append(out, f.resolved.prefix...)
		out = binary.LittleEndian.AppendUint64(out, 1)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(f.name)))
		out = binary.LittleEndian.AppendUint32(out, uint32(len(f.points)))
		out = append(out, f.name...)
		for _, p := range f.points {
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(p.Value))
			out = binary.LittleEndian.AppendUint64(out, uint64(p.Timestamp))
		}
		fs[i].payload = out[start:len(out):len(out)]
	}
	return out, nil
}
func decodeFrames(b []byte) ([]frame, error) {
	if len(b) >= 4 && bytes.Equal(b[:4], []byte{'V', '2', 'B', 1}) {
		f, e := decodeFrame(b)
		return []frame{f}, e
	}
	if len(b) < 8 || !bytes.Equal(b[:4], []byte{'V', '2', 'M', 1}) {
		return nil, errors.New("invalid multi receipt envelope")
	}
	n := int(binary.BigEndian.Uint32(b[4:8]))
	if n == 0 || n > (len(b)-8)/12 {
		return nil, errors.New("invalid receipt count")
	}
	b = b[8:]
	fs := make([]frame, 0, n)
	for range n {
		if len(b) < 12 {
			return nil, errors.New("truncated receipt")
		}
		clock := int64(binary.BigEndian.Uint64(b))
		size := int(binary.BigEndian.Uint32(b[8:]))
		b = b[12:]
		if size > len(b) {
			return nil, errors.New("truncated receipt payload")
		}
		f, e := decodeFrame(b[:size])
		if e != nil {
			return nil, e
		}
		f.clock = clock
		fs = append(fs, f)
		b = b[size:]
	}
	if len(b) != 0 {
		return nil, errors.New("trailing receipt bytes")
	}
	return fs, nil
}

// Includes active/frozen frame structs, copied points and encoded receipts.
// Credits survive admission and are released only after the seal publishes.
func frameCharge(fs []frame) int64 {
	var n int64
	for _, f := range fs {
		n += int64(512 + len(f.name) + len(f.points)*32 + len(f.resolved.prefix))
	}
	return n
}

func encodedFramesSize(fs []frame) int {
	n := 8
	for _, f := range fs {
		n += 12 + len(f.resolved.prefix) + 16 + len(f.name) + 16*len(f.points)
	}
	return n
}

func (o *owner) encodeReceipts(fs []frame) ([]byte, error) {
	o.receiptScratch = slices.Grow(o.receiptScratch[:0], len(fs))[:len(fs)]
	entries := o.receiptScratch
	defer clear(entries)
	for i, f := range fs {
		if f.resolved == nil {
			return nil, errors.New("unresolved receipt")
		}
		entries[i] = partlog.DictionaryFrame{Key: f.name, Definition: f.resolved.prefix, Clock: f.clock, Points: f.points}
	}
	return o.journalEncoder.Encode(entries)
}

type receiptDecoder struct {
	catalog    partlog.CatalogDecoder
	dictionary partlog.DictionaryDeltaDecoder
	predictor  partlog.PredictorV3Decoder
	policies   map[string]*resolvedMetric
}

func (d *receiptDecoder) decode(b []byte) ([]frame, error) {
	if !partlog.IsDictionaryDeltaRecord(b) && !partlog.IsPredictorV3Record(b) && !partlog.IsCatalogRecord(b) {
		return decodeFrames(b)
	}
	var entries []partlog.DictionaryFrame
	var e error
	if partlog.IsCatalogRecord(b) {
		entries, e = d.catalog.Decode(b)
	} else if partlog.IsPredictorV3Record(b) {
		entries, e = d.predictor.Decode(b)
	} else {
		entries, e = d.dictionary.Decode(b)
	}
	if e != nil {
		return nil, e
	}
	if d.policies == nil {
		d.policies = make(map[string]*resolvedMetric)
	}
	fs := make([]frame, len(entries))
	for i, f := range entries {
		r := d.policies[f.Key]
		if r == nil || !bytes.Equal(r.prefix, f.Definition) {
			p := f.Definition
			if len(p) < 12 || !bytes.Equal(p[:4], []byte{'V', '2', 'B', 1}) {
				return nil, errors.New("invalid dictionary policy")
			}
			n, g := uint64(binary.BigEndian.Uint32(p[4:8])), uint64(binary.BigEndian.Uint32(p[8:12]))
			if n+g != uint64(len(p)-12) {
				return nil, errors.New("invalid dictionary policy lengths")
			}
			r = &resolvedMetric{name: f.Key, group: partsformat.Group(string(p[12+n:])), prefix: p}
			if e = json.Unmarshal(p[12:12+n], &r.policy); e != nil {
				return nil, e
			}
			if _, e = rollupConfig(r.policy); e != nil {
				return nil, e
			}
			if r.group == "" || len(r.group) > partsformat.MaxNameBytes {
				return nil, errors.New("invalid dictionary group")
			}
			d.policies[f.Key] = r
		}
		fs[i] = frame{name: f.Key, resolved: r, group: r.group, policy: r.policy, clock: f.Clock, points: f.Points}
	}
	return fs, nil
}
