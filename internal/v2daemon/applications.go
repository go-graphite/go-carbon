package v2daemon

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"math"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

// Application is one original per-metric UpdateMany call. Order increases in
// the producer stream; Clock is admission time, never reconstructed event time.
// Policy includes the immutable schema/aggregation and portable identity.
// Sequence preserves source point order even across equal timestamps.
type Application struct {
	Name     string
	Policy   []byte
	Order    uint64
	Clock    int64
	Points   []points.Point
	Sequence []uint64
}

// AppendApplications retains explicit original applications in the existing
// pending journal group. Drain syncs definitions before the journal, then
// pipelines the same detached groups to core. No additional sender queue exists.
func (p *Producer) AppendApplications(apps []Application) error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer < 2 || len(apps) == 0 || len(apps) > 32768 {
		return p.fail(errors.New("invalid application packet"))
	}
	n, order := 0, p.applyOrder
	for _, a := range apps {
		if a.Name == "" || a.Order <= order || len(a.Points) == 0 || len(a.Points) != len(a.Sequence) || len(a.Points) > 32768 || n > 32768-len(a.Points) {
			return p.fail(errors.New("invalid original application"))
		}

		order = a.Order
		n += len(a.Points)
	}
	if p.cursor > math.MaxUint64-uint64(n) {
		return p.fail(errors.New("application cursor exhausted"))
	}
	frames := make([]partlog.DictionaryFrame, len(apps))
	ids := make([]uint64, len(apps))
	batches := make([]v2core.Batch, len(apps))
	var rows []v2core.Row
	if last := len(p.spareRows); last > 0 {
		rows = p.spareRows[last-1]
		p.spareRows[last-1] = nil
		p.spareRows = p.spareRows[:last-1]
	}
	if cap(rows) < n {
		rows = make([]v2core.Row, 0, n)
	} else {
		rows = rows[:0]
	}
	for i, a := range apps {
		previousCount := p.catalog.Count()
		id, err := p.catalog.Intern(a.Name, a.Policy)
		if err != nil {
			return p.fail(err)
		}
		if p.layer == 3 && id > previousCount {
			var policy v2core.RollupPolicy
			if err := json.Unmarshal(a.Policy, &policy); err != nil || string(policy.Name) != a.Name {
				return p.fail(errors.New("application identity/policy mismatch"))
			}
			canonical, err := v2core.EncodeRollupPolicy(policy)
			if err != nil || !bytes.Equal(canonical, a.Policy) {
				return p.fail(errors.New("invalid application policy"))
			}
		}
		ids[i] = id
		frames[i] = partlog.DictionaryFrame{Key: a.Name, Definition: a.Policy, Clock: a.Clock, Points: a.Points}
		start := len(rows)
		for j, point := range a.Points {
			rows = append(rows, v2core.Row{ID: NumericID(id, p.index, p.count), Time: point.Timestamp, Bits: math.Float64bits(point.Value), Seq: a.Sequence[j]})
		}
		batches[i] = v2core.Batch{ID: NumericID(id, p.index, p.count), Order: a.Order, Clock: a.Clock, Policy: bytes.Clone(a.Policy), Points: rows[start:len(rows):len(rows)]}
	}
	start := time.Now()
	encoded, err := p.encoder.EncodeIDs(frames, ids)
	if err != nil {
		return p.fail(err)
	}
	cursor := p.cursor + uint64(n)
	record := encodeApplications(cursor, ids, batches, encoded)
	p.EncodeTime += time.Since(start)
	p.records = append(p.records, record)
	p.packets = append(p.packets, packet{rows: rows, batches: batches, cursor: cursor})
	p.cursor = cursor
	p.applyOrder = order
	p.pendingRows += n
	if p.pendingRows >= p.maxPending {
		p.ForcedGroups++
		return p.Drain()
	}
	if !time.Now().Before(p.nextSync) {
		return p.Drain()
	}
	return nil
}

var applicationMagic = []byte{'V', '2', 'A', 1}
var applicationCRC = crc32.MakeTable(crc32.Castagnoli)

// The existing PDC4 point codec is nested unchanged. The outer checksum binds
// source IDs, application orders and sequences to those original PDC4 frames.
func encodeApplications(cursor uint64, ids []uint64, batches []v2core.Batch, pdc4 []byte) []byte {
	b := append([]byte(nil), applicationMagic...)
	b = binary.AppendUvarint(b, cursor)
	b = binary.AppendUvarint(b, uint64(len(batches)))
	for i, a := range batches {
		b = binary.AppendUvarint(b, ids[i])
		b = binary.AppendUvarint(b, a.Order)
		b = binary.AppendUvarint(b, uint64(len(a.Points)))
		for _, p := range a.Points {
			b = binary.AppendUvarint(b, p.Seq)
		}
	}
	b = binary.AppendUvarint(b, uint64(len(pdc4)))
	b = append(b, pdc4...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, applicationCRC))
}

// DecodeApplications verifies a journal application packet against the synced
// catalog and returns detached original groups. It does not mutate core state.
// The decoder retains PDC4 segment predictor state and must be reused in order.
func DecodeApplications(record []byte, decoder *partlog.CatalogDecoder, catalog *partlog.NameCatalog, producer, producers int) ([]v2core.Batch, uint64, error) {
	invalid := errors.New("invalid original application envelope")
	if producer < 0 || producers < 1 || producer >= producers || len(record) < 8 || !bytes.Equal(record[:4], applicationMagic) || crc32.Checksum(record[:len(record)-4], applicationCRC) != binary.LittleEndian.Uint32(record[len(record)-4:]) {
		return nil, 0, invalid
	}
	b := record[4 : len(record)-4]
	read := func() (uint64, bool) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, false
		}
		b = b[n:]
		return v, true
	}
	cursor, ok := read()
	if !ok || cursor == 0 {
		return nil, 0, invalid
	}
	count, ok := read()
	if !ok || count == 0 || count > 32768 {
		return nil, 0, invalid
	}
	out := make([]v2core.Batch, int(count))
	ids := make([]uint64, int(count))
	total := uint64(0)
	order := uint64(0)
	for i := range out {
		id, ok := read()
		if !ok || id == 0 || id-1 > (math.MaxUint64-uint64(producer)-1)/uint64(producers) {
			return nil, 0, invalid
		}
		o, ok := read()
		if !ok || o <= order {
			return nil, 0, invalid
		}
		order = o
		n, ok := read()
		if !ok || n == 0 || n > 32768-total {
			return nil, 0, invalid
		}
		total += n
		ids[i] = id
		out[i] = v2core.Batch{ID: NumericID(id, producer, producers), Order: o, Points: make([]v2core.Row, int(n))}
		for j := range out[i].Points {
			seq, ok := read()
			if !ok {
				return nil, 0, invalid
			}
			out[i].Points[j].Seq = seq
		}
	}
	size, ok := read()
	if !ok || size != uint64(len(b)) {
		return nil, 0, invalid
	}
	frames, err := decoder.Decode(b)
	if err != nil {
		return nil, 0, err
	}
	if len(frames) != len(out) {
		return nil, 0, invalid
	}
	for i, f := range frames {
		name, policy, ok := catalog.Lookup(ids[i])
		if !ok || name != f.Key || !bytes.Equal(policy, f.Definition) || len(f.Points) != len(out[i].Points) {
			return nil, 0, invalid
		}
		out[i].Clock = f.Clock
		out[i].Policy = bytes.Clone(policy)
		for j, p := range f.Points {
			out[i].Points[j].ID = out[i].ID
			out[i].Points[j].Time = p.Timestamp
			out[i].Points[j].Bits = math.Float64bits(p.Value)
		}
	}
	if total > cursor {
		return nil, 0, invalid
	}
	return out, cursor, nil
}
