package v2daemon

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/bits"
	"slices"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/internal/v2replay"
	"github.com/go-graphite/go-carbon/points"
)

// Application is one original per-metric UpdateMany call. Order increases in
// the producer stream; Clock is admission time, never reconstructed event time.
// Policy includes the immutable schema/aggregation and portable identity.
// Sequence preserves source point order even across equal timestamps.
type Application struct {
	// Registered replaces Name and Policy after RegisterApplication validates them.
	Registered *RegisteredApplication
	Name       string
	Policy     []byte
	Order      uint64
	Clock      int64
	Points     []points.Point
	Sequence   []uint64
}

// RegisteredApplication is an immutable producer-local catalog identity. It
// cannot cross a producer, restart, or catalog epoch. Callers retain the handle
// instead of resubmitting names and definitions for each original application.
// It holds no name: the catalog (the shared identity on layer 3) owns the one
// copy, and the cold paths that need the string resolve it by ID (24 B/name).
type RegisteredApplication struct {
	producer *Producer
	id       uint64
	policy   *v2core.ApplicationPolicy
}

// name resolves the registered name from its single owner. Callers are in the
// producer's serialized append domain, like the catalog reads in
// appendApplicationsStamped.
func (r *RegisteredApplication) name() string {
	if r.policy != nil {
		return r.policy.Name()
	}
	name, _ := r.producer.catalog.Key(r.id)
	return name
}

// RegisterApplication validates canonical identity and collision/definition
// equality at admission. It retains no caller-owned bytes.
func (p *Producer) RegisterApplication(name string, policy []byte) (*RegisteredApplication, error) {
	if p.failed != nil {
		return nil, p.failed
	}
	if p.layer < 2 {
		return nil, errors.New("application registration requires a journal")
	}
	if err := p.openCatalog(); err != nil {
		return nil, err
	}
	id, err := p.catalog.Intern(name, policy)
	if err != nil {
		return nil, err
	}
	if id < uint64(len(p.applications)) && p.applications[id] != nil {
		return p.applications[id], nil
	}
	name, _ = p.catalog.Key(id) // the catalog's own string, not the caller's
	var registered *v2core.ApplicationPolicy
	// Classic mirroring consumes canonical bytes before core submission.
	if p.layer == 3 && p.durableApplications == nil {
		registered, err = p.engine.RegisterApplicationPolicy(NumericID(id, p.index, p.count), name, policy)
		if err != nil {
			return nil, err
		}
	}
	for uint64(len(p.applications)) <= id {
		p.applications = append(p.applications, nil) // catalog IDs are dense
	}
	r := &RegisteredApplication{producer: p, id: id, policy: registered}
	p.applications[id] = r
	return r, nil
}

// AppendApplications retains explicit original applications in the existing
// pending journal group. Drain syncs definitions before the journal, then
// pipelines the same detached groups to core. No additional sender queue exists.
func (p *Producer) appendApplications(apps []Application) error {
	return p.appendApplicationsStamped(apps, nil)
}

func (p *Producer) appendApplicationsStamped(apps []Application, stamp *v2placement.Stamp) error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer < 2 || len(apps) == 0 || len(apps) > 32768 {
		return p.fail(errors.New("invalid application packet"))
	}
	n, order := 0, p.applyOrder
	for _, a := range apps {
		if (a.Registered == nil && a.Name == "") || (a.Registered != nil && (a.Registered.producer != p || a.Name != "" || len(a.Policy) != 0)) || a.Order <= order || len(a.Points) == 0 || len(a.Points) != len(a.Sequence) || len(a.Points) > 32768 || n > 32768-len(a.Points) {
			return p.fail(errors.New("invalid original application"))
		}

		order = a.Order
		n += len(a.Points)
	}
	if p.cursor > math.MaxUint64-uint64(n) {
		return p.fail(errors.New("application cursor exhausted"))
	}
	p.applicationFrames = slices.Grow(p.applicationFrames[:0], len(apps))[:len(apps)]
	frames := p.applicationFrames
	defer func() {
		for i := range frames {
			frames[i].Points = nil // borrowed from the caller
		}
	}()
	// Direct L3 producers encode straight into the owners' head rows. Classic
	// mirroring and the non-rollup layer submit detached batches instead.
	direct := p.layer == 3 && p.durableApplications == nil
	var out *v2core.Outbox
	var batches []v2core.Batch
	var rows []v2core.Row
	if direct {
		out = p.openOutbox()
	} else {
		batches = make([]v2core.Batch, len(apps))
		rows = make([]v2core.Row, 0, n)
	}
	for i, a := range apps {
		registered := a.Registered
		if registered == nil {
			var err error
			registered, err = p.RegisterApplication(a.Name, a.Policy)
			if err != nil {
				return p.fail(err)
			}
		}
		id := registered.id
		frames[i] = partlog.CatalogFrame{ID: id, Clock: a.Clock, Points: a.Points}
		if direct {
			if err := out.Start(registered.policy, a.Order, a.Clock, a.Points[0].Timestamp, len(a.Points)); err != nil {
				return p.fail(err)
			}
			for j, point := range a.Points {
				out.Point(point.Timestamp, math.Float64bits(point.Value), a.Sequence[j])
			}
			continue
		}
		start := len(rows)
		for j, point := range a.Points {
			rows = append(rows, v2core.Row{ID: NumericID(id, p.index, p.count), Time: point.Timestamp, Bits: math.Float64bits(point.Value), Seq: a.Sequence[j]})
		}
		batches[i] = v2core.Batch{ID: NumericID(id, p.index, p.count), Order: a.Order, Clock: a.Clock, Points: rows[start:len(rows):len(rows)], Registered: registered.policy}
		if registered.policy == nil {
			// Classic mirroring and the non-rollup layer consume immutable
			// catalog bytes; direct L3 carries its validated handle.
			_, batches[i].Policy, _ = p.catalog.Lookup(id)
		}
	}
	if p.l0 {
		// The packet's cursor is its last order: restart continues above
		// max(sealed cursor, loaded L0 order) without a journal.
		if direct {
			p.packets[len(p.packets)-1].cursor = order
		} else {
			p.packets = append(p.packets, packet{rows: rows, batches: batches, cursor: order})
		}
		p.cursor, p.applyOrder = order, order
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
	start := time.Now()
	encoded, err := p.encoder.EncodeFrames(frames)
	if err != nil {
		return p.fail(err)
	}
	cursor := p.cursor + uint64(n)
	var record []byte
	if stamp == nil && len(p.spareRecords) > 0 {
		last := len(p.spareRecords) - 1
		record = p.spareRecords[last][:0]
		p.spareRecords[last] = nil
		p.spareRecords = p.spareRecords[:last]
	}
	record = appendApplicationRecord(record, cursor, apps, encoded)
	if stamp != nil {
		record = encodePlacementEnvelope(*stamp, record)
	}
	if len(record) > v2replay.MaxRecord {
		return p.fail(errors.New("application journal record exceeds replay bound"))
	}
	p.EncodeTime += time.Since(start)
	p.records = append(p.records, record)
	if direct {
		p.packets[len(p.packets)-1].cursor = cursor
	} else {
		p.packets = append(p.packets, packet{rows: rows, batches: batches, cursor: cursor})
	}
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

// openOutbox returns the outbox of the last queued packet, or queues a packet
// with a free one. Consecutive groups share one outbox until a seal or Drain.
func (p *Producer) openOutbox() *v2core.Outbox {
	if n := len(p.packets); n > 0 && p.packets[n-1].out != nil {
		return p.packets[n-1].out
	}
	var out *v2core.Outbox
	if n := len(p.outboxes); n > 0 {
		out, p.outboxes = p.outboxes[n-1], p.outboxes[:n-1]
	} else {
		out = p.engine.NewOutbox()
	}
	p.packets = append(p.packets, packet{out: out})
	return out
}

// applicationMagic is the V2A2 record written now. Decoders still accept V2A1,
// which repeated the catalog ID and point count of every PDC4 frame and stored
// orders and sequences per application.
var applicationMagic = []byte{'V', '2', 'A', 2}
var applicationCRC = crc32.MakeTable(crc32.Castagnoli)

func isApplicationRecord(b []byte) bool {
	return len(b) >= 4 && string(b[:3]) == "V2A" && (b[3] == 1 || b[3] == 2)
}

func applicationVarintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

// applicationBase returns the first order when application i holds one point
// whose order and sequence are both base+i, as on the TCP path. Otherwise it
// returns 0 and the record keeps explicit orders and sequences.
func applicationBase(apps []Application) uint64 {
	for i, a := range apps {
		if len(a.Points) != 1 || a.Order != apps[0].Order+uint64(i) || a.Sequence[0] != a.Order {
			return 0
		}
	}
	return apps[0].Order
}

// The existing PDC4 point codec is nested unchanged and carries each
// application's catalog ID and points. The outer checksum binds application
// orders and sequences to those original PDC4 frames. Append into
// producer-owned storage retained until the journal fsync succeeds.
func appendApplicationRecord(dst []byte, cursor uint64, apps []Application, pdc4 []byte) []byte {
	base := applicationBase(apps)
	size := len(applicationMagic) + applicationVarintLen(cursor) + applicationVarintLen(uint64(len(apps))) + applicationVarintLen(base) + applicationVarintLen(uint64(len(pdc4))) + len(pdc4) + 4
	for i := 0; base == 0 && i < len(apps); i++ {
		size += applicationVarintLen(apps[i].Order) + applicationVarintLen(uint64(len(apps[i].Points)))
		for _, seq := range apps[i].Sequence {
			size += applicationVarintLen(seq)
		}
	}
	start := len(dst)
	b := append(slices.Grow(dst, size), applicationMagic...)
	b = binary.AppendUvarint(b, cursor)
	b = binary.AppendUvarint(b, uint64(len(apps)))
	b = binary.AppendUvarint(b, base)
	for i := 0; base == 0 && i < len(apps); i++ {
		b = binary.AppendUvarint(b, apps[i].Order)
		b = binary.AppendUvarint(b, uint64(len(apps[i].Points)))
		for _, seq := range apps[i].Sequence {
			b = binary.AppendUvarint(b, seq)
		}
	}
	b = binary.AppendUvarint(b, uint64(len(pdc4)))
	b = append(b, pdc4...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b[start:], applicationCRC))
}

// DecodeApplications verifies a journal application packet against the synced
// catalog and returns detached original groups. It does not mutate core state.
// The decoder retains PDC4 segment predictor state and must be reused in order.
func DecodeApplications(record []byte, decoder *partlog.CatalogDecoder, producer, producers int) ([]v2core.Batch, uint64, error) {
	var err error
	record, _, err = decodePlacementEnvelope(record)
	if err != nil {
		return nil, 0, err
	}
	invalid := errors.New("invalid original application envelope")
	if producer < 0 || producers < 1 || producer >= producers || len(record) < 8 || !isApplicationRecord(record) || crc32.Checksum(record[:len(record)-4], applicationCRC) != binary.LittleEndian.Uint32(record[len(record)-4:]) {
		return nil, 0, invalid
	}
	v1 := record[3] == 1
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
	base := uint64(0)
	if !v1 {
		if base, ok = read(); !ok || base > math.MaxUint64-count+1 {
			return nil, 0, invalid
		}
	}
	out := make([]v2core.Batch, int(count))
	total := uint64(0)
	order := uint64(0)
	for i := range out {
		if base != 0 {
			o := base + uint64(i)
			out[i] = v2core.Batch{Order: o, Points: []v2core.Row{{Seq: o}}}
			total++
			continue
		}
		if v1 {
			if _, ok = read(); !ok { // the catalog ID PDC4 repeats
				return nil, 0, invalid
			}
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
		out[i] = v2core.Batch{Order: o, Points: make([]v2core.Row, int(n))}
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
		if f.ID == 0 || f.ID-1 > (math.MaxUint64-uint64(producer)-1)/uint64(producers) || len(f.Points) != len(out[i].Points) {
			return nil, 0, invalid
		}
		out[i].ID = NumericID(f.ID, producer, producers)
		out[i].Clock = f.Clock
		// Immutable catalog bytes; core copies policies into its own arena.
		out[i].Policy = f.Definition
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
