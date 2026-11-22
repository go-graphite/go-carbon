package v2daemon

import (
	"errors"
	"hash/crc32"
	"math"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
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
	if p.layer != 3 {
		return nil, errors.New("application registration requires the rollup layer")
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
	if p.mirror == nil {
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

// AppendApplications queues explicit original applications in the pending
// packet. Drain syncs definitions, then pipelines the same groups to core;
// the packet's cursor is its last order, so restart continues above
// max(sealed cursor, loaded L0 order) without a journal.
func (p *Producer) AppendApplications(apps []Application) error {
	if p.failed != nil {
		return p.failed
	}
	if p.layer != 3 || len(apps) == 0 || len(apps) > 32768 {
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
	// Producers encode straight into the owners' head rows; classic mirroring
	// submits detached batches instead.
	direct := p.mirror == nil
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
			// Classic mirroring consumes immutable catalog bytes; direct
			// submission carries its validated handle.
			_, batches[i].Policy, _ = p.catalog.Lookup(id)
		}
	}
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

// A legacy producer journal holds V2A1/V2A2 application records; an L0 store
// reads only the newest record's header to refuse an unsealed journal.
var applicationCRC = crc32.MakeTable(crc32.Castagnoli)

func isApplicationRecord(b []byte) bool {
	return len(b) >= 4 && string(b[:3]) == "V2A" && (b[3] == 1 || b[3] == 2)
}
