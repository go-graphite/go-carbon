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
	Registered *v2core.ApplicationPolicy
	// LocalID is the local ID returned with Registered: the identity's row
	// slot in a dictionary store, else the producer catalog ID. It validates
	// the pair without probing the dictionary or a catalog.
	LocalID  uint64
	Name     string
	Policy   []byte
	Order    uint64
	Clock    int64
	Points   []points.Point
	Sequence []uint64
}

// RegisterApplication validates canonical identity and collision/definition
// equality at admission. It retains no caller-owned bytes.
func (p *Producer) RegisterApplication(name string, policy []byte) (*v2core.ApplicationPolicy, uint64, error) {
	if p.failed != nil {
		return nil, 0, p.failed
	}
	if p.layer != 3 {
		return nil, 0, errors.New("application registration requires the rollup layer")
	}
	if p.dictStore() {
		h, err := p.engine.AdmitIdentity(name, policy)
		if err != nil {
			return nil, 0, err
		}
		return h, uint64(h.Slot()), nil
	}
	if err := p.openCatalog(); err != nil {
		return nil, 0, err
	}
	id, err := p.catalog.Intern(name, policy)
	if err != nil {
		return nil, 0, err
	}
	return p.application(id)
}

// application returns the handle the catalog binding installed in this
// engine epoch, including during recovery.
func (p *Producer) application(id uint64) (*v2core.ApplicationPolicy, uint64, error) {
	if h := p.handle(id); h != nil {
		return h, id, nil
	}
	return nil, 0, errors.New("application catalog has no bound policy")
}

// handle returns the identity of a local ID: the row slot in a dictionary
// store, else the catalog ID.
func (p *Producer) handle(id uint64) *v2core.ApplicationPolicy {
	if p.dictStore() {
		if id > math.MaxUint32 {
			return nil
		}
		return p.engine.SlotIdentity(uint32(id))
	}
	if id < uint64(len(p.applications)) {
		return p.applications[id]
	}
	return nil
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
		if (a.Registered == nil && a.Name == "") || (a.Registered != nil && (a.LocalID == 0 || p.handle(a.LocalID) != a.Registered || a.Name != "" || len(a.Policy) != 0)) || a.Order <= order || len(a.Points) == 0 || len(a.Points) != len(a.Sequence) || len(a.Points) > 32768 || n > 32768-len(a.Points) {
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
		registered, id := a.Registered, a.LocalID
		if registered == nil {
			var err error
			registered, id, err = p.RegisterApplication(a.Name, a.Policy)
			if err != nil {
				return p.fail(err)
			}
		}
		if direct {
			if err := out.Start(registered, a.Order, a.Clock, a.Points[0].Timestamp, len(a.Points)); err != nil {
				return p.fail(err)
			}
			for j, point := range a.Points {
				out.Point(point.Timestamp, math.Float64bits(point.Value), a.Sequence[j])
			}
			continue
		}
		start := len(rows)
		for j, point := range a.Points {
			rows = append(rows, v2core.Row{ID: registered.ID(), Time: point.Timestamp, Bits: math.Float64bits(point.Value), Seq: a.Sequence[j]})
		}
		batches[i] = v2core.Batch{ID: registered.ID(), Order: a.Order, Clock: a.Clock, Points: rows[start:len(rows):len(rows)]}
		// Classic mirroring consumes the original immutable canonical bytes.
		if p.dictStore() {
			batches[i].Policy = registered.Definition()
		} else {
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
