package v2backend

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/go-graphite/go-carbon/internal/partsread"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// liveBase is published only after the sealed snapshot and all frontiers agree.
// Each reader pins its publication before inspecting any source or frontier.
type liveBase struct {
	pub       partsread.Publication
	catalog   *catalog
	source    *diskSource
	cursor    uint64
	frontiers map[string]sealrollup.Frontier
}
type liveCapture struct {
	base      *liveBase
	pin       *partsread.Handle
	frames    []frame
	workspace []*partsread.Handle
	once      sync.Once
}

func (c *liveCapture) close() {
	c.once.Do(func() {
		for _, h := range c.workspace {
			h.Close()
		}
		c.pin.Close()
		c.frames = nil
		c.workspace = nil
	})
}

// Reserve before growing read-side seed maps. Charge occupied slots rather than
// configured retention: a sparse metric can have a very large empty archive.
func (c *liveCapture) reserve(b *partsread.Budget, n int64) error {
	var p partsread.Publication
	p.Publish(partsread.View{})
	h, e := p.Pin(b, n)
	p.Close()
	if e != nil {
		return e
	}
	c.workspace = append(c.workspace, h)
	return nil
}

// Called on the admission owner. Only slice headers are copied: accepted frame
// values and their point arrays are immutable. Seal publication can race this
// exchange; filtering against its pinned cursor makes either generation exact.
func (o *owner) captureLive(name string) (*liveCapture, error) {
	for {
		base := o.readBase.Load()
		if base == nil {
			return nil, ErrClosed
		}
		var charge int64
		var count int
		for _, head := range [][]frame{o.pending, o.active} {
			for _, f := range head {
				if f.cursor > base.cursor && (name == "" || f.name == name) {
					charge += int64(256 + len(f.name) + len(f.points)*16)
					count++
				}
			}
		}
		// Budget detached reducer input before allocating it. Seed maps reserve
		// additional credits as occupied slots are encountered, not by retention.
		if name != "" && count > 0 {
			for _, f := range [][]frame{o.pending, o.active} {
				for _, v := range f {
					if v.cursor > base.cursor && v.name == name {
						charge += int64(len(v.points)) * 64
					}
				}
			}
		}
		pin, e := base.pub.Pin(o.s.budget, charge)
		if errors.Is(e, partsread.ErrClosed) {
			continue
		}
		if e != nil {
			return nil, e
		}
		c := &liveCapture{base: base, pin: pin, frames: make([]frame, 0, count)}
		for _, head := range [][]frame{o.pending, o.active} {
			for _, f := range head {
				if f.cursor > base.cursor && (name == "" || f.name == name) {
					c.frames = append(c.frames, f)
				}
			}
		}
		return c, nil
	}
}
func (s *Store) capture(ctx context.Context, o *owner, name string) (*liveCapture, error) {
	if e := s.status(); e != nil {
		return nil, e
	}
	ack := make(chan answer, 1)
	select {
	case o.requests <- command{op: 'v', frame: &frame{name: name}, answer: ack}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, ErrClosed
	}
	select {
	case a := <-ack:
		return a.live, a.err
	case <-ctx.Done():
		go func() {
			select {
			case a := <-ack:
				if a.live != nil {
					a.live.close()
				}
			case <-o.done:
				select {
				case a := <-ack:
					if a.live != nil {
						a.live.close()
					}
				default:
				}
			}
		}()
		return nil, ctx.Err()
	case <-o.done:
		select {
		case a := <-ack:
			if a.live != nil {
				a.live.close()
			}
		default:
		}
		return nil, ErrClosed
	}
}

// livePublication reduces the pinned accepted prefix on the read side. It never
// writes, syncs or seals. The response pin owns both the base and live receipts.
func (s *Store) livePublication(ctx context.Context, name string) (_ *partsread.Publication, _ func(), err error) {
	c, e := s.capture(ctx, s.owners[s.route(name)], name)
	if e != nil {
		return nil, nil, e
	}
	defer func() {
		if err != nil {
			c.close()
		}
	}()
	if e := ctx.Err(); e != nil {
		return nil, nil, e
	}
	cat := c.base.catalog
	var records []partsread.Record
	if len(c.frames) > 0 {
		last := c.frames[len(c.frames)-1]
		m, exists := cat.Lookup(name)
		if !exists {
			m = toReadMetric(last.policy)
			m.ID = 1
			cat = &catalog{names: map[string]partsread.Metric{name: m}, ids: map[uint64]string{m.ID: name}}
		}
		cfg, e := rollupConfig(last.policy)
		if e != nil {
			return nil, nil, e
		}
		prepared, e := sealrollup.Prepare(cfg)
		if e != nil {
			return nil, nil, e
		}
		var samples int64
		for _, f := range c.frames {
			samples += int64(len(f.points))
		}
		if e = c.reserve(s.budget, samples*int64(len(m.Archives))*256+int64(len(c.frames))*64); e != nil {
			return nil, nil, e
		}
		slots := make([]map[int64]sealrollup.Sample, len(m.Archives))
		for i, a := range m.Archives {
			slots[i] = make(map[int64]sealrollup.Sample)
			if !exists || i+1 == len(m.Archives) {
				continue
			}
			e = c.base.source.Scan(ctx, m.ID, a, 0, a.Step*a.Points, func(p partsread.Sample) error {
				slot := (p.Time / a.Step) % a.Points
				old, ok := slots[i][slot]
				if !ok || p.Sequence > old.Sequence {
					if !ok && len(slots[i])%1024 == 0 {
						if e := c.reserve(s.budget, 1024*96); e != nil {
							return e
						}
					}
					slots[i][slot] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
				}
				return nil
			})
			if e != nil {
				return nil, nil, e
			}
		}
		batches := make([]sealrollup.Batch, len(c.frames))
		for i, f := range c.frames {
			b := sealrollup.Batch{Order: f.order, Now: f.clock, Points: make([]sealrollup.Sample, len(f.points))}
			for j, p := range f.points {
				b.Points[j] = sealrollup.Sample{Time: p.Timestamp, Bits: math.Float64bits(p.Value), Sequence: uint64(j + 1)}
			}
			batches[i] = b
		}
		plan, e := prepared.Plan(c.base.frontiers[name], func(a int, slot int64) (sealrollup.Sample, bool) { p, ok := slots[a][slot]; return p, ok }, nil, batches)
		if e != nil {
			return nil, nil, e
		}
		for _, r := range plan.Records {
			records = append(records, partsread.Record{ID: m.ID, Step: m.Archives[r.Archive].Step, Sample: partsread.Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}})
		}
	}
	view := partsread.View{Catalog: cat, Close: c.close}
	if _, ok := c.base.catalog.Lookup(name); ok {
		view.Parts = []partsread.Source{c.base.source}
	}
	if len(records) > 0 {
		view.Head = []partsread.Source{partsread.Freeze(records)}
	}
	p := &partsread.Publication{}
	p.Publish(view)
	return p, p.Close, nil
}
