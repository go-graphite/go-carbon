package v2daemon

import (
	"context"
	"fmt"

	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/points"
)

// Placement is the store boundary: storage owns journal/core durability, while
// the authority owns admission and persisted epoch floors.
type Placement interface {
	Register() v2placement.Sink
	CheckNames([]string) error
	Replay(v2placement.Stamp) error
	Fence(context.Context, string, []v2placement.EndMarker, func() (v2placement.Frontier, v2placement.Roots, error)) (v2placement.Fence, error)
}

// Append remains the unversioned experiment path. A placed daemon must use
// AppendPlaced; rejected routing metadata never poisons the producer journal.
func (p *Producer) Append(names []string, _ []uint64, data []points.Point, sequence []uint64, clock int64) error {
	if p.admission != nil {
		return v2placement.ErrUnversioned
	}
	return p.appendNamed(names, data, sequence, clock)
}
func (p *Producer) AppendApplications(apps []Application) error {
	if p.admission != nil {
		return v2placement.ErrUnversioned
	}
	return p.appendApplications(apps)
}
func (p *Producer) AppendPlaced(stamp v2placement.Stamp, names []string, data []points.Point, sequence []uint64, clock int64) error {
	if p.admission == nil {
		return fmt.Errorf("placement is not configured")
	}
	release, e := p.admission.Begin(stamp)
	if e != nil {
		return e
	}
	defer release()
	if e = p.placement.CheckNames(names); e != nil {
		return e
	}
	if p.layer != 2 || len(names) == 0 || len(names) != len(data) || len(names) != len(sequence) || len(names) > 32768 {
		return p.fail(fmt.Errorf("invalid placed named batch"))
	}
	apps := make([]Application, len(names))
	for i, name := range names {
		apps[i] = Application{Name: name, Order: p.applyOrder + uint64(i) + 1, Clock: clock, Points: data[i : i+1], Sequence: sequence[i : i+1]}
	}
	return p.appendApplicationsStamped(apps, &stamp)
}
func (p *Producer) AppendApplicationsPlaced(stamp v2placement.Stamp, apps []Application) error {
	if p.admission == nil {
		return fmt.Errorf("placement is not configured")
	}
	release, e := p.admission.Begin(stamp)
	if e != nil {
		return e
	}
	defer release()
	for _, app := range apps {
		name := app.Name
		if app.Registered != nil {
			if app.Registered.producer != p || app.Name != "" || len(app.Policy) != 0 {
				return fmt.Errorf("invalid registered application")
			}
			name = app.Registered.name
		}
		if e = p.placement.CheckNames([]string{name}); e != nil {
			return e
		}
	}
	return p.appendApplicationsStamped(apps, &stamp)
}

// FencePlacement closes intake and waits for admitted batches before draining
// journals and synchronously sealing all core owners. Callers must stop invoking
// producer lifecycle methods (Seal/Drain/Close) before this control operation.
// certify must verify the authenticated end markers against durable stream ACKs
// and return coherent pinned part/catalog/policy/tombstone roots. Until that
// adapter exists, returning an error keeps the PG fenced without a receipt.
func (s *Store) FencePlacement(ctx context.Context, operation string, ends []v2placement.EndMarker, certify func([][]uint64) (v2placement.Roots, error)) (v2placement.Fence, error) {
	if s.cfg.Placement == nil {
		return v2placement.Fence{}, fmt.Errorf("placement is not configured")
	}
	return s.cfg.Placement.Fence(ctx, operation, ends, func() (v2placement.Frontier, v2placement.Roots, error) {
		for _, p := range s.Producers {
			if e := p.Drain(); e != nil {
				return nil, v2placement.Roots{}, e
			}
			if p.cursor == 0 {
				continue
			}
			var e error
			if p.layer == 3 {
				e = p.engine.AppendBatches(p.index, nil, p.cursor, true)
			} else {
				e = p.engine.Append(p.index, nil, p.cursor, true)
			}
			if e != nil {
				return nil, v2placement.Roots{}, e
			}
		}
		frontiers := s.Core.DurableFrontiers()
		if certify == nil {
			return nil, v2placement.Roots{}, fmt.Errorf("durable coverage certification required")
		}
		roots, e := certify(frontiers)
		if e != nil {
			return nil, roots, e
		}
		var f v2placement.Frontier
		for owner, cursors := range frontiers {
			for producer, cursor := range cursors {
				f = append(f, v2placement.Position{Owner: fmt.Sprintf("owner-%d/producer-%d", owner, producer), Cursor: cursor})
			}
		}
		return f, roots, nil
	})
}
