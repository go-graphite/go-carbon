package v2daemon

import (
	"errors"
	"net"
	"slices"

	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

// PolicyResolver returns the immutable policy for a newly admitted name.
// TCP contains points rather than classic persister UpdateMany boundaries. Each
// point is an explicit application; batching socket reads must not change it.
type PolicyResolver func(string) ([]byte, error)
type applicationOwner struct {
	p          *Producer
	resolve    PolicyResolver
	policies   map[string]*RegisteredApplication
	throttle   func(*points.Points, bool) bool
	fenceDrain bool
	apps       []Application
	sequence   []uint64
}

func (o *applicationOwner) Append(names []string, data []points.Point, _ []uint64, clock int64) error {
	o.apps = slices.Grow(o.apps[:0], len(names))
	o.sequence = slices.Grow(o.sequence[:0], len(names))[:len(names)]
	apps := o.apps[:0]
	defer func() { clear(apps); o.apps = apps[:0] }()
	var previous *RegisteredApplication
	for i, name := range names {
		if o.throttle != nil && o.throttle(&points.Points{Metric: name, Data: data[i : i+1]}, false) {
			continue
		}
		registered := previous
		if registered == nil || registered.name != name {
			registered = o.policies[name]
		}
		if registered == nil {
			policy, err := o.resolve(name)
			if err != nil {
				return err
			}
			registered, err = o.p.RegisterApplication(name, policy)
			if err != nil {
				return err
			}
			if o.policies == nil {
				o.policies = map[string]*RegisteredApplication{}
			}
			o.policies[name] = registered
		}
		previous = registered
		index := len(apps)
		order := o.p.applyOrder + uint64(index) + 1
		o.sequence[index] = order
		apps = append(apps, Application{Registered: registered, Order: order, Clock: clock, Points: data[i : i+1], Sequence: o.sequence[index : index+1 : index+1]})
	}
	if len(apps) == 0 {
		return nil
	}
	return o.p.AppendApplications(apps)
}
func (o *applicationOwner) Seal() error {
	// Ordinary journal syncs leave part formation to the core's size/1s
	// thresholds. One producer tick must not seal every core owner.
	// Expiry still needs the existing synchronous frontier fence in Drain.
	if o.fenceDrain {
		return o.p.Seal()
	}
	return nil
}
func (o *applicationOwner) Drain() error {
	if err := o.p.Drain(); err != nil {
		return err
	}
	if o.fenceDrain && o.p.cursor > 0 {
		// Pending expiry reservations release only after this producer's original
		// cursor has reached every core owner, rather than just its journal fsync.
		return o.p.engine.AppendBatches(o.p.index, nil, o.p.cursor, true)
	}
	return nil
}

func (s *Store) ReceiveTCP(l net.Listener, cfg v2recv.Config) (*v2recv.Server, error) {
	if s.cfg.Placement != nil {
		return nil, errors.New("raw TCP requires unplaced input; stamped transport is not implemented")
	}
	if s.cfg.Layer != 1 && s.cfg.Layer != 2 {
		return nil, errors.New("raw TCP adapter requires named layer 1 or journaled layer 2")
	}
	owners := make([]v2recv.Owner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = p
	}
	return v2recv.Start(l, owners, cfg)
}

// ReceiveApplicationsTCP wires canonical L3 rollups and lossless journal replay.
// Geometry and original application order survive receiver/restart boundaries.
func (s *Store) ReceiveApplicationsTCP(l net.Listener, cfg v2recv.Config, resolve PolicyResolver) (*v2recv.Server, error) {
	return s.ReceiveApplicationsTCPWithAdmission(l, cfg, resolve, nil)
}

// ReceiveApplicationsTCPWithAdmission applies the host quota gate before journaling.
func (s *Store) ReceiveApplicationsTCPWithAdmission(l net.Listener, cfg v2recv.Config, resolve PolicyResolver, throttle func(*points.Points, bool) bool) (*v2recv.Server, error) {
	if s.cfg.Layer != 3 || resolve == nil || s.cfg.Placement != nil {
		return nil, errors.New("L3 TCP requires policy resolver and unplaced input; stamped transport is not implemented")
	}
	owners := make([]v2recv.Owner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = &applicationOwner{p: p, resolve: resolve, throttle: throttle, fenceDrain: s.cfg.Hooks.Expire != nil}
	}
	return v2recv.Start(l, owners, cfg)
}
