package v2daemon

import (
	"errors"
	"net"

	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

// PolicyResolver returns the immutable policy for a newly admitted name.
// TCP contains points rather than classic persister UpdateMany boundaries. Each
// point is an explicit application; batching socket reads must not change it.
type PolicyResolver func(string) ([]byte, error)
type applicationOwner struct {
	p       *Producer
	resolve PolicyResolver
}

func (o *applicationOwner) Append(names []string, data []points.Point, _ []uint64, clock int64) error {
	apps := make([]Application, len(names))
	for i, name := range names {
		policy, err := o.resolve(name)
		if err != nil {
			return err
		}
		order := o.p.applyOrder + uint64(i) + 1
		apps[i] = Application{Name: name, Policy: policy, Order: order, Clock: clock, Points: data[i : i+1], Sequence: []uint64{order}}
	}
	return o.p.AppendApplications(apps)
}
func (o *applicationOwner) Seal() error  { return o.p.Seal() }
func (o *applicationOwner) Drain() error { return o.p.Drain() }

func (s *Store) ReceiveTCP(l net.Listener, cfg v2recv.Config) (*v2recv.Server, error) {
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
	if s.cfg.Layer != 3 || resolve == nil || s.cfg.Placement != nil {
		return nil, errors.New("L3 TCP requires policy resolver and unplaced input; stamped transport is not implemented")
	}
	owners := make([]v2recv.Owner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = &applicationOwner{p, resolve}
	}
	return v2recv.Start(l, owners, cfg)
}
