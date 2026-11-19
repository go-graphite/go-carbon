package v2daemon

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

// PolicyResolver returns the immutable policy for a newly admitted name.
// TCP contains points rather than classic persister UpdateMany boundaries. Each
// point is an explicit application; batching socket reads must not change it.
// An error refuses only that name's point.
type PolicyResolver func(string) ([]byte, error)

// ErrNameTooLong refuses names classic cannot store either: longer than
// points.MaxMetricNameBytes, or with a Whisper path component (the leaf with
// its ".wsp") over the 255-byte filesystem name limit.
var ErrNameTooLong = errors.New("metric name too long")

func checkNameLimits(name string) error {
	if len(name) > points.MaxMetricNameBytes {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrNameTooLong, len(name), points.MaxMetricNameBytes)
	}
	for rest := name; ; {
		i := strings.IndexByte(rest, '.')
		leaf := i < 0
		if leaf {
			i = len(rest) + len(".wsp")
		}
		if i > maxFileNameBytes {
			return fmt.Errorf("%w: path component of %d bytes, limit %d", ErrNameTooLong, i, maxFileNameBytes)
		}
		if leaf {
			return nil
		}
		rest = rest[i+1:]
	}
}

const maxFileNameBytes = 255 // NAME_MAX on Linux filesystems

type applicationOwner struct {
	p        *Producer
	resolve  PolicyResolver
	policies map[string]*RegisteredApplication
	throttle func(*points.Points, bool) bool
	reject   func(name string, err error) // one refused point; nil ignores
	apps     []Application
	sequence []uint64
}

// register admits a new name. Its errors refuse only this name unless the
// producer itself failed; a catalog I/O failure surfaces at the next Drain.
func (o *applicationOwner) register(name string) (*RegisteredApplication, error) {
	if err := checkNameLimits(name); err != nil {
		return nil, err
	}
	name = strings.Clone(name) // retained; the receiver's batch text is borrowed
	policy, err := o.resolve(name)
	if err != nil {
		return nil, err
	}
	registered, err := o.p.RegisterApplication(name, policy)
	if err != nil {
		return nil, err
	}
	if o.policies == nil {
		o.policies = map[string]*RegisteredApplication{}
	}
	o.policies[name] = registered
	return registered, nil
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
			var err error
			if registered, err = o.register(name); err != nil {
				if o.p.failed != nil {
					return err
				}
				if o.reject != nil {
					o.reject(name, err)
				}
				continue
			}
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

// Ordinary ticks sync the journal; only maintenance requires a core frontier.
func (o *applicationOwner) Seal() error  { return nil }
func (o *applicationOwner) Drain() error { return o.p.Drain() }
func (o *applicationOwner) Flush() error { return o.p.Flush() }

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
	return s.ReceiveApplicationsTCPWithAdmission(l, cfg, resolve, nil, nil)
}

// ReceiveApplicationsTCPWithAdmission applies the host quota gate before
// journaling. reject sees each point refused by its name; intake continues.
func (s *Store) ReceiveApplicationsTCPWithAdmission(l net.Listener, cfg v2recv.Config, resolve PolicyResolver, throttle func(*points.Points, bool) bool, reject func(string, error)) (*v2recv.Server, error) {
	if s.cfg.Layer != 3 || resolve == nil || s.cfg.Placement != nil {
		return nil, errors.New("L3 TCP requires policy resolver and unplaced input; stamped transport is not implemented")
	}
	owners := make([]v2recv.Owner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = &applicationOwner{p: p, resolve: resolve, throttle: throttle, reject: reject}
	}
	return v2recv.Start(l, owners, cfg)
}
