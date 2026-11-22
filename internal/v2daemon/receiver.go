package v2daemon

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/go-graphite/go-carbon/internal/namefp"
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

// applicationOwner maps a name to its producer-local catalog ID with one
// namefp.Table probe keyed by the name's 96-bit fingerprint. No name bytes are
// read or kept: a miss proves the name is new, and it resolves the policy and
// registers once per lifetime.
type applicationOwner struct {
	p        *Producer
	resolve  PolicyResolver
	throttle func(*points.Points, bool) bool
	reject   func(name string, err error) // one refused point; nil ignores
	names    namefp.Table                 // fingerprint → producer-local catalog ID
	apps     []Application
	sequence []uint64
}

// register admits a new name; the borrowed batch name is not retained. Its
// errors refuse only this name unless the producer itself failed; a catalog
// I/O failure surfaces at the next Drain.
func (o *applicationOwner) register(name string, fp namefp.Fingerprint) (uint32, error) {
	if err := checkNameLimits(name); err != nil {
		return 0, err
	}
	policy, err := o.resolve(name)
	if err != nil {
		return 0, err
	}
	registered, err := o.p.RegisterApplication(name, policy)
	if err != nil {
		return 0, err
	}
	if registered.id > namefp.MaxLocal {
		return 0, errors.New("receiver name table: producer catalog ID exceeds 32 bits")
	}
	o.names.Put(fp, uint32(registered.id))
	return uint32(registered.id), nil
}

func (o *applicationOwner) Append(names []string, hashes []uint64, data []points.Point, _ []uint64, clock int64) error {
	return o.appendNames(names, nil, nil, hashes, data, clock)
}

func (o *applicationOwner) AppendBytes(text []byte, ends []int, hashes []uint64, data []points.Point, _ []uint64, clock int64) error {
	return o.appendNames(nil, text, ends, hashes, data, clock)
}

func (o *applicationOwner) appendNames(names []string, text []byte, ends []int, hashes []uint64, data []points.Point, clock int64) error {
	o.apps = slices.Grow(o.apps[:0], len(data))
	o.sequence = slices.Grow(o.sequence[:0], len(data))[:len(data)]
	apps := o.apps[:0]
	defer func() { clear(apps); o.apps = apps[:0] }()
	var previous namefp.Fingerprint
	var local uint32 // 0 until the first admitted name: catalog IDs start at 1
	start := 0
	for i := range data {
		var name string
		var raw []byte
		if ends != nil {
			raw = text[start:ends[i]]
			start = ends[i]
		} else {
			name = names[i]
		}
		var route uint64
		switch {
		case hashes != nil:
			route = hashes[i]
		case ends != nil:
			route = namefp.Route(raw)
		default:
			route = namefp.Route(name)
		}
		var hash uint64
		if ends != nil {
			hash = namefp.HashBytes(raw)
		} else {
			hash = namefp.Hash(name)
		}
		fp := namefp.Of(hash, route)
		if local == 0 || previous != fp {
			local, _ = o.names.Get(fp)
			previous = fp
		}
		if ends != nil {
			if local == 0 {
				name = string(raw)
			} else if o.throttle != nil {
				name = o.p.applications[local].name() // quota checks reuse the catalog string
			}
		}
		if o.throttle != nil && o.throttle(&points.Points{Metric: name, Data: data[i : i+1]}, false) {
			continue
		}
		if local == 0 {
			var err error
			if local, err = o.register(name, fp); err != nil {
				if o.p.failed != nil {
					return err
				}
				if o.reject != nil {
					o.reject(name, err)
				}
				continue // local is 0, so the next point probes again
			}
		}
		index := len(apps)
		order := o.p.applyOrder + uint64(index) + 1
		o.sequence[index] = order
		apps = append(apps, Application{Registered: o.p.applications[local], Order: order, Clock: clock, Points: data[i : i+1], Sequence: o.sequence[index : index+1 : index+1]})
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
	if s.cfg.Layer != 1 {
		return nil, errors.New("raw TCP adapter requires named layer 1")
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
	if s.cfg.Layer != 3 || resolve == nil {
		return nil, errors.New("L3 TCP requires a policy resolver")
	}
	owners := make([]v2recv.Owner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = &applicationOwner{p: p, resolve: resolve, throttle: throttle, reject: reject}
	}
	return v2recv.Start(l, owners, cfg)
}
