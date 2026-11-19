package v2daemon

import (
	"errors"
	"math"
	"net"
	"slices"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
)

// PolicyResolver returns the immutable policy for a newly admitted name.
// TCP contains points rather than classic persister UpdateMany boundaries. Each
// point is an explicit application; batching socket reads must not change it.
type PolicyResolver func(string) ([]byte, error)

// applicationOwner maps a name to its producer-local catalog ID with one probe
// of a pointer-free open-addressing table keyed by the name's 96-bit
// fingerprint. No name bytes are read or kept: a miss proves the name is new,
// and it resolves the policy and registers once per lifetime.
type applicationOwner struct {
	p        *Producer
	resolve  PolicyResolver
	throttle func(*points.Points, bool) bool
	slots    []nameSlot // power-of-two length, at most 3/4 used
	used     int
	apps     []Application
	sequence []uint64
}

// nameSlot is 16 bytes with no pointers, so the GC never scans the table.
// local 0 marks an empty slot: catalog IDs start at 1.
type nameSlot struct {
	fp    namefp.Fingerprint
	local uint32 // producer-local catalog ID
}

// find returns fp's slot, or the empty slot where it belongs.
func (o *applicationOwner) find(fp namefp.Fingerprint) *nameSlot {
	mask := uint64(len(o.slots) - 1)
	for i := fp.Hash & mask; ; i = (i + 1) & mask {
		if s := &o.slots[i]; s.local == 0 || s.fp == fp {
			return s
		}
	}
}

func (o *applicationOwner) grow() {
	old := o.slots
	o.slots = make([]nameSlot, max(1024, 2*len(old)))
	for _, s := range old {
		if s.local != 0 {
			*o.find(s.fp) = s
		}
	}
}

// register admits a new name; the borrowed batch name is not retained.
func (o *applicationOwner) register(name string, fp namefp.Fingerprint) (nameSlot, error) {
	policy, err := o.resolve(name)
	if err != nil {
		return nameSlot{}, err
	}
	registered, err := o.p.RegisterApplication(name, policy)
	if err != nil {
		return nameSlot{}, err
	}
	if registered.id > math.MaxUint32 {
		return nameSlot{}, errors.New("receiver name table: producer catalog ID exceeds 32 bits")
	}
	if (o.used+1)*4 > len(o.slots)*3 {
		o.grow()
	}
	s := o.find(fp)
	*s = nameSlot{fp: fp, local: uint32(registered.id)}
	o.used++
	return *s, nil
}

func (o *applicationOwner) Append(names []string, hashes []uint64, data []points.Point, _ []uint64, clock int64) error {
	o.apps = slices.Grow(o.apps[:0], len(names))
	o.sequence = slices.Grow(o.sequence[:0], len(names))[:len(names)]
	apps := o.apps[:0]
	defer func() { clear(apps); o.apps = apps[:0] }()
	if len(o.slots) == 0 {
		o.grow()
	}
	var previous nameSlot
	for i, name := range names {
		if o.throttle != nil && o.throttle(&points.Points{Metric: name, Data: data[i : i+1]}, false) {
			continue
		}
		var fnv uint64
		if hashes != nil {
			fnv = hashes[i]
		} else {
			fnv = v2recv.NameHash(name)
		}
		if fp := namefp.Of(name, fnv); previous.local == 0 || previous.fp != fp {
			if previous = *o.find(fp); previous.local == 0 {
				var err error
				if previous, err = o.register(name, fp); err != nil {
					return err
				}
			}
		}
		index := len(apps)
		order := o.p.applyOrder + uint64(index) + 1
		o.sequence[index] = order
		apps = append(apps, Application{Registered: o.p.applications[previous.local], Order: order, Clock: clock, Points: data[i : i+1], Sequence: o.sequence[index : index+1 : index+1]})
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
	return s.ReceiveApplicationsTCPWithAdmission(l, cfg, resolve, nil)
}

// ReceiveApplicationsTCPWithAdmission applies the host quota gate before journaling.
func (s *Store) ReceiveApplicationsTCPWithAdmission(l net.Listener, cfg v2recv.Config, resolve PolicyResolver, throttle func(*points.Points, bool) bool) (*v2recv.Server, error) {
	if s.cfg.Layer != 3 || resolve == nil || s.cfg.Placement != nil {
		return nil, errors.New("L3 TCP requires policy resolver and unplaced input; stamped transport is not implemented")
	}
	owners := make([]v2recv.Owner, len(s.Producers))
	for i, p := range s.Producers {
		owners[i] = &applicationOwner{p: p, resolve: resolve, throttle: throttle}
	}
	return v2recv.Start(l, owners, cfg)
}
