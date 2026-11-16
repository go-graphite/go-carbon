package v2daemon

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/v2placement"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/internal/v2wire"
	"github.com/go-graphite/go-carbon/points"
	"math"
	"net"
	"sync"
	"time"
)

type relayPosition struct {
	Sequence uint64
	Digest   string
}

func relayScope(stamp v2placement.Stamp) v2placement.Stamp {
	stamp.MapDigest = ""
	stamp.RelaySequence = 0
	stamp.RelayDigest = ""
	return stamp
}
func (p *Producer) rememberRelay(stamp v2placement.Stamp) error {
	if stamp.RelaySequence == 0 || len(stamp.RelayDigest) != 64 {
		return errors.New("invalid journal relay position")
	}
	k := relayScope(stamp)
	last := p.relayPositions[k]
	if last.Sequence == ^uint64(0) || stamp.RelaySequence != last.Sequence+1 {
		return errors.New("journal relay position gap")
	}
	if p.relayPositions == nil {
		p.relayPositions = make(map[v2placement.Stamp]relayPosition)
	}
	p.relayPositions[k] = relayPosition{stamp.RelaySequence, stamp.RelayDigest}
	return nil
}

type relayReceiver struct {
	store    *Store
	locks    []sync.Mutex
	resolve  PolicyResolver
	document interface{ Document() v2placement.Document }
}

func (r *relayReceiver) AcceptPlaced(f v2wire.Frame) error {
	if err := f.Validate(); err != nil || f.Kind != v2wire.Data {
		return errors.New("invalid placed delivery")
	}
	// All names on a relay stream use one serial producer. Geometry is persisted
	// by the daemon and cannot silently change across restart.
	h := uint64(14695981039346656037)
	for _, s := range []string{f.Stream.Producer, "/", f.Stream.Lane} {
		for i := range s {
			h = (h ^ uint64(s[i])) * 1099511628211
		}
	}
	i := int(h % uint64(len(r.locks)))
	r.locks[i].Lock()
	defer r.locks[i].Unlock()
	p := r.store.Producers[i]
	stamp := v2placement.Stamp{Group: f.Stream.Group, Replica: f.Stream.Replica, MapDigest: f.Stream.MapDigest, Epoch: f.Stream.Epoch, Lane: v2placement.Lane{Producer: f.Stream.Producer, Stream: f.Stream.Lane}, RelaySequence: f.Sequence, RelayDigest: f.Digest()}
	last := p.relayPositions[relayScope(stamp)]
	if f.Sequence == last.Sequence {
		if last.Digest != stamp.RelayDigest {
			return errors.New("conflicting relay retry")
		}
		return nil
	}
	if last.Sequence == ^uint64(0) || f.Sequence != last.Sequence+1 {
		return errors.New("relay sequence gap or stale retry")
	}
	d := r.document.Document()
	selector, v, err := d.Routing.Select(f.Name)
	if err != nil {
		return err
	}
	rep, err := d.Lookup(selector.GroupID(), f.Stream.Replica)
	if err != nil {
		return err
	}
	if selector.GroupID() != f.Stream.Group || v != f.VShard || rep.Writer != f.Stream.Store || rep.Epoch != f.Stream.Epoch || d.Digest() != f.Stream.MapDigest || d.Revision != f.Stream.Revision {
		return v2placement.ErrStaleEpoch
	}
	app := Application{Name: f.Name, Order: p.applyOrder + 1, Clock: time.Now().Unix(), Points: []points.Point{{Value: math.Float64frombits(f.Bits), Timestamp: f.Timestamp}}, Sequence: []uint64{p.applyOrder + 1}}
	if r.resolve != nil {
		app.Policy, err = r.resolve(f.Name)
		if err != nil {
			return err
		}
	}
	if err = p.AppendApplicationsPlaced(stamp, []Application{app}); err != nil {
		return err
	}
	// Drain syncs catalog + stamped journal before ACK. Recovery derives the
	// dedup frontier from that same journal; there is no receipt/data crash gap.
	if err = p.Drain(); err != nil {
		return err
	}
	return p.rememberRelay(stamp)
}

// ReceivePlacedTCP owns all producer methods until Server.Wait completes. Stop
// and wait before fencing, closing or invoking producer lifecycle methods.
// Each endpoint authenticates one persistent producer incarnation using its
// own >=32-byte secret. Secrets must not be reused across producer identities.
func (s *Store) ReceivePlacedTCP(l net.Listener, cfg v2recv.Config, resolve PolicyResolver) (*v2recv.Server, error) {
	d, ok := s.cfg.Placement.(interface{ Document() v2placement.Document })
	if !ok || s.cfg.Layer < 2 || (s.cfg.Layer == 3 && resolve == nil) {
		return nil, fmt.Errorf("placed TCP requires document authority and durable layer/policy")
	}
	if !s.relayActive.CompareAndSwap(false, true) {
		return nil, errors.New("placed receiver already owns producers")
	}
	cfg.Placement = &relayReceiver{store: s, locks: make([]sync.Mutex, len(s.Producers)), resolve: resolve, document: d}
	onStopped := cfg.OnStopped
	cfg.OnStopped = func() {
		s.relayActive.Store(false)
		if onStopped != nil {
			onStopped()
		}
	}
	server, err := v2recv.Start(l, nil, cfg)
	if err != nil {
		s.relayActive.Store(false)
		return nil, err
	}

	return server, nil
}

// DurableRelayEnds reads exact recovered journal positions after receiver Wait.
// Zero-position lanes are included, so a missing producer cannot disappear from
// the configured census. Callers must retain exclusive producer ownership.
func (s *Store) DurableRelayEnds() ([]v2placement.EndMarker, error) {
	if s.relayActive.Load() {
		return nil, errors.New("wait for placed receiver before reading durable frontier")
	}
	authority, ok := s.cfg.Placement.(interface {
		Document() v2placement.Document
		Scope() (string, string, string)
	})
	if !ok {
		return nil, errors.New("placement authority scope required")
	}
	group, replica, store := authority.Scope()
	doc := authority.Document()
	rep, err := doc.Lookup(group, replica)
	if err != nil {
		return nil, err
	}
	if rep.Writer != store {
		return nil, v2placement.ErrStaleEpoch
	}
	positions := make(map[v2placement.Lane]uint64)
	for _, p := range s.Producers {
		for stamp, pos := range p.relayPositions {
			if stamp.Group == group && stamp.Replica == replica && stamp.Epoch == rep.Epoch {
				if _, exists := positions[stamp.Lane]; exists {
					return nil, errors.New("relay stream is split across storage producers")
				}
				positions[stamp.Lane] = pos.Sequence
			}
		}
	}
	ends := make([]v2placement.EndMarker, len(rep.Producers))
	for i, lane := range rep.Producers {
		n := positions[lane]
		ends[i] = v2placement.EndMarker{Lane: lane, Final: n, Durable: n}
	}
	return ends, nil
}

// FenceRelayPlacement validates the relay's complete final census against the
// store's durable journal before closing intake and sealing actual core owners.
// certify still owns pinned source/target coverage; no socket event supplies it.
func (s *Store) FenceRelayPlacement(ctx context.Context, operation string, ends []v2placement.EndMarker, certify func([][]uint64) (v2placement.Roots, error)) (v2placement.Fence, error) {
	local, err := s.DurableRelayEnds()
	if err != nil {
		return v2placement.Fence{}, err
	}
	if len(local) != len(ends) {
		return v2placement.Fence{}, errors.New("incomplete relay final census")
	}
	seen := make(map[v2placement.Lane]bool)
	for _, end := range ends {
		if seen[end.Lane] {
			return v2placement.Fence{}, errors.New("duplicate relay final lane")
		}
		seen[end.Lane] = true
		found := false
		for _, known := range local {
			if end == known {
				found = true
				break
			}
		}
		if !found {
			return v2placement.Fence{}, errors.New("relay final differs from durable store position")
		}
	}
	if !s.relayActive.CompareAndSwap(false, true) {
		return v2placement.Fence{}, errors.New("receiver started during fence preflight")
	}
	defer s.relayActive.Store(false)
	return s.FencePlacement(ctx, operation, ends, certify)
}
