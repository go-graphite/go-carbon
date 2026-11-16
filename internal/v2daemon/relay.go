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
	Recent   map[uint64]string
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
	if last.Recent == nil {
		last.Recent = make(map[uint64]string)
	}
	last.Recent[stamp.RelaySequence] = stamp.RelayDigest
	if stamp.RelaySequence > 256 {
		delete(last.Recent, stamp.RelaySequence-256)
	}
	last.Sequence = stamp.RelaySequence
	last.Digest = stamp.RelayDigest
	p.relayPositions[k] = last
	return nil
}

type relayReceiver struct {
	store    *Store
	locks    []sync.Mutex
	resolve  PolicyResolver
	document interface{ Document() v2placement.Document }
}

func (r *relayReceiver) AcceptPlaced(f v2wire.Frame) error {
	return r.AcceptPlacedBatch([]v2wire.Frame{f})
}

// AcceptPlacedBatch keeps a bounded retry window equal to the transport's
// maximum outstanding group. All ACKs follow one catalog/journal drain.
func (r *relayReceiver) AcceptPlacedBatch(frames []v2wire.Frame) (result error) {
	if len(frames) == 0 || len(frames) > 256 {
		return errors.New("invalid placed batch size")
	}
	first := frames[0].Stream
	for _, f := range frames {
		if e := f.Validate(); e != nil || f.Kind != v2wire.Data || f.Stream.Producer != first.Producer || f.Stream.Lane != first.Lane {
			return errors.New("invalid or mixed placed batch")
		}
	}
	h := uint64(14695981039346656037)
	for _, s := range []string{first.Producer, "/", first.Lane} {
		for i := range s {
			h = (h ^ uint64(s[i])) * 1099511628211
		}
	}
	i := int(h % uint64(len(r.locks)))
	r.locks[i].Lock()
	defer r.locks[i].Unlock()
	p := r.store.Producers[i]
	if p.failed != nil {
		return p.failed
	}
	// Staged positions are private until the entire valid prefix is durable.
	staged := make(map[v2placement.Stamp]relayPosition)
	stamps := make([]v2placement.Stamp, 0, len(frames))
	defer func() {
		if len(stamps) == 0 {
			return
		}
		if e := p.Drain(); e != nil {
			result = e
			return
		}
		for _, stamp := range stamps {
			if e := p.rememberRelay(stamp); e != nil {
				result = e
				return
			}
		}
	}()
	d := r.document.Document()
	digest := d.Digest()
	for _, f := range frames {
		stamp := v2placement.Stamp{Group: f.Stream.Group, Replica: f.Stream.Replica, MapDigest: f.Stream.MapDigest, Epoch: f.Stream.Epoch, Lane: v2placement.Lane{Producer: f.Stream.Producer, Stream: f.Stream.Lane}, RelaySequence: f.Sequence, RelayDigest: f.Digest()}
		scope := relayScope(stamp)
		last, ok := staged[scope]
		if !ok {
			last = p.relayPositions[scope]
		}
		if f.Sequence <= last.Sequence {
			digest := last.Recent[f.Sequence]
			if f.Sequence == last.Sequence {
				digest = last.Digest
			}
			if digest == "" || digest != stamp.RelayDigest {
				return errors.New("conflicting or expired relay retry")
			}
			continue
		}
		if last.Sequence == ^uint64(0) || f.Sequence != last.Sequence+1 {
			return errors.New("relay sequence gap")
		}
		selector, v, e := d.Routing.Select(f.Name)
		if e != nil {
			return e
		}
		rep, e := d.Lookup(selector.GroupID(), f.Stream.Replica)
		if e != nil {
			return e
		}
		if selector.GroupID() != f.Stream.Group || v != f.VShard || rep.Writer != f.Stream.Store || rep.Epoch != f.Stream.Epoch || digest != f.Stream.MapDigest || d.Revision != f.Stream.Revision {
			return v2placement.ErrStaleEpoch
		}
		app := Application{Name: f.Name, Order: p.applyOrder + 1, Clock: time.Now().Unix(), Points: []points.Point{{Value: math.Float64frombits(f.Bits), Timestamp: f.Timestamp}}, Sequence: []uint64{p.applyOrder + 1}}
		if r.resolve != nil {
			app.Policy, e = r.resolve(f.Name)
			if e != nil {
				return e
			}
		}
		if e = p.AppendApplicationsPlaced(stamp, []Application{app}); e != nil {
			return e
		}
		// Clone the small retry map on first mutation, keeping durable state private.
		if !ok {
			recent := make(map[uint64]string, len(last.Recent)+1)
			for n, digest := range last.Recent {
				recent[n] = digest
			}
			last.Recent = recent
		}
		last.Sequence = f.Sequence
		last.Digest = stamp.RelayDigest
		last.Recent[f.Sequence] = stamp.RelayDigest
		staged[scope] = last
		stamps = append(stamps, stamp)
	}
	return nil
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
