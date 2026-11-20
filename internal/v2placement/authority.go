package v2placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var ErrStaleEpoch = errors.New("stale placement epoch or map")
var ErrUnversioned = errors.New("placement stamp required")

// Stamp is immutable admission metadata. Receivers must authenticate Lane;
// this contract does not treat a caller-provided producer name as authentication.
type Stamp struct {
	RelaySequence uint64 `json:"relay_sequence,omitempty"`
	RelayDigest   string `json:"relay_digest,omitempty"`
	Group         string `json:"group"`
	Replica       string `json:"replica"`
	MapDigest     string `json:"map_digest"`
	Epoch         uint64 `json:"epoch"`
	Lane          Lane   `json:"lane"`
}

// ReplayHistory retains previously admitted maps and their validated successor
// receipts for journal recovery only. It never reopens live old-epoch intake.
type ReplayHistory struct {
	Document    Document     `json:"document"`
	Activations []Activation `json:"activations"`
}
type durableState struct {
	History     []ReplayHistory `json:"replay_history,omitempty"`
	Group       string          `json:"group"`
	Replica     string          `json:"replica"`
	Store       string          `json:"store"`
	Claim       string          `json:"claim"`
	Document    Document        `json:"document"`
	ClosedEpoch uint64          `json:"closed_epoch"`
	Fence       *Fence          `json:"fence,omitempty"`
	Activations []Activation    `json:"activations"`
}
type admissionState struct {
	durableState
	blocked      bool
	mapDigest    string
	replicaState Replica
}

// Authority is scoped to one PG/replica/store incarnation. Control operations
// serialize; intake uses only a producer-local atomic counter and immutable map.
// One authority must have one owner process; the daemon owns the exclusive root.
type Authority struct {
	control                     sync.Mutex
	current                     atomic.Pointer[admissionState]
	path, group, replica, store string
	lanes                       []*Admission
}

// Admission belongs to exactly one serial sink. Register before starting intake.
// Sink is the admission interface consumed by store adapters.
type Sink interface{ Begin(Stamp) (func(), error) }

type Admission struct {
	authority *Authority
	active    atomic.Int64
}

func OpenAuthority(root string, d Document, group, replica, store string) (*Authority, error) {
	if e := d.Validate(); e != nil {
		return nil, e
	}
	if _, e := d.Lookup(group, replica); e != nil {
		return nil, e
	}
	found := false
	for _, s := range d.Stores {
		found = found || s.ID == store && !s.Blackhole
	}
	if !found {
		return nil, fmt.Errorf("unknown live store incarnation")
	}
	a := &Authority{path: filepath.Join(root, "placement.json"), group: group, replica: replica, store: store}
	state := durableState{Document: cloneDocument(d), Group: group, Replica: replica, Store: store}
	b, e := os.ReadFile(a.path)
	switch {
	case e == nil:
		if e = uniqueFields(b); e != nil {
			return nil, e
		}
		dec := json.NewDecoder(bytesReader(b))
		dec.DisallowUnknownFields()
		if e = dec.Decode(&state); e != nil {
			return nil, e
		}
		if state.Group != group || state.Replica != replica || state.Store != store {
			return nil, fmt.Errorf("persisted authority scope differs")
		}
		if e = state.Document.Validate(); e != nil {
			return nil, e
		}
		for i, h := range state.History {
			successor := state.Document
			if i+1 < len(state.History) {
				successor = state.History[i+1].Document
			}
			if e = h.Document.ValidateTransition(successor, h.Activations); e != nil {
				return nil, fmt.Errorf("invalid replay authority history: %w", e)
			}
		}
		if state.Document.Digest() != d.Digest() {
			return nil, fmt.Errorf("persisted placement differs: need %s", state.Document.Digest())
		}
		r, _ := d.Lookup(group, replica)
		if (state.Claim != "" && state.ClosedEpoch == 0) || (state.Fence != nil && (state.Fence.Epoch > state.ClosedEpoch || state.Fence.Store != store || state.Fence.Group != group || state.Fence.Replica != replica)) {
			return nil, fmt.Errorf("invalid persisted fence floor")
		}
		if state.ClosedEpoch > r.Epoch {
			return nil, fmt.Errorf("fence floor ahead of map")
		}
	case errors.Is(e, os.ErrNotExist):
		if d.Revision != 1 {
			return nil, fmt.Errorf("new authority must bootstrap at revision one")
		}
		if e = os.MkdirAll(root, 0700); e != nil {
			return nil, e
		}
		if e = a.save(state); e != nil {
			return nil, e
		}
	default:
		return nil, e
	}
	a.publish(state, false)
	return a, nil
}
func (a *Authority) Register() Sink {
	a.control.Lock()
	defer a.control.Unlock()
	l := &Admission{authority: a}
	a.lanes = append(a.lanes, l)
	return l
}
func (a *Authority) Document() Document { return cloneDocument(a.current.Load().Document) }
func cloneDocument(d Document) Document {
	b, _ := json.Marshal(d)
	var out Document
	_ = json.Unmarshal(b, &out)
	return out
}

// Begin protects a whole batch, including journal/core admission, against fence.
// release must execute even on failure. No duplicate ACK is manufactured here.
func (l *Admission) Begin(s Stamp) (func(), error) {
	a := l.authority
	state := a.current.Load()
	l.active.Add(1)
	release := func() { l.active.Add(-1) }
	if state != a.current.Load() || state.blocked {
		release()
		return nil, ErrStaleEpoch
	}
	r := state.replicaState
	if s.Group != a.group || s.Replica != a.replica || s.MapDigest != state.mapDigest || s.Epoch != r.Epoch || r.Writer != a.store || s.Epoch <= state.ClosedEpoch {
		release()
		return nil, ErrStaleEpoch
	}
	found := false
	for _, lane := range r.Producers {
		found = found || lane == s.Lane
	}
	if !found {
		release()
		return nil, fmt.Errorf("unregistered producer lane")
	}
	return release, nil
}
func (a *Authority) CheckNames(names []string) error {
	d := a.current.Load().Document
	for _, name := range names {
		s, _, e := d.Routing.Select(name)
		if e != nil {
			return e
		}
		if s.GroupID() != a.group {
			return fmt.Errorf("name outside placement group")
		}
	}
	return nil
}

// Replay authorizes an already admitted durable journal record. A closed epoch
// can recover its own accepted prefix without reopening intake. The original
// map, epoch, writer incarnation and authenticated lane must still match the
// persisted authority. Historical maps require separate certified coverage and
// are deliberately refused here rather than restamping an old record.
func (a *Authority) Replay(s Stamp) error {
	state := a.current.Load()
	permits := func(d Document) bool {
		if s.Group != a.group || s.Replica != a.replica || s.MapDigest != d.Digest() {
			return false
		}
		r, e := d.Lookup(a.group, a.replica)
		if e != nil || s.Epoch != r.Epoch || r.Writer != a.store {
			return false
		}
		for _, lane := range r.Producers {
			if lane == s.Lane {
				return true
			}
		}
		return false
	}
	if permits(state.Document) {
		return nil
	}
	for _, h := range state.History {
		if permits(h.Document) {
			return nil
		}
	}
	return ErrStaleEpoch
}
func (a *Authority) save(s durableState) error {
	b, e := json.Marshal(s)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(a.path), ".placement-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, a.path); e != nil {
		return e
	}
	dir, e := os.Open(filepath.Dir(a.path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}

// Fence closes intake durably before waiting for active batches. Interrupted or
// failed fencing stays closed across restart. seal must drain accepted journals
// and synchronously seal every owner; its returned roots/frontier are attested
// by the storage adapter, not inferred from connection state or a timeout.
func (a *Authority) Fence(ctx context.Context, operation string, ends []EndMarker, seal func() (Frontier, Roots, error)) (Fence, error) {
	a.control.Lock()
	defer a.control.Unlock()
	state := a.current.Load()
	d := state.Document
	r, _ := d.Lookup(a.group, a.replica)
	f := Fence{Group: a.group, Replica: a.replica, Store: a.store, Epoch: r.Epoch, MapDigest: d.Digest(), Operation: operation, WriterSet: digest(r.Producers), Ends: ends}
	// Validate the census before closing intake. Root/frontier are supplied later.
	f.Frontier = Frontier{{Owner: "preflight"}}
	h := digest("preflight")
	f.Roots = Roots{h, h, h, h}
	if e := f.Validate(d); e != nil {
		return Fence{}, e
	}
	if state.Claim != "" && state.ClosedEpoch == r.Epoch && state.Claim != operation {
		return Fence{}, fmt.Errorf("fence already claimed")
	}
	if state.Fence != nil && state.Fence.Epoch == r.Epoch {
		if state.Fence.Operation != operation {
			return Fence{}, fmt.Errorf("fence already claimed")
		}
		return cloneFence(*state.Fence), nil
	}
	closing := state.durableState
	closing.ClosedEpoch = r.Epoch
	closing.Claim = operation
	a.publish(closing, true)
	if e := a.save(closing); e != nil {
		return Fence{}, e
	}
	for {
		busy := false
		for _, l := range a.lanes {
			busy = busy || l.active.Load() != 0
		}
		if !busy {
			break
		}
		select {
		case <-ctx.Done():
			return Fence{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	if seal == nil {
		return Fence{}, fmt.Errorf("durable seal callback required")
	}
	var e error
	f.Frontier, f.Roots, e = seal()
	if e != nil {
		return Fence{}, e
	}
	if e = f.Validate(d); e != nil {
		return Fence{}, e
	}
	closing.Fence = &f
	if e = a.save(closing); e != nil {
		return Fence{}, e
	}
	a.publish(closing, false)
	return cloneFence(f), nil
}

// Adopt blocks admissions until the successor and its certificates are synced.
// Failure leaves intake blocked in this process; persisted fences never regress.
func (a *Authority) Adopt(next Document, receipts []Activation) error {
	a.control.Lock()
	defer a.control.Unlock()
	state := a.current.Load()
	if next.Digest() == state.Document.Digest() {
		return nil
	}
	if e := state.Document.ValidateTransition(next, receipts); e != nil {
		return e
	}
	oldReplica, _ := state.Document.Lookup(a.group, a.replica)
	nextReplica, _ := next.Lookup(a.group, a.replica)
	if oldReplica.Writer == a.store && nextReplica.Epoch != oldReplica.Epoch {
		found := false
		for _, receipt := range receipts {
			if receipt.Fence.Group == a.group && receipt.Fence.Replica == a.replica && state.Fence != nil && receipt.Fence.Digest() == state.Fence.Digest() {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("local writer requires its own durable fence before adoption")
		}
	}
	a.publish(state.durableState, true)
	// Caller must quiesce before adoption; reject rather than racing a live batch.
	for _, l := range a.lanes {
		if l.active.Load() != 0 {
			return fmt.Errorf("placement adoption requires quiescent sinks")
		}
	}
	n := state.durableState
	n.History = append(append([]ReplayHistory(nil), n.History...), ReplayHistory{Document: state.Document, Activations: append([]Activation(nil), receipts...)})
	n.Document = cloneDocument(next)
	n.Activations = append(append([]Activation(nil), n.Activations...), receipts...)
	if e := a.save(n); e != nil {
		return e
	}
	a.publish(n, false)
	return nil
}

func (a *Authority) publish(s durableState, blocked bool) {
	b, _ := json.Marshal(s)
	var detached durableState
	_ = json.Unmarshal(b, &detached)
	s = detached
	r, _ := s.Document.Lookup(a.group, a.replica)
	a.current.Store(&admissionState{durableState: s, blocked: blocked, mapDigest: s.Document.Digest(), replicaState: r})
}

func cloneFence(f Fence) Fence {
	b, _ := json.Marshal(f)
	var out Fence
	_ = json.Unmarshal(b, &out)
	return out
}

// Scope is the immutable journal authority identity, independent of who owns
// the current epoch after a move.
func (a *Authority) Scope() (group, replica, store string) { return a.group, a.replica, a.store }
