// Package partspublish prepares durable immutable-part publications. The owner
// commits Plan.Value in the same transaction as its data and journal frontier,
// then calls Plan.Commit. Readers never acquire the publisher's mutex.
package partspublish

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	pf "github.com/go-graphite/go-carbon/internal/partsformat"
	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

const CodecV1 = "fixed24-le-v1"

// Ref identifies an immutable, never-reused relative directory. Scope is the
// core codec's opaque placement-group/routing-vshard identity.
type Ref struct {
	Name       string
	Generation uint64
	Version    uint32
	Codec      string
	Scope      []byte
}

// Envelope is the only recovery authority. Catalog includes complete portable
// identities and policies, in the owner's format; numeric read IDs are rebuilt.
type Envelope struct {
	Version    uint32
	Generation uint64
	Catalog    json.RawMessage
	Frontier   []uint64
	Parts      []Ref
}

// DecodeCatalog builds a frozen catalog and resolves complete block keys using
// the core's codec. Resolve must reject keys outside ref.Scope or the catalog.
// The returned byte charge includes retained catalog and index allocations.
type DecodeCatalog func(json.RawMessage) (catalog pr.Catalog, resolve func(Ref, []byte) (uint64, error), retained int64, err error)

type Options struct {
	Root          string
	DecodeCatalog DecodeCatalog
	// Step is a fault-injection seam. Production callers leave it nil.
	Step func(string)
}

type Publisher struct {
	mu          sync.Mutex // owner lifecycle only, never acquired by a query
	options     Options
	publication pr.Publication
	current     Envelope
	currentView pr.View // non-owning descriptor of the committed publication
	parts       map[string]*part
	retired     []*part
	pending     *Plan
	closed      bool
	failed      bool
}

type part struct {
	mu         sync.Mutex
	ref        Ref
	source     *pr.BlockFileSource
	users      int
	persistent bool
}

func (s *part) retain() { s.mu.Lock(); s.users++; s.mu.Unlock() }
func (s *part) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users--
	if s.users == 0 {
		s.source.Close()
	}
}

func New(options Options) (*Publisher, error) {
	if options.Root == "" || options.DecodeCatalog == nil {
		return nil, errors.New("publisher requires root and catalog decoder")
	}
	if err := os.MkdirAll(options.Root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(options.Root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("publisher root is not a directory")
	}
	return &Publisher{options: options, parts: map[string]*part{}}, nil
}

func (p *Publisher) step(s string) {
	if p.options.Step != nil {
		p.options.Step(s)
	}
}

// Pin delegates directly to the atomic reader publication.
func (p *Publisher) Pin(b *pr.Budget, work int64) (*pr.Handle, error) {
	return p.publication.Pin(b, work)
}

// AcquireView transfers a separately pinned coherent view to another existing
// reader Publication. It is an owner-lifecycle operation, not a per-query call.
// Its Close shares the same part refcounts as ordinary readers and copy pins.
func (p *Publisher) AcquireView() (pr.View, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.current.Generation == 0 {
		return pr.View{}, pr.ErrClosed
	}
	v := p.currentView
	v.Frontier = slices.Clone(v.Frontier)
	v.Head = slices.Clone(v.Head)
	v.Parts = slices.Clone(v.Parts)
	v.Blocks = slices.Clone(v.Blocks)
	v.RetainedBytes += int64(len(v.Frontier)*8 + (len(v.Head)+len(v.Parts)+len(v.Blocks))*16)
	var held []*part
	for _, s := range p.parts {
		s.retain()
		held = append(held, s)
	}
	var once sync.Once
	v.Close = func() {
		once.Do(func() {
			for _, s := range held {
				s.release()
			}
		})
	}
	return v, nil
}

type Request struct {
	Envelope Envelope
	// Added contains authoritative archive replacements, already reduced at seal.
	// Existing active parts are retained by reference rather than rewritten.
	Added map[string][]pr.BlockData
}

type Plan struct {
	// Value is detached and immutable. Store it at an owner-reserved key in the
	// same atomic transaction as raw/archive SSTs and the owner's frontier.
	Value    []byte
	p        *Publisher
	envelope Envelope
	view     pr.View
	active   map[string]*part
	opened   []*part
	done     bool
}

func validRef(r Ref) bool {
	return r.Name != "" && r.Name != "." && r.Name != ".." && filepath.Base(r.Name) == r.Name && r.Version == 1 && r.Codec == CodecV1 && r.Generation > 0 && len(r.Scope) > 0
}
func equalRef(a, b Ref) bool {
	return a.Name == b.Name && a.Generation == b.Generation && a.Version == b.Version && a.Codec == b.Codec && bytes.Equal(a.Scope, b.Scope)
}

func cloneEnvelope(e Envelope) Envelope {
	e.Catalog = slices.Clone(e.Catalog)
	e.Frontier = slices.Clone(e.Frontier)
	e.Parts = slices.Clone(e.Parts)
	for i := range e.Parts {
		e.Parts[i].Scope = slices.Clone(e.Parts[i].Scope)
	}
	return e
}

func (p *Publisher) prepare(e Envelope) (*Plan, error) {
	if p.closed || p.failed {
		return nil, errors.New("publisher closed or requires restart")
	}
	if p.pending != nil {
		return nil, errors.New("publication already pending")
	}
	if e.Version != 1 || e.Generation <= p.current.Generation || len(e.Frontier) == 0 || !json.Valid(e.Catalog) {
		return nil, errors.New("invalid publication envelope")
	}
	if len(p.current.Frontier) > 0 {
		if len(e.Frontier) != len(p.current.Frontier) {
			return nil, errors.New("frontier shape changed")
		}
		for i, v := range e.Frontier {
			if v < p.current.Frontier[i] {
				return nil, errors.New("frontier regressed")
			}
		}
	}
	e = cloneEnvelope(e)
	value, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	if len(value) > 64<<20 {
		return nil, errors.New("publication envelope exceeds limit")
	}
	cat, resolve, charge, err := p.options.DecodeCatalog(e.Catalog)
	if err != nil {
		return nil, err
	}
	if cat == nil || resolve == nil || charge < 0 {
		return nil, errors.New("invalid decoded catalog")
	}
	plan := &Plan{p: p, Value: value, envelope: e, active: map[string]*part{}}
	var held []*part
	fail := func(err error) (*Plan, error) {
		for _, s := range held {
			s.release()
		}
		for _, s := range plan.opened {
			s.release()
		}
		return nil, err
	}
	view := pr.View{Generation: e.Generation, Frontier: slices.Clone(e.Frontier), Catalog: cat, RetainedBytes: charge + int64(len(value))}
	for _, r := range e.Parts {
		if !validRef(r) || r.Generation > e.Generation || plan.active[r.Name] != nil {
			return fail(errors.New("invalid or duplicate part reference"))
		}
		// Reopen on catalog changes: a source directory contains local read IDs.
		// Sharing is safe only when the catalog bytes (and thus remap) match.
		s := p.parts[r.Name]
		if s != nil && !equalRef(s.ref, r) {
			return fail(errors.New("immutable part name reused"))
		}
		if s != nil && !bytes.Equal(e.Catalog, p.current.Catalog) {
			s = nil
		}
		if s == nil {
			path := filepath.Join(p.options.Root, r.Name)
			info, er := os.Lstat(path)
			if er != nil {
				return fail(er)
			}
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return fail(errors.New("part is not a directory"))
			}
			src, er := pr.OpenBlockPartWithIDs(path, func(k []byte) (uint64, error) { return resolve(r, k) })
			if er != nil {
				return fail(er)
			}
			if src.Generation != r.Generation {
				src.Close()
				return fail(errors.New("part generation mismatch"))
			}
			if er = verifyPart(path, src, r, cat, resolve); er != nil {
				src.Close()
				return fail(er)
			}
			s = &part{ref: r, source: src, users: 1}
			plan.opened = append(plan.opened, s)
		}
		s.retain()
		held = append(held, s)
		plan.active[r.Name] = s
		view.Parts = append(view.Parts, s.source)
		view.RetainedBytes += s.source.RetainedBytes()
	}
	view.Close = func() {
		for _, s := range held {
			s.release()
		}
	}
	plan.view = view
	return plan, nil
}

// Verify every referenced payload before the durable frontier can advance or
// restart can reopen reads. OpenBlockPart already checks file/extent metadata;
// scanning each distinct physical archive checks all block CRCs as well.
func verifyPart(path string, src *pr.BlockFileSource, ref Ref, cat pr.Catalog, resolve func(Ref, []byte) (uint64, error)) error {
	f, err := os.Open(filepath.Join(path, "manifest.json"))
	if err != nil {
		return err
	}
	b, err := io.ReadAll(io.LimitReader(f, (16<<20)+1))
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if len(b) > 16<<20 {
		return errors.New("immutable manifest exceeds limit")
	}
	var m pr.BlockManifest
	if err = json.Unmarshal(b, &m); err != nil {
		return err
	}
	seen := map[[3]uint64]bool{}
	var partID pf.PartID
	for _, block := range m.Blocks {
		key, err := pf.Decode(block.Key)
		if err != nil {
			return err
		}
		prefix, err := pf.PartitionPrefixForGroup(key.Group, key.VShard, key.Partition)
		if err != nil {
			return err
		}
		if !bytes.Equal(prefix, ref.Scope) {
			return errors.New("part crosses portable scope")
		}
		if partID == (pf.PartID{}) {
			partID = key.Part
		}
		if key.Part == (pf.PartID{}) || key.Part != partID {
			return errors.New("part UUID missing or inconsistent")
		}
		id, err := resolve(ref, block.Key)
		if err != nil {
			return err
		}
		a := block.Archive
		metric, ok := cat.Lookup(string(key.Metric))
		if !ok || metric.ID != id || uint64(key.Archive) >= uint64(len(metric.Archives)) || metric.Archives[key.Archive] != a {
			return errors.New("block archive differs from catalog policy")
		}
		if a.Step <= 0 || a.Points <= 0 || a.Step > int64(^uint64(0)>>1)/a.Points {
			return errors.New("invalid archive span")
		}
		archiveKey := [3]uint64{id, uint64(a.Step), uint64(a.Points)}
		if seen[archiveKey] {
			continue
		}
		seen[archiveKey] = true
		if err = src.Scan(context.Background(), id, a, 0, a.Step*a.Points, func(pr.Sample) error { return nil }); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot holds the committed portable envelope and its exact file resources
// for whole-part copies. It must accompany any external database snapshot that
// retains this envelope. Close it when that snapshot/copy is released.
type Snapshot struct {
	Value   []byte
	once    sync.Once
	release func()
}

func (s *Snapshot) Close() { s.once.Do(s.release) }
func (p *Publisher) PinEnvelope() (*Snapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.current.Generation == 0 {
		return nil, pr.ErrClosed
	}
	value, err := json.Marshal(p.current)
	if err != nil {
		return nil, err
	}
	var held []*part
	for _, s := range p.parts {
		s.retain()
		held = append(held, s)
	}
	return &Snapshot{Value: value, release: func() {
		for _, s := range held {
			s.release()
		}
	}}, nil
}

// PlanSeal writes and syncs complete parts before preparing a frozen view. No
// recoverable references or live publication change until the owner commits.
func (p *Publisher) PlanSeal(request Request) (*Plan, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.failed || p.pending != nil {
		return nil, errors.New("publisher unavailable")
	}
	for name, blocks := range request.Added {
		var ref *Ref
		for i := range request.Envelope.Parts {
			r := &request.Envelope.Parts[i]
			if r.Name == name {
				ref = r
				break
			}
		}
		if ref == nil || !validRef(*ref) {
			return nil, errors.New("added part lacks valid reference")
		}
		stage, err := os.MkdirTemp(p.options.Root, ".publish-")
		if err != nil {
			return nil, err
		}
		path, err := pr.PublishBlockPart(stage, name, ref.Generation, blocks)
		if err != nil {
			_ = os.RemoveAll(stage)
			return nil, err
		}
		p.step("staged")
		dest := filepath.Join(p.options.Root, name)
		if _, err = os.Lstat(dest); !os.IsNotExist(err) {
			_ = os.RemoveAll(stage)
			if err == nil {
				return nil, os.ErrExist
			}
			return nil, err
		}
		if err = os.Rename(path, dest); err != nil {
			_ = os.RemoveAll(stage)
			return nil, err
		}
		_ = os.Remove(stage)
		f, err := os.Open(p.options.Root)
		if err != nil {
			return nil, err
		}
		if err = errors.Join(f.Sync(), f.Close()); err != nil {
			return nil, err
		}
	}
	p.step("synced")
	plan, err := p.prepare(request.Envelope)
	if err != nil {
		return nil, err
	}
	p.pending = plan
	return plan, nil
}

// Commit cannot fail: all validation/open work preceded the owner's durable
// transaction. The owner must call it exactly after successful atomic ingest.
func (plan *Plan) Commit() {
	p := plan.p
	p.mu.Lock()
	defer p.mu.Unlock()
	if plan.done {
		return
	}
	if p.pending != plan {
		panic("publication commit without pending plan")
	}
	p.step("committed")
	old := p.parts
	for _, s := range plan.active {
		s.mu.Lock()
		s.persistent = true
		s.mu.Unlock()
	}
	p.parts = plan.active
	p.current = plan.envelope
	p.currentView = plan.view
	p.pending = nil
	plan.done = true
	p.publication.Publish(plan.view)
	p.step("published")
	for name, s := range old {
		if plan.active[name] != s {
			s.mu.Lock()
			s.persistent = false
			s.mu.Unlock()
			s.release()
			p.retired = append(p.retired, s)
		}
	}
}

// Abort is only valid after the owner knows its atomic transaction failed.
// Ambiguous commit outcomes require restart/recovery, never another live seal.
func (plan *Plan) Abort() {
	p := plan.p
	p.mu.Lock()
	defer p.mu.Unlock()
	if plan.done {
		return
	}
	plan.done = true
	plan.view.Close()
	for _, s := range plan.opened {
		s.release()
	}
	p.pending = nil
	p.failed = true
}

// Recover accepts only the value read from the owner's committed envelope key.
// It never enumerates directories or promotes a higher orphan generation.
func (p *Publisher) Recover(value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.current.Generation != 0 {
		return errors.New("recover requires a fresh publisher")
	}
	if len(value) > 64<<20 {
		return errors.New("publication envelope exceeds limit")
	}
	var e Envelope
	if err := json.Unmarshal(value, &e); err != nil {
		return err
	}
	plan, err := p.prepare(e)
	if err != nil {
		return err
	}
	for _, s := range plan.active {
		s.mu.Lock()
		s.persistent = true
		s.mu.Unlock()
	}
	p.parts = plan.active
	p.current = plan.envelope
	p.currentView = plan.view
	p.publication.Publish(plan.view)
	return nil
}

// Collect deletes only parts removed by a durable commit in this process and
// drained of all pinned views. Recovery orphans need a separate reconciler.
func (p *Publisher) Collect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var keep []*part
	var result error
	for _, s := range p.retired {
		s.mu.Lock()
		if s.persistent || s.users != 0 {
			s.mu.Unlock()
			keep = append(keep, s)
			continue
		}
		// A remapped descriptor set may share this same physical directory.
		if p.parts[s.ref.Name] != nil {
			s.mu.Unlock()
			continue
		}
		// Catalog remaps open distinct descriptor sets for one physical part.
		// Every such set participates in its lifetime, including old views.
		pinned := false
		for _, other := range p.retired {
			if other == s || other.ref.Name != s.ref.Name {
				continue
			}
			other.mu.Lock()
			pinned = pinned || other.users != 0 || other.persistent
			other.mu.Unlock()
		}
		if pinned {
			s.mu.Unlock()
			keep = append(keep, s)
			continue
		}
		p.step("retiring")
		err := os.RemoveAll(filepath.Join(p.options.Root, s.ref.Name))
		s.mu.Unlock()
		if err != nil {
			result = errors.Join(result, err)
			keep = append(keep, s)
		}
	}
	p.retired = keep
	if result == nil {
		f, err := os.Open(p.options.Root)
		if err != nil {
			return err
		}
		result = errors.Join(f.Sync(), f.Close())
	}
	return result
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if plan := p.pending; plan != nil {
		plan.done = true
		plan.view.Close()
		for _, s := range plan.opened {
			s.release()
		}
		p.pending = nil
	}
	p.publication.Close()
	for _, s := range p.parts {
		s.release()
	}
	p.parts = nil
	p.currentView = pr.View{}
}

func (r Ref) String() string { return fmt.Sprintf("%s@%d/%s", r.Name, r.Generation, r.Codec) }
