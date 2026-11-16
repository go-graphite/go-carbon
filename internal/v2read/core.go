package v2read

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

// Reader publishes independent owner generations. FetchMany pins every owner
// once, so all names on an owner see the same sealed snapshot and pending tail.
// Cross-owner snapshots have independent frontiers, as does the core itself.
// Stop producers, close Reader, wait for Fetch consumers, then close the engine.
// The engine owns the underlying Pebble databases; Close rejects later handoffs.
type Reader struct {
	// Identities is fixed before opening the engine or publishing snapshots.
	Identities *v2core.PolicyCatalog

	reads readCounters

	closed    atomic.Bool
	owners    []atomic.Pointer[generation]
	caches    []sealedCache
	Budget    *Budget
	WorkBytes int64
	work      chan *sealrollup.Workspace
	// Now is fixed by the caller before intake; nil uses time.Now.
	Now func() time.Time
}
type generation struct {
	refs    atomic.Int64
	sealed  *sealedData
	pending []v2core.Batch
	once    sync.Once
	metrics map[string]*metricState
	err     error
}
type sealedCache struct {
	mu             sync.Mutex // read-side only; Publish never acquires this lock
	index          *v2core.RollupSnapshot
	limit          int64
	fullMu         sync.Mutex
	full           []resolvedEntry
	fullBytes      int64
	pendingMu      sync.Mutex
	reductions     []pendingReduction
	reductionBytes int64
	orderMu        sync.Mutex
	ordered        []orderedSamples
	orderedBytes   int64
}
type sealedData struct {
	cache    *sealedCache
	index    *v2core.RollupSnapshot
	refs     atomic.Int64
	number   uint64
	reader   *Reader
	snapshot *pebble.Snapshot
	once     sync.Once
	err      error
}

type metricState struct {
	sealed   *v2core.RollupSnapshot
	name     string
	policy   v2core.RollupPolicy
	meta     v2core.RollupMetadata
	records  []sealrollup.Record
	pending  []sealrollup.Batch
	once     sync.Once
	frozen   *resolvedSource
	reader   *Reader
	err      error
	resource *sealedData
}

func New(owners int, budgetBytes, workBytes int64) (*Reader, error) {
	if owners < 1 || budgetBytes < 1 || workBytes < 0 {
		return nil, errors.New("invalid read geometry/budget")
	}
	r := &Reader{owners: make([]atomic.Pointer[generation], owners), caches: make([]sealedCache, owners), Budget: NewBudget(budgetBytes), WorkBytes: workBytes, work: make(chan *sealrollup.Workspace, owners)}
	for i := range r.caches {
		r.caches[i].limit = min(4<<20, budgetBytes/int64(owners)/32)
	}
	return r, nil
}

// Publish is the first consumer of v2core.Hooks.PublishBatches. Ownership of
// snapshot and the detached batch arena transfers even when this returns an error.
// It does no archive scan or reduction on the writer. Owners serialize callbacks.
func (r *Reader) Publish(owner int, snapshot *pebble.Snapshot, pending []v2core.Batch, number uint64) error {
	if r.closed.Load() {
		return snapshot.Close()
	}
	if owner < 0 || owner >= len(r.owners) {
		_ = snapshot.Close()
		return errors.New("invalid publication owner")
	}
	g := &generation{pending: pending}
	// Retain the previous publication while borrowing its sealed index. Close
	// may race publication; a failed retain means a fresh sealed resource is needed.
	old := r.owners[owner].Load()
	if old != nil && old.retain() {
		if old.sealed.number == number {
			g.sealed = old.sealed
			g.sealed.refs.Add(1)
		}
		old.release()
	}
	if g.sealed != nil {
		_ = snapshot.Close()
	} else {
		g.sealed = &sealedData{snapshot: snapshot, number: number, cache: &r.caches[owner], reader: r}
		g.sealed.refs.Store(1)
	}
	g.refs.Store(1)
	if old := r.owners[owner].Swap(g); old != nil {
		old.release()
	}
	if r.closed.Load() {
		if old := r.owners[owner].Swap(nil); old != nil {
			old.release()
		}
	}
	return nil
}
func (g *generation) release() {
	if g.refs.Add(-1) == 0 {
		if g.sealed.refs.Add(-1) == 0 {
			_ = g.sealed.snapshot.Close()
		}
	}
}
func (g *generation) retain() bool {
	for n := g.refs.Load(); n > 0; n = g.refs.Load() {
		if g.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
	return false
}
func (r *Reader) Close() {
	r.closed.Store(true)
	for i := range r.owners {
		if g := r.owners[i].Swap(nil); g != nil {
			g.release()
		}
	}
}
func (s *sealedData) load() {
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	s.index, s.err = v2core.NewRollupSnapshotWithCatalog(context.Background(), s.snapshot, s.cache.index, s.reader.Identities, s.cache.limit)
	if s.err != nil {
		return
	}
	s.cache.index = s.index
}
func (s *sealedData) lookup(name string) *metricState {
	p, meta, ok := s.index.LookupMetadata(name)
	if !ok {
		return nil
	}
	// Keep request state local; decoded sources and reductions already use
	// the bounded sealedCache. A name sweep must not grow another permanent map.
	return &metricState{policy: p, meta: meta, sealed: s.index, name: name, reader: s.reader, resource: s}
}

func (g *generation) load() {
	g.sealed.once.Do(g.sealed.load)
	if g.sealed.err != nil {
		g.err = g.sealed.err
		return
	}
	g.metrics = make(map[string]*metricState)
	for _, b := range g.pending {
		var p v2core.RollupPolicy
		if g.err = json.Unmarshal(b.Policy, &p); g.err != nil {
			return
		}
		canonical, err := v2core.EncodeRollupPolicy(p)
		if err != nil {
			g.err = err
			return
		}
		if !bytes.Equal(canonical, b.Policy) {
			g.err = errors.New("noncanonical pending policy")
			return
		}
		name := string(p.Name)
		m := g.metrics[name]
		if m == nil {
			if base := g.sealed.lookup(name); base != nil {
				m = &metricState{policy: base.policy, meta: base.meta, sealed: base.sealed, name: name, reader: g.sealed.reader, resource: g.sealed}
				g.metrics[name] = m
			}
		}
		if m == nil {
			m = &metricState{name: name, policy: p, meta: v2core.RollupMetadata{ID: b.ID, Policy: canonical}, reader: g.sealed.reader, resource: g.sealed}
			g.metrics[name] = m
		}
		if m.meta.ID != b.ID || !bytes.Equal(m.meta.Policy, b.Policy) {
			g.err = errors.New("pending identity/policy mismatch")
			return
		}
		if b.Order <= m.meta.Frontier.Order {
			g.err = errors.New("pending batch overlaps sealed frontier")
			return
		}
		batch := sealrollup.Batch{Order: b.Order, Now: b.Clock, Points: make([]sealrollup.Sample, len(b.Points))}
		for i, p := range b.Points {
			batch.Points[i] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		}
		m.pending = append(m.pending, batch)
		m.meta.Expired = false
	}
}
func (m *metricState) resolve() {
	var base *resolvedSource
	if m.sealed != nil {
		base, m.err = m.resource.resolved(m.name, m.policy, m.meta)
		if m.err != nil {
			return
		}
	} else {
		base = newResolvedSource(m.meta.ID, m.policy.Config.Archives, m.records)
	}
	if len(m.pending) == 0 {
		m.frozen = base
		return
	}
	batches, frontier := m.pending, m.meta.Frontier
	if cached := m.resource.cachedReduction(m); cached != nil {
		base, frontier = cached.source, cached.frontier
		batches = batches[len(cached.batches):]
		if len(batches) == 0 {
			m.frozen = base
			return
		}
	}
	prepared, err := sealrollup.Prepare(m.policy.Config)
	if err != nil {
		m.err = err
		return
	}
	var workspace *sealrollup.Workspace
	select {
	case workspace = <-m.reader.work:
	default:
		workspace = new(sealrollup.Workspace)
	}
	defer func() {
		if workspace.RetainedBytes() <= m.reader.WorkBytes {
			select {
			case m.reader.work <- workspace:
			default:
			}
		}
	}()
	// Direct physical lookups avoid repeatedly scanning every changed slot for
	// small fan-in buckets. Both paths retain the canonical reducer and summation
	// order; wide sparse buckets keep its bounded range reader.
	var ranges sealrollup.RangeView = base.rangeSamples
	direct := true
	for i := 1; i < len(base.archives); i++ {
		if base.archives[i].Step/base.archives[i-1].Step > 1024 {
			direct = false
		}
	}
	if direct {
		ranges = nil
	}
	// Order only this requested name's new boundaries; unrequested pending
	// metrics never pay stable-sort work during generation materialization.
	ordered := make([]sealrollup.Batch, len(batches))
	copy(ordered, batches)
	for i := range ordered {
		ordered[i].Points = m.resource.cache.orderSamples(ordered[i].Points)
	}
	plan, err := prepared.PlanWithWorkspace(frontier, base.view, ranges, ordered, workspace)
	if err != nil {
		m.err = err
		return
	}
	m.frozen = base.replace(plan.Records)
	m.resource.rememberReduction(m, plan.Next)
}

// FetchMany adapts the carbonserver fetch boundary for an already expanded set
// of names. The callback runs while all owner generations and byte reservations
// are pinned. Values are read-only and valid until the callback returns.
// now is explicit so all names share one archive-selection clock.
func (r *Reader) FetchMany(ctx context.Context, names []string, from, until, now int64, consume func(string, *Series) error) error {
	return r.fetchMany(ctx, names, from, until, now, consume, nil)
}

func (r *Reader) fetchMany(ctx context.Context, names []string, from, until, now int64, consume func(string, *Series) error, mask func(bool)) error {
	started := time.Now()
	defer func() { r.reads.observe(time.Since(started)) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	pinned := make([]*generation, len(r.owners))
	defer func() {
		for _, g := range pinned {
			if g != nil {
				g.release()
			}
		}
	}()
	for i := range r.owners {
		for {
			g := r.owners[i].Load()
			if g == nil {
				break
			}
			if g.retain() {
				pinned[i] = g
				break
			}
		}
	}
	for _, name := range names {
		var metric *metricState
		for _, g := range pinned {
			if g == nil {
				continue
			}
			g.once.Do(g.load)
			if g.err != nil {
				return g.err
			}
			m := g.metrics[name]
			if m == nil {
				m = g.sealed.lookup(name)
			}
			if m != nil {
				if metric != nil {
					return fmt.Errorf("multiple owners for %q", name)
				}
				metric = m
			}
		}
		if metric == nil {
			return ErrNotFound
		}
		if mask != nil {
			mask(metric.meta.Expired || metric.meta.ResetOrder > 0)
		}
		if metric.meta.Expired {
			return ErrNotFound
		}
		var source Source
		if len(metric.pending) > 0 || metric.sealed == nil {
			metric.once.Do(metric.resolve)
			if metric.err != nil {
				return metric.err
			}
			source = metric.frozen
		} else {
			source = &sealedSource{metric: metric}
		}
		archives := make([]Archive, len(metric.policy.Config.Archives))
		for i, a := range metric.policy.Config.Archives {
			archives[i] = Archive{Step: a.Step, Points: a.Count}
		}
		m := Metric{ID: metric.meta.ID, Archives: archives, XFilesFactor: metric.policy.Config.XFilesFactor, AggregationMethod: aggregationName(metric.policy.Config.Method)}
		catalog := CatalogFuncs{LookupFunc: func(n string) (Metric, bool) { return m, n == name }}
		var p Publication
		retained := int64(0)
		if metric.sealed != nil {
			retained = metric.sealed.RetainedBytes() + 3*metric.resource.cache.limit
		}
		if metric.frozen != nil {
			retained += metric.frozen.RetainedBytes()
		}
		p.Publish(View{Catalog: catalog, Blocks: []Source{source}, RetainedBytes: retained})
		h, err := p.Pin(r.Budget, r.WorkBytes)
		p.Close()
		if err != nil {
			return err
		}
		err = func() error {
			defer h.Close()
			s, e := h.Render(ctx, name, from, until, now)
			if e != nil {
				return e
			}
			return consume(name, s)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func aggregationName(method sealrollup.Method) string {
	switch method {
	case sealrollup.Average:
		return "average"
	case sealrollup.Sum:
		return "sum"
	case sealrollup.Last:
		return "last"
	case sealrollup.Max:
		return "max"
	case sealrollup.Min:
		return "min"
	case sealrollup.First:
		return "first"
	}
	return ""
}
