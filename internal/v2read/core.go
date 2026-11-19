package v2read

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
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

	reads  readCounters
	stages stageCounters
	// scanAllOwners is the test oracle: every owner sees every name.
	scanAllOwners bool
	// fullHistory is the range-resolve oracle: pending names resolve over the
	// whole sealed history instead of the request's touched slots.
	fullHistory bool

	closed    atomic.Bool
	owners    []atomic.Pointer[generation]
	caches    []sealedCache
	Budget    *Budget
	WorkBytes int64
	work      chan *sealrollup.Workspace
	// Now is fixed by the caller before intake; nil uses time.Now.
	Now func() time.Time
	// BlockCacheBytes sizes the sealed-part block cache that all owners
	// share; fixed before the first publication, zero uses the default.
	BlockCacheBytes int64
	blocksOnce      sync.Once
	blocks          *pebble.Cache
	closedBlocks    atomic.Bool
}

// DefaultBlockCacheBytes holds the index and data blocks a cold single-name
// read touches on every live part of its owner. A per-owner cache sized from
// the read budget (85 KiB at 96 owners) held no 256 KiB dense block, so every
// request decompressed one block per live part again.
const DefaultBlockCacheBytes = 512 << 20

// blockCache returns the shared cache. Shards stay at least 8 MiB so each
// holds whole dense blocks; Pebble's default would split it 2*GOMAXPROCS ways.
func (r *Reader) blockCache() *pebble.Cache {
	r.blocksOnce.Do(func() {
		n := r.BlockCacheBytes
		if n <= 0 {
			n = DefaultBlockCacheBytes
		}
		r.blocks = pebble.NewCacheWithShards(n, int(max(1, min(int64(2*runtime.GOMAXPROCS(0)), n/(8<<20)))))
	})
	return r.blocks
}

type generation struct {
	refs    atomic.Int64
	sealed  *sealedData
	pending []v2core.Batch
	index   *pendingIndex
	once    sync.Once
	metrics map[string]*metricState
	err     error
}
type sealedCache struct {
	ranges   *v2core.RollupReadCache
	loadGate chan struct{}
	mu       sync.Mutex // read-side only; Publish never acquires this lock
	index    *v2core.RollupSnapshot
	number   uint64
	limit    int64
}
type sealedData struct {
	parts     *v2core.OpenedRollupParts
	cache     *sealedCache
	index     *v2core.RollupSnapshot
	refs      atomic.Int64
	number    uint64
	reader    *Reader
	snapshot  *pebble.Snapshot
	inventory *v2core.RollupInventory
	once      sync.Once
	err       error
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
		r.caches[i].loadGate = make(chan struct{}, 1)
		r.caches[i].loadGate <- struct{}{}
		r.caches[i].limit = min(4<<20, budgetBytes/int64(owners)/32)
		r.caches[i].ranges = v2core.NewRollupReadCache(r.caches[i].limit)
	}
	return r, nil
}

// Publish is the first consumer of v2core.Hooks.PublishBatches. Ownership of
// snapshot and the detached batch arena transfers even when this returns an error.
// It does no archive scan or reduction on the writer. Owners serialize callbacks.
func (r *Reader) Publish(owner int, snapshot *pebble.Snapshot, pending []v2core.Batch, number uint64) error {
	return r.PublishWithInventory(owner, snapshot, pending, number, nil)
}

// PublishWithInventory transfers the core's coherent immutable binding set.
func (r *Reader) PublishWithInventory(owner int, snapshot *pebble.Snapshot, pending []v2core.Batch, number uint64, inventory *v2core.RollupInventory) error {
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
		g.index = nextPendingIndex(old.index, old.pending, pending)
		if old.sealed.number == number && old.sealed.inventory == inventory {
			g.sealed = old.sealed
			g.sealed.refs.Add(1)
		}
		if g.sealed == nil && inventory != nil {
			parts, err := v2core.OpenRollupPartsWithCache(context.Background(), snapshot, inventory, old.sealed.parts, r.Identities, r.blockCache(), min(4<<20, r.caches[owner].limit))
			if err != nil {
				old.release()
				_ = snapshot.Close()
				return err
			}
			g.sealed = &sealedData{parts: parts, snapshot: snapshot, inventory: inventory, number: number, cache: &r.caches[owner], reader: r}
			g.sealed.refs.Store(1)
		}
		old.release()
	}
	if g.index == nil {
		g.index = nextPendingIndex(nil, nil, pending)
	}
	if g.sealed != nil && g.sealed.snapshot != snapshot {
		_ = snapshot.Close()
	} else if g.sealed == nil {
		parts, err := v2core.OpenRollupPartsWithCache(context.Background(), snapshot, inventory, nil, r.Identities, r.blockCache(), min(4<<20, r.caches[owner].limit))
		if err != nil {
			_ = snapshot.Close()
			return err
		}
		g.sealed = &sealedData{parts: parts, snapshot: snapshot, inventory: inventory, number: number, cache: &r.caches[owner], reader: r}
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
			g.sealed.parts.Close()
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
	// Open parts hold their own references; this drops only the Reader's.
	r.blocksOnce.Do(func() {})
	if r.blocks != nil && r.closedBlocks.CompareAndSwap(false, true) {
		r.blocks.Unref()
	}
}
func (s *sealedData) load() {
	// The prior directory is immutable. Keep it usable while this generation
	// loads; sealedData.once coordinates only readers of this same generation.
	s.cache.mu.Lock()
	prior := s.cache.index
	s.cache.mu.Unlock()
	s.index, s.err = v2core.NewRollupSnapshotWithCatalogAndInventory(context.Background(), s.snapshot, prior, s.reader.Identities, s.inventory, s.cache.limit)
	if s.err != nil {
		return
	}
	s.cache.mu.Lock()
	// An older pinned generation may finish after a newer publication.
	if s.cache.index == nil || s.number >= s.cache.number {
		s.cache.index, s.cache.number = s.index, s.number
	}
	s.cache.mu.Unlock()
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
	g.loadPending()
}
func (g *generation) loadPending() { g.loadPendingContext(context.Background()) }
func (g *generation) loadPendingContext(ctx context.Context) {
	if g.sealed.err != nil {
		g.err = g.sealed.err
		return
	}
	g.metrics = make(map[string]*metricState)
	for _, b := range g.pending {
		if g.err = ctx.Err(); g.err != nil {
			return
		}
		if b.Registered != nil && len(b.Policy) == 0 {
			b.Policy = b.Registered.Definition()
		}
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
			if i%1024 == 0 {
				if g.err = ctx.Err(); g.err != nil {
					return
				}
			}
			batch.Points[i] = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Seq}
		}
		m.pending = append(m.pending, batch)
		m.meta.Expired = false
	}
}

// resolveContext reduces pending batches over the sealed slots the render
// window [from, until) at now and the reducer can observe.
func (m *metricState) resolveContext(ctx context.Context, from, until, now int64) {
	if m.err = ctx.Err(); m.err != nil {
		return
	}
	var base *resolvedSource
	switch {
	case m.sealed != nil && m.reader.fullHistory:
		var records []sealrollup.Record
		if records, m.err = m.resource.index.Records(ctx, m.name); m.err != nil {
			return
		}
		base = newResolvedSource(m.meta.ID, m.policy.Config.Archives, records)
	case m.sealed != nil:
		if base, m.err = m.resource.rangeBase(ctx, m, touchedRanges(m.policy.Config.Archives, m.pending, from, until, now)); m.err != nil {
			return
		}
	default:
		base = newResolvedSource(m.meta.ID, m.policy.Config.Archives, m.records)
	}
	if len(m.pending) == 0 {
		m.frozen = base
		return
	}
	batches, frontier := m.pending, m.meta.Frontier
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
		ordered[i].Points = orderSamples(ordered[i].Points)
	}
	plan, err := prepared.PlanWithWorkspace(frontier, base.view, ranges, ordered, workspace)
	if err != nil {
		m.err = err
		return
	}
	m.frozen = base.replace(plan.Records)
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
	stage := time.Now()
	for i := range r.owners {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
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
	r.stages.observe(StagePin, stage)
	stage = time.Now()
	views, errs := r.viewsForNames(ctx, pinned, names)
	r.stages.observe(StageViews, stage)
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	for _, name := range names {
		var metric *metricState
		for _, g := range views {
			if g == nil {
				continue
			}
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
			mask(metric.meta.BaselineOrder > 0 || metric.meta.Expired || metric.meta.ResetOrder > 0)
		}
		if metric.meta.Expired {
			return ErrNotFound
		}
		var source Source
		if len(metric.pending) > 0 || metric.sealed == nil {
			stage = time.Now()
			metric.resolveContext(ctx, from, until, now)
			r.stages.observe(StageResolve, stage)
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
		retained := 4 * metric.resource.cache.limit
		if metric.sealed != nil {
			retained += metric.sealed.RetainedBytes()
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
			stage := time.Now()
			s, e := h.Render(ctx, name, from, until, now)
			r.stages.observe(StageRender, stage)
			if e != nil {
				return e
			}
			stage = time.Now()
			defer func() { r.stages.observe(StageConsume, stage) }()
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

// viewsForNames resolves request views of independent owners concurrently.
// Each name is looked up only on the owner that holds it (OwnerFor, the same
// routing as intake); owners with no requested name get a nil view. An owner
// whose sealed data has no part directory enumerates its names, so it receives
// every name. Callers consume errs in owner order, as a sequential loop would.
func (r *Reader) viewsForNames(ctx context.Context, pinned []*generation, names []string) ([]*generation, []error) {
	views := make([]*generation, len(pinned))
	errs := make([]error, len(pinned))
	routed := splitByOwner(names, len(pinned))
	if r.scanAllOwners {
		for i := range routed {
			routed[i] = names
		}
	}
	var wg sync.WaitGroup
	for i, g := range pinned {
		if g == nil {
			continue
		}
		own := routed[i]
		if g.sealed.parts == nil {
			own = names
		}
		if len(own) == 0 {
			continue
		}
		wg.Go(func() { views[i], errs[i] = g.forNames(ctx, own, &r.stages) })
	}
	wg.Wait()
	return views, errs
}

// forNames creates request-local metadata; no request waits for a whole-store
// generation loader. The caller retains the publication through this call.
func (g *generation) forNames(ctx context.Context, names []string, st *stageCounters) (*generation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g.sealed.parts == nil {
		return g.discover(ctx)
	}
	stage := time.Now()
	index, err := g.sealed.parts.SnapshotForNames(ctx, g.sealed.snapshot, names, g.sealed.cache.ranges)
	st.observe(StageSnapshot, stage)
	if err != nil {
		return nil, err
	}
	stage = time.Now()
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[n] = true
	}
	pendingIdx := g.index
	if pendingIdx == nil {
		pendingIdx = nextPendingIndex(nil, nil, g.pending)
	}
	positions, err := pendingIdx.candidates(g.pending, names)
	if err != nil {
		return nil, err
	}
	var pending []v2core.Batch
	for _, i := range positions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, err := pendingName(&g.pending[i])
		if err != nil {
			return nil, err
		}
		if wanted[name] {
			pending = append(pending, g.pending[i])
		}
	}
	st.observe(StagePending, stage)
	q := &generation{pending: pending, sealed: &sealedData{index: index, reader: g.sealed.reader, cache: g.sealed.cache, number: g.sealed.number}}
	stage = time.Now()
	q.loadPendingContext(ctx)
	st.observe(StageLoadPending, stage)
	return q, q.err
}

// Discovery alone enumerates names. Serialize its incremental refresh per owner
// so overlapping generations never independently reload the same new parts.
func (g *generation) discover(ctx context.Context) (*generation, error) {
	s := g.sealed
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.cache.loadGate:
	}
	defer func() { s.cache.loadGate <- struct{}{} }()
	s.cache.mu.Lock()
	prior := s.cache.index
	s.cache.mu.Unlock()
	if s.parts != nil {
		ctx = v2core.WithOpenedRollupParts(ctx, s.parts)
	}
	index, err := v2core.NewRollupSnapshotWithCatalogAndInventory(ctx, s.snapshot, prior, s.reader.Identities, s.inventory, s.cache.limit)
	if err != nil {
		return nil, err
	}
	s.cache.mu.Lock()
	if s.cache.index == nil || s.number >= s.cache.number {
		s.cache.index, s.cache.number = index, s.number
	}
	s.cache.mu.Unlock()
	q := &generation{pending: g.pending, sealed: &sealedData{index: index, reader: s.reader, cache: s.cache, number: s.number}}
	q.loadPendingContext(ctx)
	return q, q.err
}
