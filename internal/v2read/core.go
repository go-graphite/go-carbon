package v2read

import (
	"context"
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

	closed atomic.Bool
	owners []atomic.Pointer[generation]
	caches []sealedCache
	// renders admits budgetBytes/WorkBytes concurrent renders; each holds at
	// most WorkBytes (32B per slot) until its consumer returns.
	renders   chan struct{}
	WorkBytes int64
	work      chan *sealrollup.Workspace
	// Now is fixed by the caller before intake; nil uses time.Now.
	Now func() time.Time
	// BlockCacheBytes sizes the sealed-part block cache that all owners
	// share; fixed before the first publication, zero uses the default.
	BlockCacheBytes int64
	blocksOnce      sync.Once
	// cacheMu orders block cache references against Close: publications
	// re-check closed and open parts under the read side; Close drops the
	// Reader's own reference under the write side.
	cacheMu sync.RWMutex
	blocks  *pebble.Cache
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

// generation is one owner publication: its own registry references on the
// sealed parts plus the pending tail. Request views copy the sealed fields
// with a request-local directory and own no references.
type generation struct {
	refs  atomic.Int64
	parts *v2core.OpenedRollupParts
	// snapshot stays open only while parts.NeedsSnapshot(). A head-only
	// publication shares it through base, the publication that owns it.
	snapshot  *pebble.Snapshot
	base      *generation
	inventory *v2core.RollupInventory
	number    uint64
	cache     *sealedCache
	reader    *Reader
	sealed    *v2core.RollupSnapshot // request views only
	pending   []v2core.Batch
	view      v2core.PendingView
	metrics   map[string]*metricState
	err       error
}
type sealedCache struct {
	ranges   *v2core.RollupReadCache
	loadGate chan struct{}
	mu       sync.Mutex // read-side only; Publish never acquires this lock
	index    *v2core.RollupSnapshot
	number   uint64
	limit    int64
}
type metricState struct {
	sealed   *v2core.RollupSnapshot
	name     string
	identity *v2core.ApplicationPolicy // last pending handle checked against meta
	policy   v2core.RollupPolicy
	meta     v2core.RollupMetadata
	records  []sealrollup.Record
	pending  []sealrollup.Batch
	frozen   *resolvedSource
	reader   *Reader
	err      error
	resource *generation
}

func New(owners int, budgetBytes, workBytes int64) (*Reader, error) {
	if owners < 1 || budgetBytes < 1 || workBytes < 0 {
		return nil, errors.New("invalid read geometry/budget")
	}
	r := &Reader{owners: make([]atomic.Pointer[generation], owners), caches: make([]sealedCache, owners), renders: make(chan struct{}, budgetBytes/max(workBytes, 1)), WorkBytes: workBytes, work: make(chan *sealrollup.Workspace, owners)}
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
// A nil snapshot is a head-only publication: number and inventory equal the
// current generation's, whose parts it takes its own references on.
func (r *Reader) PublishWithInventory(owner int, snapshot *pebble.Snapshot, pending []v2core.Batch, number uint64, inventory *v2core.RollupInventory) error {
	return r.PublishPending(owner, snapshot, v2core.PendingBatches(pending), number, inventory)
}

// PublishPending transfers an immutable owner publication and its sealed parts.
// Head-only publications retain their own references on the sealed parts.
func (r *Reader) PublishPending(owner int, snapshot *pebble.Snapshot, pending v2core.PendingView, number uint64, inventory *v2core.RollupInventory) error {
	closeSnapshot := func() error {
		if snapshot == nil {
			return nil
		}
		return snapshot.Close()
	}
	if r.closed.Load() {
		return closeSnapshot()
	}
	if owner < 0 || owner >= len(r.owners) {
		_ = closeSnapshot()
		return errors.New("invalid publication owner")
	}
	g := &generation{view: pending, number: number, inventory: inventory, cache: &r.caches[owner], reader: r}
	// Retain the previous publication while sharing its sealed parts. Close
	// may race publication; a failed retain means fresh parts are needed.
	// The owner's part registry, carried by the inventory, shares opened parts
	// with the previous publication, which stays installed until the swap.
	shared := false
	old := r.owners[owner].Load()
	if old != nil && old.retain() {
		if shared = old.number == number && old.inventory == inventory; shared {
			g.parts = old.parts.Retain()
			if g.snapshot = old.snapshot; g.snapshot != nil {
				g.base = old.base
				if g.base == nil {
					g.base = old
				}
				g.base.refs.Add(1)
			}
		}
		old.release()
	}
	if !shared && snapshot == nil {
		if r.closed.Load() {
			return nil
		}
		return errors.New("head-only publication without a pinned sealed generation")
	}
	if shared && g.snapshot != snapshot {
		_ = closeSnapshot()
	} else if !shared {
		// A publication past the first closed check must not take a block
		// cache reference after Close dropped the Reader's last one.
		r.cacheMu.RLock()
		if r.closed.Load() {
			r.cacheMu.RUnlock()
			return closeSnapshot()
		}
		parts, err := v2core.OpenRollupParts(context.Background(), snapshot, inventory, r.Identities, r.blockCache(), min(4<<20, r.caches[owner].limit))
		r.cacheMu.RUnlock()
		if err != nil {
			_ = snapshot.Close()
			return err
		}
		g.parts, g.snapshot = parts, snapshot
		if !parts.NeedsSnapshot() {
			// Registry references keep the part files; the GC fence no
			// longer waits for this snapshot.
			_ = snapshot.Close()
			g.snapshot = nil
		}
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
	if g.refs.Add(-1) != 0 {
		return
	}
	g.parts.Close()
	if g.base != nil {
		g.base.release()
	} else if g.snapshot != nil {
		_ = g.snapshot.Close()
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
	// Publications see closed before they open parts, so none reaches the
	// cache after this.
	r.cacheMu.Lock()
	if r.blocks != nil {
		r.blocks.Unref()
		r.blocks = nil
	}
	r.cacheMu.Unlock()
}

// lookup reads a request view's sealed directory.
func (g *generation) lookup(name string) *metricState {
	p, meta, ok := g.sealed.LookupMetadata(name)
	if !ok {
		return nil
	}
	// Keep request state local; decoded sources and reductions already use
	// the bounded sealedCache. A name sweep must not grow another permanent map.
	return &metricState{policy: p, meta: meta, sealed: g.sealed, name: name, reader: g.reader, resource: g}
}

func (g *generation) loadPendingContext(ctx context.Context, names ...string) {
	g.metrics = make(map[string]*metricState)
	pending := g.pending
	if pending == nil && g.view.Len() > 0 {
		pending = g.view.Batches()
	}
	for _, b := range pending {
		if g.err = ctx.Err(); g.err != nil {
			return
		}
		// Admission interned every rollup batch through the catalog's canonical
		// JSON oracle and published only that identity, never raw policy bytes.
		h := b.Registered
		if h == nil {
			g.err = errors.New("pending batch without registered policy")
			return
		}
		var name string
		if len(names) == 0 {
			name = h.Name()
		} else {
			name, g.err = pendingRequestName(g.sealed, h, nil, names)
			if g.err != nil || name == "" {
				if g.err == nil {
					g.err = errors.New("pending identity does not match request")
				}
				return
			}
		}
		m := g.metrics[name]
		if m == nil {
			m = &metricState{name: name, reader: g.reader, resource: g}
			if base := g.lookup(name); base != nil {
				m.policy, m.meta, m.sealed = base.policy, base.meta, base.sealed
			} else {
				m.policy, m.meta, m.identity = h.RollupPolicy(), v2core.RollupMetadata{ID: b.ID, Policy: h.Definition()}, h
			}
			g.metrics[name] = m
		}
		// One catalog hands every batch of an ID the same handle: compare the
		// definition bytes once per name, then only the handle.
		if m.meta.ID != b.ID || (h != m.identity && !h.MatchesDefinition(m.meta.Policy)) {
			g.err = errors.New("pending identity/policy mismatch")
			return
		}
		m.identity = h
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
		if records, m.err = m.resource.sealed.Records(ctx, m.name); m.err != nil {
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
				m = g.lookup(name)
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
		select {
		case r.renders <- struct{}{}:
		default:
			return ErrAdmission
		}
		err := func() error {
			defer func() { <-r.renders }()
			stage := time.Now()
			s, workspace, e := render(ctx, m, source, from, until, now, r.WorkBytes)
			r.stages.observe(StageRender, stage)
			if workspace != nil {
				defer releaseRenderWorkspace(workspace)
			}
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
		if g.parts == nil {
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
	if err := g.inventory.AwaitHydration(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g.parts == nil {
		return g.discover(ctx)
	}
	stage := time.Now()
	index, err := g.parts.SnapshotForNames(ctx, g.snapshot, names, g.cache.ranges)
	st.observe(StageSnapshot, stage)
	if err != nil {
		return nil, err
	}
	stage = time.Now()
	pending, err := selectPending(ctx, index, g.view, names)
	if err != nil {
		return nil, err
	}
	st.observe(StagePending, stage)
	q := &generation{pending: pending, sealed: index, reader: g.reader, cache: g.cache, number: g.number}
	stage = time.Now()
	q.loadPendingContext(ctx, names...)
	st.observe(StageLoadPending, stage)
	return q, q.err
}

// Discovery alone enumerates names. Serialize its incremental refresh per owner
// so overlapping generations never independently reload the same new parts.
// Without legacy rows it reads registry-held parts only, never a snapshot.
func (g *generation) discover(ctx context.Context) (*generation, error) {
	if err := g.inventory.AwaitHydration(ctx); err != nil {
		return nil, err
	}
	c := g.cache
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.loadGate:
	}
	defer func() { c.loadGate <- struct{}{} }()
	c.mu.Lock()
	prior := c.index
	c.mu.Unlock()
	if g.parts != nil {
		ctx = v2core.WithOpenedRollupParts(ctx, g.parts)
	}
	index, err := v2core.NewRollupSnapshot(ctx, g.snapshot, prior, g.reader.Identities, g.inventory, c.limit)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	// An older pinned generation may finish after a newer publication.
	if c.index == nil || g.number >= c.number {
		c.index, c.number = index, g.number
	}
	c.mu.Unlock()
	q := &generation{pending: g.view.Batches(), sealed: index, reader: g.reader, cache: c, number: g.number}
	q.loadPendingContext(ctx)
	return q, q.err
}
