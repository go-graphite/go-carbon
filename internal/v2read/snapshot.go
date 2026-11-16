// Package v2read provides immutable publications for experimental part readers.
// Writers transfer frozen resources to Publish. No reader locks a writer mutex.
package v2read

import (
	"context"
	"errors"
	"sync/atomic"
)

var (
	ErrAdmission = errors.New("snapshot read byte budget exceeded")
	ErrClosed    = errors.New("snapshot publication closed")
	ErrNotFound  = errors.New("metric not found")
	ErrConflict  = errors.New("equal sequence has conflicting sample bits")
	ErrShape     = errors.New("render base shape differs from selected archive")
)

type Archive struct{ Step, Points int64 }
type Metric struct {
	ID                uint64
	Archives          []Archive
	AggregationMethod string
	XFilesFactor      float32
}

// Catalog must be immutable, including all returned archive slices. Find is
// supplied by the existing Graphite index so its glob semantics stay unchanged.
type Catalog interface {
	Lookup(string) (Metric, bool)
	Find(context.Context, string, func(string) error) error
}
type Sample struct {
	Time           int64
	Bits, Sequence uint64
}

// Source owns a frozen head, part set or sealed archive block set. Scan streams
// all versions occupying the physical ring slots addressed by [from, until),
// including newer timestamps outside that range. Rollup replacements must have
// a new Sequence. Stored NaNs are occupied slots. Never access a mutable head.
// HasArchive reports whether any slot exists, even outside the query range;
// Whisper uses this distinction for aligned zero-width queries.
type Source interface {
	Scan(context.Context, uint64, Archive, int64, int64, func(Sample) error) error
	HasArchive(context.Context, uint64, int64) (bool, error)
}

// View is transferred to Publication. Never mutate it, its slices, catalog or
// sources after publication. Close runs once after replacement and the last pin.
// RetainedBytes accounts for all memory/resources kept alive by this view.
type View struct {
	// Generation and Frontier identify this coherent shard publication.
	Generation          uint64
	Frontier            []uint64
	Catalog             Catalog
	Head, Parts, Blocks []Source
	RetainedBytes       int64
	Close               func()
}
type version struct {
	view View
	refs atomic.Int64
}

func (v *version) release() {
	if v.refs.Add(-1) == 0 && v.view.Close != nil {
		v.view.Close()
	}
}
func (v *version) retain() bool {
	for n := v.refs.Load(); n > 0; n = v.refs.Load() {
		if v.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
	return false
}

// Budget bounds bytes held by active readers, rather than request count.
// A slow reader pays for its pinned resources as well as its own merge/output.
type Budget struct {
	limit int64
	used  atomic.Int64
}

func NewBudget(limit int64) *Budget { return &Budget{limit: limit} }
func (b *Budget) Used() int64       { return b.used.Load() }
func (b *Budget) take(n int64) bool {
	if n < 0 || n > b.limit {
		return false
	}
	for used := b.used.Load(); used <= b.limit-n; used = b.used.Load() {
		if b.used.CompareAndSwap(used, used+n) {
			return true
		}
	}
	return false
}

type Publication struct{ current atomic.Pointer[version] }

// Publish accepts one coherent shard generation, not independently published
// head/part pointers. The owner serializes generation order before calling it.
func (p *Publication) Publish(view View) {
	v := &version{view: view}
	v.refs.Store(1)
	if old := p.current.Swap(v); old != nil {
		old.release()
	}
}
func (p *Publication) Close() {
	if old := p.current.Swap(nil); old != nil {
		old.release()
	}
}

// Handle and its returned results are valid until Close. Each handle has one
// reader owner; publication and budget are safe for concurrent use.
type Handle struct {
	v             *version
	budget        *Budget
	charged, work int64
	closed        atomic.Bool
	workspaces    *renderWorkspace
}

func (p *Publication) Pin(b *Budget, workBytes int64) (*Handle, error) {
	if workBytes < 0 {
		return nil, ErrAdmission
	}
	for {
		v := p.current.Load()
		if v == nil {
			return nil, ErrClosed
		}
		if !v.retain() {
			continue
		}
		retained := v.view.RetainedBytes
		if retained < 0 || retained > int64(^uint64(0)>>1)-workBytes || !b.take(retained+workBytes) {
			v.release()
			return nil, ErrAdmission
		}
		return &Handle{v: v, budget: b, charged: retained + workBytes, work: workBytes}, nil
	}
}
func (h *Handle) Close() {
	if h.closed.CompareAndSwap(false, true) {
		h.v.release()
		for w := h.workspaces; w != nil; {
			next := w.next
			releaseRenderWorkspace(w)
			w = next
		}
		h.workspaces = nil
		h.budget.used.Add(-h.charged)
	}
}
func (h *Handle) Find(ctx context.Context, pattern string, yield func(string) error) error {
	if h.closed.Load() {
		return ErrClosed
	}
	return h.v.view.Catalog.Find(ctx, pattern, yield)
}
