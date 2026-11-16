// Package mixedread combines an immutable legacy store with post-cutover parts.
package mixedread

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsread"
)

var ErrPolicy = errors.New("mixed reader archive policy differs across cutover")

// Options binds one immutable legacy/epoch routing generation. LegacyCatalog
// uses the existing immutable Graphite index, preserving its glob semantics.
// SuppressLegacy hides deleted/recreated names; it must come from the same
// immutable routing generation as the catalogs and selected part publications.
// Part publications may advance within an epoch, but changing an epoch or its
// legacy binding requires publishing a new Reader and draining the old one.
// Storage owners retain all handles and publications until Reader callers drain.
type Options struct {
	// Snapshot optionally binds a complete immutable part generation for this
	// Reader. Its owner drains Reader callers before closing it. It must carry
	// a LegacyCatalog whose values are *LegacyBinding or nil.
	Snapshot      *partsread.Snapshot
	Legacy        *chunkstore.Store
	LegacyCatalog partsread.Catalog
	// UnifiedCatalog declares Parts.Catalog authoritative for the complete name
	// union, including legacy suppression, in the same immutable generation.
	// The publisher builds it through the existing Graphite index.
	UnifiedCatalog bool
	Parts          partsread.Adapter
	SuppressLegacy func(string) bool
	// Converted is the persisted, verified name prefix already owned by v2.
	Converted *Cursor
	// LegacyHorizon is a verified inclusive upper bound on every stored legacy
	// archive timestamp. Zero disables this optimization. It must account for
	// all physical slots, not merely the log cutover time.
	LegacyHorizon int64
	// MaxFindNames bounds duplicate tracking for a wide find; zero selects 1M.
	MaxFindNames int
	Now          func() time.Time
}
type request struct {
	handle  partsread.Handle
	legacy  partsread.Series
	current partsread.Series
}
type Reader struct {
	opts Options
}

// Request storage contains no generation-owned pointers after release. Share it
// across immutable Readers so publication turnover and fresh cold handles do
// not allocate a new pool and scratch object for each generation.
var requests sync.Pool

func (r *Reader) takeRequest() *request {
	if q := requests.Get(); q != nil {
		return q.(*request)
	}
	return new(request)
}
func (r *Reader) releaseRequest(q *request) {
	q.handle.Close()
	q.legacy = partsread.Series{}
	q.current = partsread.Series{}
	requests.Put(q)
}

func New(opts Options) (*Reader, error) {
	if opts.LegacyHorizon < 0 || opts.LegacyHorizon > math.MaxUint32 {
		return nil, errors.New("invalid legacy timestamp horizon")
	}
	if opts.Legacy == nil || !opts.Legacy.ReadOnly() {
		return nil, errors.New("mixed reader requires a read-only legacy store")
	}
	if opts.LegacyCatalog == nil || opts.Parts.Select == nil || opts.Parts.Catalog == nil || opts.Parts.Budget == nil {
		return nil, errors.New("mixed reader requires immutable legacy and part catalogs and a byte budget")
	}
	// Reserve both render workspaces before either backend allocates. The
	// underlying part reader enforces 32 bytes/slot; a straddling query also
	// owns the legacy result until the consumer finishes.
	if opts.Parts.WorkBytes <= 0 || opts.Parts.WorkBytes > (1<<63-1)/2 {
		return nil, errors.New("invalid mixed render byte budget")
	}
	opts.Parts.WorkBytes *= 2
	if opts.MaxFindNames < 0 {
		return nil, errors.New("negative find bound")
	}
	if opts.MaxFindNames == 0 {
		opts.MaxFindNames = 1_000_000
	}
	if opts.Converted != nil {
		converted, err := opts.Converted.Filter()
		if err != nil {
			return nil, err
		}
		suppressed := opts.SuppressLegacy
		opts.SuppressLegacy = func(name string) bool {
			return converted(name) || suppressed != nil && suppressed(name)
		}
	}
	if opts.Now == nil {
		opts.Now = opts.Legacy.Now
	}
	opts.Parts.Now = opts.Now
	if opts.Snapshot != nil && (!opts.UnifiedCatalog || !opts.Snapshot.HasLegacyCatalog()) {
		return nil, errors.New("bound snapshot requires complete explicit legacy metadata")
	}
	return &Reader{opts: opts}, nil
}
func (r *Reader) legacyAllowed(name string) bool {
	return r.opts.SuppressLegacy == nil || !r.opts.SuppressLegacy(name)
}

// Find emits the catalog union once per name. It forwards each side's original
// index query and bounds the duplicate set; it does not implement a glob parser.
func (r *Reader) Find(ctx context.Context, pattern string, yield func(string) error) error {
	q := r.takeRequest()
	defer r.releaseRequest(q)
	h := &q.handle
	var err error
	if r.opts.Snapshot != nil {
		err = r.opts.Snapshot.BorrowInto(h, r.opts.Parts.Budget, r.opts.Parts.WorkBytes)
	} else {
		err = r.opts.Parts.Catalog.PinInto(h, r.opts.Parts.Budget, r.opts.Parts.WorkBytes)
	}
	if err != nil {
		return err
	}
	if r.opts.UnifiedCatalog {
		if err := ctx.Err(); err != nil {
			return err
		}
		// The complete index already owns deduplication and bounded query work;
		// MaxFindNames bounds only the fallback's temporary duplicate set.
		return h.Find(ctx, pattern, yield)
	}

	seen := make(map[string]struct{})
	var nameBytes int64
	emit := func(name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := seen[name]; ok {
			return nil
		}
		// Include name bytes and conservative map/string bookkeeping. Keep
		// this reservation pinned through both catalogs and consumer callbacks.
		cost := int64(len(name)) + 96
		if len(seen) >= r.opts.MaxFindNames || cost > r.opts.Parts.WorkBytes-nameBytes {
			return partsread.ErrAdmission
		}
		nameBytes += cost
		seen[name] = struct{}{}
		return yield(name)
	}
	if err := h.Find(ctx, pattern, emit); err != nil {
		return err
	}
	return r.opts.LegacyCatalog.Find(ctx, pattern, func(name string) error {
		if !r.legacyAllowed(name) {
			return nil
		}
		return emit(name)
	})
}

// Fetch consumes the result while its part snapshot and byte budget are held.
// A v2 replacement wins whenever its physical slot is occupied, even if it is
// a NaN or a newer ring epoch outside the requested interval. Untouched slots
// fall back to legacy. This also preserves post-cutover late corrections to
// timestamps before cutover. Rollups must already be exact sealed replacements.
func (r *Reader) Fetch(ctx context.Context, name string, from, until int64, consume func(*partsread.Series) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.opts.Snapshot != nil {
		return r.fetchBound(ctx, name, from, until, consume)
	}
	var legacyMetric partsread.Metric
	var hasLegacy bool
	if r.legacyAllowed(name) {
		legacyMetric, hasLegacy = r.opts.LegacyCatalog.Lookup(name)
	}
	p := r.opts.Parts.Select(name)
	if p == nil {
		if !hasLegacy {
			return partsread.ErrNotFound
		}
		h, err := r.opts.Parts.Catalog.Pin(r.opts.Parts.Budget, r.opts.Parts.WorkBytes)
		if err != nil {
			return err
		}
		defer h.Close()
		old, err := r.fetchLegacy(ctx, name, legacyMetric, from, until)
		if err != nil {
			return err
		}
		return consume(old)
	}

	q := r.takeRequest()
	defer r.releaseRequest(q)
	h := &q.handle
	err := p.PinInto(h, r.opts.Parts.Budget, r.opts.Parts.WorkBytes)
	if err != nil {
		return err
	}
	var old *partsread.Series
	skipLegacy := hasLegacy && r.afterLegacyHorizon(legacyMetric, from)
	if hasLegacy && !skipLegacy {
		old, err = r.fetchLegacy(ctx, name, legacyMetric, from, until)
		if err != nil {
			return err
		}
	}
	var current *partsread.Series
	// An existing legacy result supplies the only dense output. RenderInto
	// replaces physical slots directly; no dense occupancy scan or merge follows.
	// One-slot results keep the ordinary path because an empty opposite archive
	// can select a zero-width shape instead of Whisper's populated extension.
	if old != nil && len(old.Values) > 1 {
		current, err = h.RenderInto(ctx, name, from, until, r.opts.Now().Unix(), old)
	} else {
		current, err = h.Render(ctx, name, from, until, r.opts.Now().Unix())
	}
	if errors.Is(err, partsread.ErrNotFound) && hasLegacy {
		if skipLegacy {
			old, err = r.fetchLegacy(ctx, name, legacyMetric, from, until)
			if err != nil {
				return err
			}
		}
		return consume(old)
	}
	if errors.Is(err, partsread.ErrShape) {
		return fmt.Errorf("%w: %w", ErrPolicy, err)
	}
	if err != nil {
		return err
	}
	if current == nil {
		return consume(old)
	}
	if hasLegacy && !samePolicy(legacyMetric, current.Metric) {
		return ErrPolicy
	}
	if int64(len(current.Values)) > r.opts.Parts.WorkBytes/64 {
		return partsread.ErrAdmission
	}
	if len(current.Occupied) != len(current.Values) {
		return errors.New("part reader omitted physical slot occupancy")
	}
	if old == nil || len(old.Values) == 0 || len(old.Values) > 1 {
		return consume(current)
	}
	if len(current.Values) == 0 {
		return consume(old)
	}
	if current.From != old.From || current.Until != old.Until || current.Step != old.Step || len(current.Values) != len(old.Values) {
		return fmt.Errorf("%w: render shapes differ", ErrPolicy)
	}
	// Untouched pooled part slots are immutable: sparse workspace release only
	// resets occupied indices. Put the combined result in legacy's owned output.
	for i, occupied := range current.Occupied {
		if occupied {
			old.Values[i] = current.Values[i]
		}
	}
	current.Values = old.Values
	return consume(current)
}

func (r *Reader) afterLegacyHorizon(metric partsread.Metric, from int64) bool {
	horizon := r.opts.LegacyHorizon
	if horizon == 0 {
		return false
	}
	if from >= horizon {
		return true
	}
	var retention int64
	for _, a := range metric.Archives {
		if a.Step <= 0 || a.Points <= 0 || a.Points > math.MaxUint32/a.Step {
			return false
		}
		retention = max(retention, a.Step*a.Points)
	}
	return retention > 0 && r.opts.Now().Unix()-retention >= horizon
}

func (r *Reader) fetchLegacy(ctx context.Context, name string, metric partsread.Metric, from, until int64) (*partsread.Series, error) {
	if int64(int(from)) != from || int64(int(until)) != until {
		return nil, errors.New("legacy query timestamp overflows int")
	}
	if err := r.checkLegacyBudget(metric, from, until); err != nil {
		return nil, err
	}
	s, err := r.opts.Legacy.Fetch(ctx, name, int(from), int(until))
	if err != nil || s == nil {
		return nil, err
	}
	return &partsread.Series{Metric: metric, From: int64(s.FromTime), Until: int64(s.UntilTime), Step: int64(s.Step), Values: s.Values}, nil
}
func samePolicy(a, b partsread.Metric) bool {
	return slices.Equal(a.Archives, b.Archives) && a.AggregationMethod == b.AggregationMethod && a.XFilesFactor == b.XFilesFactor
}

// Bound the legacy output before Fetch allocates it. The extra slot covers
// Whisper's populated, aligned zero-width range extension.
func (r *Reader) checkLegacyBudget(m partsread.Metric, from, until int64) error {
	if from > until || len(m.Archives) == 0 {
		return ErrPolicy
	}
	now := r.opts.Now().Unix()
	if now < 0 || now > math.MaxUint32 {
		return ErrPolicy
	}
	var retention int64
	for _, a := range m.Archives {
		if a.Step <= 0 || a.Points <= 0 || a.Points > math.MaxUint32/a.Step {
			return ErrPolicy
		}
		retention = max(retention, a.Step*a.Points)
	}
	oldest := now - retention
	if from > now || until < oldest {
		return nil
	}
	from, until = max(from, oldest), min(until, now)
	a := m.Archives[len(m.Archives)-1]
	for _, candidate := range m.Archives {
		if candidate.Step*candidate.Points >= now-from {
			a = candidate
			break
		}
	}
	if (until-from)/a.Step+1 > r.opts.Parts.WorkBytes/64 {
		return partsread.ErrAdmission
	}
	return nil
}

// LegacyBinding is constructed once when the coherent catalog is published.
// The associated store remains open until the reader generation drains.
type LegacyBinding struct {
	frozen *chunkstore.FrozenMetric
	metric partsread.Metric
}

func BindLegacy(frozen *chunkstore.FrozenMetric, m partsread.Metric) (*LegacyBinding, error) {
	if frozen == nil {
		return nil, ErrPolicy
	}
	c := frozen.Config()
	if c.AggregationMethod.String() != m.AggregationMethod || c.XFilesFactor != m.XFilesFactor || len(c.Retentions) != len(m.Archives) {
		return nil, ErrPolicy
	}
	for i, a := range m.Archives {
		if a.Step != int64(c.Retentions[i].Step) || a.Points != int64(c.Retentions[i].Count) {
			return nil, ErrPolicy
		}
	}
	m.Archives = slices.Clone(m.Archives)
	return &LegacyBinding{frozen: frozen, metric: m}, nil
}
func (r *Reader) fetchFrozen(ctx context.Context, q *request, b *LegacyBinding, from, until int64) (*partsread.Series, error) {
	if int64(int(from)) != from || int64(int(until)) != until {
		return nil, errors.New("legacy query timestamp overflows int")
	}
	if err := r.checkLegacyBudget(b.metric, from, until); err != nil {
		return nil, err
	}
	var result *partsread.Series
	err := r.opts.Legacy.FetchFrozenValues(ctx, b.frozen, int(from), int(until), func(from, until, step int, values []float64) error {
		q.legacy = partsread.Series{Metric: b.metric, From: int64(from), Until: int64(until), Step: int64(step), Values: values}
		result = &q.legacy
		return nil
	})
	return result, err
}
func (r *Reader) fetchBound(ctx context.Context, name string, from, until int64, consume func(*partsread.Series) error) error {
	q := r.takeRequest()
	defer r.releaseRequest(q)
	h := &q.handle
	if err := r.opts.Snapshot.BorrowInto(h, r.opts.Parts.Budget, r.opts.Parts.WorkBytes); err != nil {
		return err
	}
	resolved, err := h.Resolve(name)
	if err != nil {
		return err
	}
	var binding *LegacyBinding
	if resolved.LegacyPresent() && r.legacyAllowed(name) {
		var ok bool
		binding, ok = resolved.Legacy().(*LegacyBinding)
		if !ok || binding == nil || !samePolicy(binding.metric, resolved.Metric()) {
			return ErrPolicy
		}
	}
	legacyMetric := resolved.Metric()
	hasLegacy := binding != nil
	var old *partsread.Series
	skipLegacy := hasLegacy && r.afterLegacyHorizon(legacyMetric, from)
	if hasLegacy && !skipLegacy {
		old, err = r.fetchFrozen(ctx, q, binding, from, until)
		if err != nil {
			return err
		}
	}
	if old != nil && len(old.Values) > 1 {
		overlaps, err := h.HasOverlayResolved(ctx, resolved, old)
		if err != nil {
			return err
		}
		if !overlaps {
			return consume(old)
		}
	}
	var current *partsread.Series
	// An existing legacy result supplies the only dense output. RenderInto
	// replaces physical slots directly; no dense occupancy scan or merge follows.
	// One-slot results keep the ordinary path because an empty opposite archive
	// can select a zero-width shape instead of Whisper's populated extension.
	if old != nil && len(old.Values) > 1 {
		current, err = h.RenderResolvedTo(ctx, resolved, from, until, r.opts.Now().Unix(), old, &q.current)
	} else {
		current, err = h.RenderResolvedTo(ctx, resolved, from, until, r.opts.Now().Unix(), nil, &q.current)
	}
	if errors.Is(err, partsread.ErrNotFound) && hasLegacy {
		if skipLegacy {
			old, err = r.fetchFrozen(ctx, q, binding, from, until)
			if err != nil {
				return err
			}
		}
		return consume(old)
	}
	if errors.Is(err, partsread.ErrShape) {
		return fmt.Errorf("%w: %w", ErrPolicy, err)
	}
	if err != nil {
		return err
	}
	if current == nil {
		return consume(old)
	}
	if hasLegacy && !samePolicy(legacyMetric, current.Metric) {
		return ErrPolicy
	}
	if int64(len(current.Values)) > r.opts.Parts.WorkBytes/64 {
		return partsread.ErrAdmission
	}
	if len(current.Occupied) != len(current.Values) {
		return errors.New("part reader omitted physical slot occupancy")
	}
	if old == nil || len(old.Values) == 0 || len(old.Values) > 1 {
		return consume(current)
	}
	if len(current.Values) == 0 {
		return consume(old)
	}
	if current.From != old.From || current.Until != old.Until || current.Step != old.Step || len(current.Values) != len(old.Values) {
		return fmt.Errorf("%w: render shapes differ", ErrPolicy)
	}
	// Untouched pooled part slots are immutable: sparse workspace release only
	// resets occupied indices. Put the combined result in legacy's owned output.
	for i, occupied := range current.Occupied {
		if occupied {
			old.Values[i] = current.Values[i]
		}
	}
	current.Values = old.Values
	return consume(current)
}
