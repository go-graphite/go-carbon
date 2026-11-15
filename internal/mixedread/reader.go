// Package mixedread combines an immutable legacy store with post-cutover parts.
package mixedread

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
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
	Legacy         *chunkstore.Store
	LegacyCatalog  partsread.Catalog
	Parts          partsread.Adapter
	SuppressLegacy func(string) bool
	// Converted is the persisted, verified name prefix already owned by v2.
	Converted *Cursor
	// MaxFindNames bounds duplicate tracking for a wide find; zero selects 1M.
	MaxFindNames int
	Now          func() time.Time
}
type Reader struct{ opts Options }

func New(opts Options) (*Reader, error) {
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
	return &Reader{opts}, nil
}
func (r *Reader) legacyAllowed(name string) bool {
	return r.opts.SuppressLegacy == nil || !r.opts.SuppressLegacy(name)
}

// Find emits the catalog union once per name. It forwards each side's original
// index query and bounds the duplicate set; it does not implement a glob parser.
func (r *Reader) Find(ctx context.Context, pattern string, yield func(string) error) error {
	seen := make(map[string]struct{})
	emit := func(name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := seen[name]; ok {
			return nil
		}
		if len(seen) >= r.opts.MaxFindNames {
			return partsread.ErrAdmission
		}
		seen[name] = struct{}{}
		return yield(name)
	}
	if err := r.opts.Parts.Find(ctx, pattern, emit); err != nil {
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
	legacyMetric, hasLegacy := r.opts.LegacyCatalog.Lookup(name)
	hasLegacy = hasLegacy && r.legacyAllowed(name)
	delivered := false
	err := r.opts.Parts.Fetch(ctx, name, from, until, func(current *partsread.Series) error {
		delivered = true
		if current != nil && int64(len(current.Values)) > r.opts.Parts.WorkBytes/64 {
			return partsread.ErrAdmission
		}
		if !hasLegacy {
			return consume(current)
		}
		if current != nil && !samePolicy(legacyMetric, current.Metric) {
			return ErrPolicy
		}
		if current != nil {
			if len(current.Occupied) != len(current.Values) {
				return errors.New("part reader omitted physical slot occupancy")
			}
			all := len(current.Values) > 0
			for _, occupied := range current.Occupied {
				all = all && occupied
			}
			if all {
				return consume(current)
			}
		}
		old, e := r.fetchLegacy(ctx, name, from, until)
		if e != nil {
			return e
		}
		if current == nil {
			return consume(old)
		}
		if old == nil {
			return consume(current)
		}
		// Whisper extends an aligned zero-width range by one slot only when
		// that backend has a populated archive. The other side can be empty.
		if len(current.Values) == 0 && len(old.Values) > 0 {
			return consume(old)
		}
		if len(old.Values) == 0 && len(current.Values) > 0 {
			return consume(current)
		}
		if current.From != old.From || current.Until != old.Until || current.Step != old.Step || len(current.Values) != len(old.Values) {
			return fmt.Errorf("%w: render shapes differ", ErrPolicy)
		}
		// Render results belong to this reader, so filling holes does not change
		// the published sources. Reuse legacy's slice; no third output allocation.
		for i, occupied := range current.Occupied {
			if occupied {
				old.Values[i] = current.Values[i]
			}
		}
		current.Values = old.Values
		return consume(current)
	})
	if !delivered && errors.Is(err, partsread.ErrNotFound) && hasLegacy {
		// A missing part publication/catalog entry must still reserve memory
		// through the consumer. Pin the immutable catalog for this fallback.
		h, e := r.opts.Parts.Catalog.Pin(r.opts.Parts.Budget, r.opts.Parts.WorkBytes)
		if e != nil {
			return e
		}
		defer h.Close()
		old, e := r.fetchLegacy(ctx, name, from, until)
		if e != nil {
			return e
		}
		return consume(old)
	}
	return err
}
func (r *Reader) fetchLegacy(ctx context.Context, name string, from, until int64) (*partsread.Series, error) {
	if int64(int(from)) != from || int64(int(until)) != until {
		return nil, errors.New("legacy query timestamp overflows int")
	}
	metric, ok := r.opts.LegacyCatalog.Lookup(name)
	if !ok {
		return nil, partsread.ErrNotFound
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
