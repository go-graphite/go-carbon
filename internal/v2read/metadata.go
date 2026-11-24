package v2read

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// Metadata pins exactly the same publication used by render. It does not need
// a renderable time range and works for stored policies with no current points.
func (r *Reader) Metadata(ctx context.Context, name string) (Metric, error) {
	metric, _, err := r.MetadataWithMask(ctx, name)
	return metric, err
}

// MetadataWithMask reads discovery policy and reset authority from the same pins.
func (r *Reader) MetadataWithMask(ctx context.Context, name string) (Metric, bool, error) {
	return r.metadataWithMask(ctx, name, false)
}

// BaselineMetadata also returns expired policy so readmission keeps the installed
// baseline authority while the reset barrier prevents historical resurrection.
func (r *Reader) BaselineMetadata(ctx context.Context, name string) (Metric, error) {
	m, _, err := r.metadataWithMask(ctx, name, true)
	if err == nil && m.BaselineOrder == 0 {
		return Metric{}, ErrNotFound
	}
	return m, err
}

func (r *Reader) metadataWithMask(ctx context.Context, name string, includeExpired bool) (Metric, bool, error) {
	masked := false
	var result Metric
	found := false
	pins, err := r.loadOwners(ctx)
	if err != nil {
		return Metric{}, masked, err
	}
	defer releaseOwners(pins)
	views, errs := r.viewsForNames(ctx, pins, []string{name})
	for i, g := range views {
		if pins[i] == nil {
			continue
		}
		if errs[i] != nil {
			return Metric{}, masked, errs[i]
		}
		if g == nil {
			continue
		}
		m := g.metrics[name]
		if m == nil {
			m = g.lookup(name)
		}
		if m != nil {
			masked = masked || m.meta.BaselineOrder > 0 || m.meta.Expired || m.meta.ResetOrder > 0
		}
		if m != nil && (includeExpired || !m.meta.Expired) {
			if found {
				return Metric{}, masked, ErrNotFound
			}
			found = true
			result = Metric{BaselineOrder: m.meta.BaselineOrder, ID: m.meta.ID, XFilesFactor: m.policy.Config.XFilesFactor, AggregationMethod: aggregationName(m.policy.Config.Method)}
			for _, a := range m.policy.Config.Archives {
				result.Archives = append(result.Archives, Archive{Step: a.Step, Points: a.Count})
			}
		}
	}
	if !found {
		return Metric{}, masked, ErrNotFound
	}
	result.Archives = slices.Clone(result.Archives)
	return result, masked, nil
}

// Names projects the pinned reader publications, including admitted pending
// applications; producer-local mutable catalogs are never inspected.
func (r *Reader) Names(ctx context.Context) ([]string, error) {
	names, _, err := r.NamesWithMasks(ctx)
	return names, err
}

// NamesWithMasks projects visible names and classic reset barriers together.
// Each owner is pinned once; masks never sample a later publication than names.
// Owners are independent, so up to GOMAXPROCS of them discover at once; each
// holds one decoded part at a time. Errors surface in owner order.
func (r *Reader) NamesWithMasks(ctx context.Context) ([]string, map[string]bool, error) {
	pins, err := r.loadOwners(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer releaseOwners(pins)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type owned struct {
		names, masked []string
		err           error
	}
	out := make([]owned, len(pins))
	slots := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for i, g := range pins {
		if g == nil {
			continue
		}
		wg.Go(func() {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				out[i].err = ctx.Err()
				return
			}
			o := &out[i]
			visit := func(n string, meta v2core.RollupMetadata) {
				if meta.BaselineOrder > 0 || meta.Expired || meta.ResetOrder > 0 {
					o.masked = append(o.masked, n)
				}
				if !meta.Expired {
					o.names = append(o.names, n)
				}
			}
			d, err := g.discover(ctx)
			if err == nil {
				// Enumeration projects names and masks; it need not retain the
				// all-name metadata directory. Ordinary renders build request-local
				// directories, and fallback readers can rebuild on their next read.
				defer func() {
					g.cache.mu.Lock()
					if g.cache.index == d.sealed {
						g.cache.index = nil
					}
					g.cache.mu.Unlock()
				}()
				err = d.sealed.MetadataContext(ctx, func(n string, _ v2core.RollupPolicy, meta v2core.RollupMetadata) {
					if pending := d.metrics[n]; pending != nil {
						meta = pending.meta
					}
					visit(n, meta)
				})
			}
			if err != nil {
				o.err = err
				cancel()
				return
			}
			for n, m := range d.metrics {
				visit(n, m.meta)
			}
			slices.Sort(o.names)
			o.names = slices.Compact(o.names)
		})
	}
	wg.Wait()
	// The first failing owner cancels the rest; report its error, not theirs.
	var failed error
	for _, o := range out {
		if o.err != nil && (failed == nil || errors.Is(failed, context.Canceled) && !errors.Is(o.err, context.Canceled)) {
			failed = o.err
		}
	}
	if failed != nil {
		return nil, nil, failed
	}
	masks := make(map[string]bool)
	lists := make([][]string, len(out))
	for i, o := range out {
		lists[i] = o.names
		for _, n := range o.masked {
			masks[n] = true
		}
	}
	return MergeNames(lists...), masks, nil
}

// MergeNames merges sorted, duplicate-free lists into one sorted list, keeping
// a name present in several lists once. Pairs of lists merge in parallel
// rounds, so 10M names from 96 owners take 7 linear rounds, not a sort.
func MergeNames(lists ...[]string) []string {
	for len(lists) > 1 {
		next := make([][]string, (len(lists)+1)/2)
		var wg sync.WaitGroup
		for i := range next {
			if 2*i+1 == len(lists) {
				next[i] = lists[2*i]
				continue
			}
			wg.Go(func() { next[i] = mergeTwo(lists[2*i], lists[2*i+1]) })
		}
		wg.Wait()
		lists = next
	}
	if len(lists) == 0 {
		return nil
	}
	return lists[0]
}

func mergeTwo(a, b []string) []string {
	out := make([]string, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		switch c := strings.Compare(a[0], b[0]); {
		case c < 0:
			out, a = append(out, a[0]), a[1:]
		case c > 0:
			out, b = append(out, b[0]), b[1:]
		default:
			out, a, b = append(out, a[0]), a[1:], b[1:]
		}
	}
	return append(append(out, a...), b...)
}

// loadOwners pins publications without starting any directory work.
func (r *Reader) loadOwners(ctx context.Context) ([]*generation, error) {
	// Requests own these pins until their synchronous work or callback ends.
	pins := make([]*generation, len(r.owners))
	ok := false
	defer func() {
		if !ok {
			releaseOwners(pins)
		}
	}()
	for i := range r.owners {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			g := r.owners[i].Load()
			if g == nil {
				break
			}
			if g.retain() {
				pins[i] = g
				break
			}
		}
	}
	ok = true
	return pins, nil
}

func releaseOwners(pins []*generation) {
	for _, g := range pins {
		if g != nil {
			g.release()
		}
	}
}

// Masked reports durable whole-name reset authority from the same pinned
// publication as the data. A reset keeps masking classic history after resume.
func (r *Reader) Masked(ctx context.Context, name string) (bool, error) {
	_, masked, err := r.metadataWithMask(ctx, name, true)
	if errors.Is(err, ErrNotFound) {
		err = nil
	}
	return masked, err
}

// AwaitHydration fences find's projected names on their physical owners. With
// no names (directory or negative lookup) it fences every owner. Render and
// metadata requests also fence selected owners in forNames.
func (r *Reader) AwaitHydration(ctx context.Context, names ...string) error {
	pins, err := r.loadOwners(ctx)
	if err != nil {
		return err
	}
	defer releaseOwners(pins)
	errs := make([]error, len(pins))
	selected := make([]bool, len(pins))
	for _, name := range names {
		selected[OwnerFor(name, len(pins))] = true
	}
	var wg sync.WaitGroup
	for i, g := range pins {
		if g != nil && (len(names) == 0 || selected[i]) {
			wg.Go(func() { errs[i] = g.inventory.AwaitHydration(ctx) })
		}
	}
	wg.Wait()
	return errors.Join(errs...)
}
