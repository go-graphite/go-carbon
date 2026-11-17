// Package v2index publishes immutable live-name views for carbonserver discovery.
// It is a projection, not an ID allocator, durable catalog or expiry authority.
package v2index

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
)

// Source returns a detached, immutable host-wide union of live catalog names.
// Revisions increase on every membership change, including expiry/recreation.
// Implementations must not expose mutable producer-local NameCatalog state.
type Source interface{ Snapshot() *Snapshot }

// levelFanout bounds the number of sorted levels: every level is more than
// levelFanout times larger than the next one, so a 17M-name view has at most
// about a dozen levels and each name is rewritten O(levelFanout*log N) times.
const levelFanout = 4

// Snapshot may be held by readers after the source publishes a replacement.
// Names live in a few immutable, sorted, pairwise disjoint levels (largest
// first). A publication shares untouched levels with the preceding view, like
// small LSM parts that merge later, instead of copying every name.
type Snapshot struct {
	revision uint64
	levels   [][]string
	n        int
}

func (s *Snapshot) Revision() uint64 { return s.revision }
func (s *Snapshot) Len() int         { return s.n }
func (s *Snapshot) Levels() int      { return len(s.levels) }
func (s *Snapshot) Names() iter.Seq[string] {
	return func(yield func(string) bool) {
		c := s.seek("")
		for {
			name, ok := c.next()
			if !ok || !yield(name) {
				return
			}
		}
	}
}

// EqualNames compares a sorted, unique candidate without materializing another
// copy of this immutable view. This is used by the periodic discovery refresh.
func (s *Snapshot) EqualNames(names []string) bool {
	if len(names) != s.n {
		return false
	}
	i := 0
	for name := range s.Names() {
		if names[i] != name {
			return false
		}
		i++
	}
	return true
}

// cursor walks the disjoint levels in global sorted order.
type cursor struct {
	levels [][]string
	pos    []int
}

func (s *Snapshot) seek(key string) cursor {
	c := cursor{levels: s.levels, pos: make([]int, len(s.levels))}
	for i, level := range s.levels {
		c.pos[i] = sort.SearchStrings(level, key)
	}
	return c
}

// peek returns the smallest remaining name and its level.
func (c *cursor) peek() (string, int) {
	best := -1
	for i, level := range c.levels {
		if c.pos[i] < len(level) && (best < 0 || level[c.pos[i]] < c.levels[best][c.pos[best]]) {
			best = i
		}
	}
	if best < 0 {
		return "", -1
	}
	return c.levels[best][c.pos[best]], best
}
func (c *cursor) next() (string, bool) {
	name, level := c.peek()
	if level < 0 {
		return "", false
	}
	c.pos[level]++
	return name, true
}

// skipTo advances every level to its first name >= key.
func (c *cursor) skipTo(key string) {
	for i, level := range c.levels {
		rest := level[c.pos[i]:]
		c.pos[i] += sort.SearchStrings(rest, key)
	}
}

// Catalog is a single-control-owner projection; Snapshot is safe concurrently.
// Publish/Apply belong at the catalog publication boundary, never per point.
// Recovery may publish its first view after discovery is attached.
type Catalog struct{ current atomic.Pointer[Snapshot] }

func (c *Catalog) Snapshot() *Snapshot {
	if s := c.current.Load(); s != nil {
		return s
	}
	return &Snapshot{}
}

func namesCopy(names []string) ([]string, error) {
	out := slices.Clone(names)
	for _, name := range out {
		// Slash/NUL and empty components do not have an unambiguous classic path.
		if name == "" || strings.ContainsAny(name, "/\x00") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
			return nil, errors.New("v2 index: name has no classic metric path")
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// Publish replaces all names atomically. Stale publications fail closed.
// Names and the input slice may be reused after return.
func (c *Catalog) Publish(revision uint64, names []string) error {
	old := c.Snapshot()
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	next, err := namesCopy(names)
	if err != nil {
		return err
	}
	snapshot := &Snapshot{revision: revision, n: len(next)}
	if len(next) > 0 {
		snapshot.levels = [][]string{next}
	}
	c.current.Store(snapshot)
	return nil
}

// Apply adds admitted names and removes committed expiry names in one view.
// Removal wins within a batch. The serial owner must fence stale expiry against
// recreated names before calling Apply. This projection cannot decide liveness.
func (c *Catalog) Apply(revision uint64, add, remove []string) error {
	return c.ApplyBeforePublish(revision, add, remove, nil)
}

// ApplyBeforePublish prepares the incremental membership merge privately. The
// callback fences dependent quota state before the immutable view is exposed.
func (c *Catalog) ApplyBeforePublish(revision uint64, add, remove []string, before func(*Snapshot) error) error {
	return c.ApplyBeforePublishContext(context.Background(), revision, add, remove, before)
}

// contains reports, for each sorted key, whether the sorted level holds it.
// Keys are searched in order, so each search starts at the previous match.
func contains(level, keys []string, found func(int)) {
	lo := 0
	for i, key := range keys {
		// Gallop from the previous match: sorted keys in a large level
		// are usually close together, so this beats a full binary search.
		step := 1
		hi := lo
		for hi < len(level) && level[hi] < key {
			lo = hi + 1
			hi += step
			step <<= 1
		}
		hi = min(hi+1, len(level))
		lo += sort.SearchStrings(level[lo:hi], key)
		if lo == len(level) {
			return
		}
		if level[lo] == key {
			found(i)
		}
	}
}

// ApplyBeforePublishContext also cancels a large merge during daemon shutdown.
// Its cost is O(changed names * levels * log N) plus the occasional level
// merge; unchanged levels are shared with the preceding view, never copied.
func (c *Catalog) ApplyBeforePublishContext(ctx context.Context, revision uint64, add, remove []string, before func(*Snapshot) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	old := c.Snapshot()
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	additions, err := namesCopy(add)
	if err != nil {
		return err
	}
	deletions := slices.Clone(remove)
	slices.Sort(deletions)
	deletions = slices.Compact(deletions)
	// Removal wins within a batch; names already present are not new.
	drop := make([]bool, len(additions))
	contains(deletions, additions, func(i int) { drop[i] = true })
	levels := make([][]string, 0, len(old.levels)+1)
	n, changed := old.n, false
	for _, level := range old.levels {
		contains(level, additions, func(i int) { drop[i] = true })
		removed := 0
		contains(level, deletions, func(int) { removed++ })
		if removed > 0 {
			kept := make([]string, 0, len(level)-removed)
			k := 0
			for i, name := range level {
				if i&1023 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				for k < len(deletions) && deletions[k] < name {
					k++
				}
				if k == len(deletions) || deletions[k] != name {
					kept = append(kept, name)
				}
			}
			level, n, changed = kept, n-removed, true
		}
		if len(level) > 0 {
			levels = append(levels, level)
		}
	}
	fresh := additions[:0]
	for i, name := range additions {
		if !drop[i] {
			fresh = append(fresh, name)
		}
	}
	if len(fresh) == 0 && !changed {
		return nil
	}
	if len(fresh) > 0 {
		levels, n = append(levels, fresh), n+len(fresh)
	}
	if levels, err = compact(ctx, levels); err != nil {
		return err
	}
	snapshot := &Snapshot{revision: revision, levels: levels, n: n}
	if before != nil {
		if err := before(snapshot); err != nil {
			return err
		}
	}
	c.current.Store(snapshot)
	return nil
}

// compact merges a level into its predecessor until every level is more than
// levelFanout times smaller than the one before it. The input slice is private.
func compact(ctx context.Context, levels [][]string) ([][]string, error) {
	for i := len(levels) - 1; i > 0; {
		if len(levels[i])*levelFanout < len(levels[i-1]) {
			i--
			continue
		}
		merged, err := merge(ctx, levels[i-1], levels[i])
		if err != nil {
			return nil, err
		}
		levels[i-1] = merged
		levels = slices.Delete(levels, i, i+1)
		i = len(levels) - 1
	}
	return levels, nil
}

func merge(ctx context.Context, a, b []string) ([]string, error) {
	out := make([]string, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if (i+j)&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if a[i] < b[j] {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...), nil
}
