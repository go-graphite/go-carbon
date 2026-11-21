// Code copied from the pre-front-coding index (base 9bac77d60aea) as a test oracle.

package v2index

import (
	"context"
	"errors"
	"iter"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
)

// oldLevelFanout bounds the number of sorted levels: every level is more than
// oldLevelFanout times larger than the next one, so a 17M-name view has at most
// about a dozen levels and each name is rewritten O(oldLevelFanout*log N) times.
const oldLevelFanout = 4

// oldSnapshot may be held by readers after the source publishes a replacement.
// Names live in a few immutable, sorted, pairwise disjoint levels (largest
// first). A publication shares untouched levels with the preceding view, like
// small LSM parts that oldMerge later, instead of copying every name.
type oldSnapshot struct {
	revision uint64
	levels   [][]string
	n        int
}

func (s *oldSnapshot) Revision() uint64 { return s.revision }
func (s *oldSnapshot) Len() int         { return s.n }
func (s *oldSnapshot) Levels() int      { return len(s.levels) }
func (s *oldSnapshot) Names() iter.Seq[string] {
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
func (s *oldSnapshot) EqualNames(names []string) bool {
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

// oldCursor walks the disjoint levels in global sorted order.
type oldCursor struct {
	levels [][]string
	pos    []int
}

func (s *oldSnapshot) seek(key string) oldCursor {
	c := oldCursor{levels: s.levels, pos: make([]int, len(s.levels))}
	for i, level := range s.levels {
		c.pos[i] = sort.SearchStrings(level, key)
	}
	return c
}

// peek returns the smallest remaining name and its level.
func (c *oldCursor) peek() (string, int) {
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
func (c *oldCursor) next() (string, bool) {
	name, level := c.peek()
	if level < 0 {
		return "", false
	}
	c.pos[level]++
	return name, true
}

// skipTo advances every level to its first name >= key.
func (c *oldCursor) skipTo(key string) {
	for i, level := range c.levels {
		rest := level[c.pos[i]:]
		c.pos[i] += sort.SearchStrings(rest, key)
	}
}

// oldCatalog is a single-control-owner projection; oldSnapshot is safe concurrently.
// Publish/Apply belong at the catalog publication boundary, never per point.
// Recovery may publish its first view after discovery is attached.
type oldCatalog struct{ current atomic.Pointer[oldSnapshot] }

func (c *oldCatalog) Snapshot() *oldSnapshot {
	if s := c.current.Load(); s != nil {
		return s
	}
	return &oldSnapshot{}
}

func oldNamesCopy(names []string) ([]string, error) {
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
func (c *oldCatalog) Publish(revision uint64, names []string) error {
	old := c.Snapshot()
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	next, err := oldNamesCopy(names)
	if err != nil {
		return err
	}
	snapshot := &oldSnapshot{revision: revision, n: len(next)}
	if len(next) > 0 {
		snapshot.levels = [][]string{next}
	}
	c.current.Store(snapshot)
	return nil
}

// Apply adds admitted names and removes committed expiry names in one view.
// Removal wins within a batch. The serial owner must fence stale expiry against
// recreated names before calling Apply. This projection cannot decide liveness.
func (c *oldCatalog) Apply(revision uint64, add, remove []string) error {
	return c.ApplyBeforePublish(revision, add, remove, nil)
}

// ApplyBeforePublish prepares the incremental membership oldMerge privately. The
// callback fences dependent quota state before the immutable view is exposed.
func (c *oldCatalog) ApplyBeforePublish(revision uint64, add, remove []string, before func(*oldSnapshot) error) error {
	return c.oldApplyBPC(context.Background(), revision, add, remove, before)
}

// oldContains reports, for each sorted key, whether the sorted level holds it.
// Keys are searched in order, so each search starts at the previous match.
func oldContains(level, keys []string, found func(int)) {
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

// oldApplyBPC also cancels a large oldMerge during daemon shutdown.
// Its cost is O(changed names * levels * log N) plus the occasional level
// oldMerge; unchanged levels are shared with the preceding view, never copied.
func (c *oldCatalog) oldApplyBPC(ctx context.Context, revision uint64, add, remove []string, before func(*oldSnapshot) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	old := c.Snapshot()
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	additions, err := oldNamesCopy(add)
	if err != nil {
		return err
	}
	deletions := slices.Clone(remove)
	slices.Sort(deletions)
	deletions = slices.Compact(deletions)
	// Removal wins within a batch; names already present are not new.
	drop := make([]bool, len(additions))
	oldContains(deletions, additions, func(i int) { drop[i] = true })
	levels := make([][]string, 0, len(old.levels)+1)
	n, changed := old.n, false
	for _, level := range old.levels {
		oldContains(level, additions, func(i int) { drop[i] = true })
		removed := 0
		oldContains(level, deletions, func(int) { removed++ })
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
	if levels, err = oldCompact(ctx, levels); err != nil {
		return err
	}
	snapshot := &oldSnapshot{revision: revision, levels: levels, n: n}
	if before != nil {
		if err := before(snapshot); err != nil {
			return err
		}
	}
	c.current.Store(snapshot)
	return nil
}

// oldCompact merges a level into its predecessor until every level is more than
// oldLevelFanout times smaller than the one before it. The input slice is private.
func oldCompact(ctx context.Context, levels [][]string) ([][]string, error) {
	for i := len(levels) - 1; i > 0; {
		if len(levels[i])*oldLevelFanout < len(levels[i-1]) {
			i--
			continue
		}
		merged, err := oldMerge(ctx, levels[i-1], levels[i])
		if err != nil {
			return nil, err
		}
		levels[i-1] = merged
		levels = slices.Delete(levels, i, i+1)
		i = len(levels) - 1
	}
	return levels, nil
}

func oldMerge(ctx context.Context, a, b []string) ([]string, error) {
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

// MatchPaths uses the sorted name projection itself as the discovery index.
// Literal prefixes seek directly into the snapshot. Directory matches skip
// their entire descendant range, so a broad find visits namespaces, not every
// metric beneath each namespace. No secondary all-name image is rebuilt.
func (s *oldSnapshot) MatchPaths(ctx context.Context, glob string, yield func(string) error) error {
	return s.VisitPaths(ctx, glob, func(path string) error {
		matched, err := filepath.Match("/"+glob, path)
		if err == nil && matched {
			return yield(path)
		}
		return nil
	})
}

// VisitPaths enumerates possible virtual paths for a glob without imposing a
// matcher dialect. Carbonserver reuses its configured classic matcher.
func (s *oldSnapshot) VisitPaths(ctx context.Context, glob string, yield func(string) error) error {
	literal := glob
	if i := strings.IndexAny(literal, "*?[\\"); i >= 0 {
		literal = literal[:i]
	}
	literal = strings.TrimSuffix(literal, ".wsp")
	prefix := strings.ReplaceAll(literal, "/", ".")
	depth := strings.Count(glob, "/") + 1
	c := s.seek(prefix)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		name, level := c.peek()
		if level < 0 || !strings.HasPrefix(name, prefix) {
			break
		}
		c.pos[level]++
		path := strings.ReplaceAll(name, ".", "/") + ".wsp"
		cut, seen := -1, 0
		for j := range path {
			if path[j] == '/' {
				seen++
				if seen == depth {
					cut = j
					break
				}
			}
		}
		if cut >= 0 {
			path = path[:cut]
			// Every descendant of this directory produces the same candidate path.
			directory := strings.ReplaceAll(path, "/", ".") + "."
			c.skipTo(directory[:len(directory)-1] + "/") // '.' + 1, exclusive prefix end
		}
		if err := yield("/" + path); err != nil {
			return err
		}
	}
	return ctx.Err()
}
