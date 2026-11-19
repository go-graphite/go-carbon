// Package v2index publishes immutable live-name views for carbonserver discovery.
// It is a projection, not an ID allocator, durable catalog or expiry authority.
package v2index

import (
	"context"
	"encoding/binary"
	"errors"
	"iter"
	"math/bits"
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
// levelFanout times larger than the next one, so a 17M-name view keeps about
// log_8(17M/10k) = 4 levels and each name is rewritten O(levelFanout*log N)
// times. It is a variable only so the layout scale test can compare fanouts.
var levelFanout = 8

// Snapshot may be held by readers after the source publishes a replacement.
// Names live in a few immutable, sorted, pairwise disjoint levels (largest
// first). A publication shares untouched levels with the preceding view, like
// small LSM parts that merge later, instead of copying every name.
type Snapshot struct {
	revision uint64
	levels   []*level
	n        int
}

func (s *Snapshot) Revision() uint64 { return s.revision }
func (s *Snapshot) Len() int         { return s.n }
func (s *Snapshot) Levels() int      { return len(s.levels) }
func (s *Snapshot) Names() iter.Seq[string] {
	return func(yield func(string) bool) {
		c := s.seek("")
		for {
			name, level := c.peek()
			if level < 0 || !yield(string(name)) {
				return
			}
			c.advance(level)
		}
	}
}

// EqualNames compares a sorted, unique candidate without materializing another
// copy of this immutable view. This is used by the periodic discovery refresh.
func (s *Snapshot) EqualNames(names []string) bool {
	if len(names) != s.n {
		return false
	}
	c := s.seek("")
	for _, want := range names {
		name, level := c.peek()
		if level < 0 || string(name) != want {
			return false
		}
		c.advance(level)
	}
	return true
}

// blockNames is the number of names per front-coded block.
const blockNames = 32

// level is an immutable sorted run of unique names, front-coded like LevelDB
// data blocks or VictoriaMetrics mergeset items: each entry stores the length
// of the prefix it shares with the previous name, then the rest. The first
// name of every block is stored whole, so a seek binary-searches block starts
// and decodes at most one block of blockNames names.
type level struct {
	data   []byte // entries: uvarint shared, uvarint suffix length, suffix
	blocks []int  // offset of each block's first (whole) name in data
	n      int
}

// levelWriter appends names in strictly increasing order.
type levelWriter struct {
	l    level
	prev []byte
}

func (w *levelWriter) add(name []byte) {
	shared := 0
	if w.l.n%blockNames == 0 {
		w.l.blocks = append(w.l.blocks, len(w.l.data))
	} else {
		shared = sharedPrefix(w.prev, name)
	}
	w.l.data = binary.AppendUvarint(w.l.data, uint64(shared))
	w.l.data = binary.AppendUvarint(w.l.data, uint64(len(name)-shared))
	w.l.data = append(w.l.data, name[shared:]...)
	w.prev = append(w.prev[:0], name...)
	w.l.n++
}

// sharedPrefix compares eight bytes at a time: names share long prefixes.
func sharedPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	i := 0
	for ; i+8 <= n; i += 8 {
		if x := binary.LittleEndian.Uint64(a[i:]) ^ binary.LittleEndian.Uint64(b[i:]); x != 0 {
			return i + bits.TrailingZeros64(x)/8
		}
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func (w *levelWriter) finish() *level {
	w.l.data = slices.Clip(w.l.data)
	w.l.blocks = slices.Clip(w.l.blocks)
	return &w.l
}

func newLevel(names []string) *level {
	var w levelWriter
	w.l.data = make([]byte, 0, len(names)*16)
	for _, name := range names {
		w.add([]byte(name))
	}
	return w.finish()
}

// levelIter decodes one level forward; key is valid after a true step.
type levelIter struct {
	l     *level
	off   int // offset of entry i
	i     int // entries consumed
	key   []byte
	valid bool
}

func (it *levelIter) next() bool {
	if it.i >= it.l.n {
		it.valid = false
		return false
	}
	shared, k := binary.Uvarint(it.l.data[it.off:])
	it.off += k
	size, k := binary.Uvarint(it.l.data[it.off:])
	it.off += k
	it.key = append(it.key[:shared], it.l.data[it.off:it.off+int(size)]...)
	it.off += int(size)
	it.i++
	it.valid = true
	return true
}

// first returns block b's first name without copying it.
func (l *level) first(b int) []byte {
	off := l.blocks[b] + 1 // shared length is always 0
	size, k := binary.Uvarint(l.data[off:])
	return l.data[off+k : off+k+int(size)]
}

// at appends name i to dst[:0]. Blocks hold a fixed number of names, so a
// position decodes at most one block (no scan), like a mergeset item lookup.
func (l *level) at(i int, dst []byte) []byte {
	it := levelIter{l: l, off: l.blocks[i/blockNames], i: i / blockNames * blockNames, key: dst[:0]}
	for it.i <= i {
		it.next()
	}
	return it.key
}

// skipTo moves forward to the first name >= key; false at the end.
func (it *levelIter) skipTo(key string) bool {
	if it.valid && string(it.key) >= key {
		return true
	}
	// Jump to the last unstarted block whose first name is <= key. Keys are
	// usually near (sorted batches, directory skips), so gallop first.
	start := (it.i + blockNames - 1) / blockNames
	lo, hi := start, start
	for step := 1; hi < len(it.l.blocks) && string(it.l.first(hi)) <= key; step <<= 1 {
		lo, hi = hi+1, hi+step
	}
	hi = min(hi, len(it.l.blocks))
	b := lo + sort.Search(hi-lo, func(j int) bool { return string(it.l.first(lo+j)) > key }) - 1
	if b >= start && b*blockNames > it.i {
		it.off, it.i = it.l.blocks[b], b*blockNames
	}
	for it.next() {
		if string(it.key) >= key {
			return true
		}
	}
	return false
}

// cursor walks the disjoint levels in global sorted order.
type cursor struct{ its []levelIter }

func (s *Snapshot) seek(key string) cursor {
	c := cursor{its: make([]levelIter, len(s.levels))}
	for i, l := range s.levels {
		c.its[i].l = l
		c.its[i].skipTo(key)
	}
	return c
}

// peek returns the smallest remaining name and its level. The name is valid
// until that level advances.
func (c *cursor) peek() ([]byte, int) {
	best := -1
	for i := range c.its {
		if c.its[i].valid && (best < 0 || string(c.its[i].key) < string(c.its[best].key)) {
			best = i
		}
	}
	if best < 0 {
		return nil, -1
	}
	return c.its[best].key, best
}

func (c *cursor) advance(level int) { c.its[level].next() }

// skipTo advances every level to its first name >= key.
func (c *cursor) skipTo(key string) {
	for i := range c.its {
		c.its[i].skipTo(key)
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
		snapshot.levels = []*level{newLevel(next)}
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

// contains reports, for each sorted key, whether the level holds it.
// Keys are searched in order, so each search starts at the previous match.
func (l *level) contains(keys []string, found func(int)) {
	it := levelIter{l: l}
	for i, key := range keys {
		if !it.skipTo(key) {
			return
		}
		if string(it.key) == key {
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
	k := 0
	for i, name := range additions {
		k += sort.SearchStrings(deletions[k:], name)
		drop[i] = k < len(deletions) && deletions[k] == name
	}
	levels := make([]*level, 0, len(old.levels)+1)
	n, changed := old.n, false
	for _, l := range old.levels {
		l.contains(additions, func(i int) { drop[i] = true })
		removed := 0
		l.contains(deletions, func(int) { removed++ })
		if removed > 0 {
			var w levelWriter
			it, k := levelIter{l: l}, 0
			for it.next() {
				if it.i&1023 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				for k < len(deletions) && deletions[k] < string(it.key) {
					k++
				}
				if k == len(deletions) || deletions[k] != string(it.key) {
					w.add(it.key)
				}
			}
			l, n, changed = w.finish(), n-removed, true
		}
		if l.n > 0 {
			levels = append(levels, l)
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
		levels, n = append(levels, newLevel(fresh)), n+len(fresh)
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
func compact(ctx context.Context, levels []*level) ([]*level, error) {
	for i := len(levels) - 1; i > 0; {
		if levels[i].n*levelFanout < levels[i-1].n {
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

func merge(ctx context.Context, a, b *level) (*level, error) {
	var w levelWriter
	w.l.data = make([]byte, 0, len(a.data)+len(b.data))
	w.l.blocks = make([]int, 0, (a.n+b.n)/blockNames+1)
	x, y := levelIter{l: a}, levelIter{l: b}
	x.next()
	y.next()
	for x.valid || y.valid {
		if (x.i+y.i)&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if !y.valid || (x.valid && string(x.key) < string(y.key)) {
			w.add(x.key)
			x.next()
		} else {
			w.add(y.key)
			y.next()
		}
	}
	return w.finish(), nil
}
