// Package v2index publishes immutable live-name views for carbonserver discovery.
// It is a projection, not an ID allocator, durable catalog or expiry authority.
package v2index

import (
	"context"
	"encoding/binary"
	"errors"
	"iter"
	"math/bits"
	"os"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"
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

// run is an immutable sorted run of unique names, front-coded like LevelDB
// data blocks or VictoriaMetrics mergeset items: each entry stores the length
// of the prefix it shares with the previous name, then the rest. The first
// name of every block is stored whole, so a seek binary-searches block starts
// and decodes at most one block of blockNames names. The base run is a
// read-only file mapping (page cache); only its block offsets are on the heap.
type run struct {
	data   []byte // entries: uvarint shared, uvarint suffix length, suffix
	blocks []int  // offset of each block's first (whole) name in data
	n      int
	path   string // base file; empty on the heap
}

// level is a run minus its removed names: sorted positions in the run. A
// removal never rewrites a run; the next merge of that run drops the names.
type level struct {
	*run
	dead []uint32
}

func (l *level) live() int { return l.n - len(l.dead) }

// levelWriter appends names in strictly increasing order. With out set it
// streams the entries to a file in chunks and keeps only block offsets.
type levelWriter struct {
	r    run
	prev []byte
	out  *os.File
	size int
	err  error
}

func (w *levelWriter) add(name []byte) {
	shared := 0
	if w.r.n%blockNames == 0 {
		w.r.blocks = append(w.r.blocks, w.size)
		if w.out != nil && len(w.r.data) >= 64<<10 {
			w.flush()
		}
	} else {
		shared = sharedPrefix(w.prev, name)
	}
	before := len(w.r.data)
	w.r.data = binary.AppendUvarint(w.r.data, uint64(shared))
	w.r.data = binary.AppendUvarint(w.r.data, uint64(len(name)-shared))
	w.r.data = append(w.r.data, name[shared:]...)
	w.size += len(w.r.data) - before
	w.prev = append(w.prev[:0], name...)
	w.r.n++
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

func (w *levelWriter) finish() *run {
	w.r.data = slices.Clip(w.r.data)
	w.r.blocks = slices.Clip(w.r.blocks)
	return &w.r
}

func newRun(names []string) *run {
	var w levelWriter
	w.r.data = make([]byte, 0, len(names)*16)
	for _, name := range names {
		w.add([]byte(name))
	}
	return w.finish()
}

func newLevel(names []string) *level { return &level{run: newRun(names)} }

// levelIter decodes one level forward, skipping removed names; key is valid
// after a true step.
type levelIter struct {
	l     *level
	off   int // offset of entry i
	i     int // entries consumed
	d     int // first dead position >= i
	key   []byte
	valid bool
}

func (it *levelIter) next() bool {
	for it.i < it.l.n {
		shared, k := binary.Uvarint(it.l.data[it.off:])
		it.off += k
		size, k := binary.Uvarint(it.l.data[it.off:])
		it.off += k
		it.key = append(it.key[:shared], it.l.data[it.off:it.off+int(size)]...)
		it.off += int(size)
		p := uint32(it.i)
		it.i++
		for it.d < len(it.l.dead) && it.l.dead[it.d] < p {
			it.d++
		}
		if it.d < len(it.l.dead) && it.l.dead[it.d] == p {
			continue
		}
		it.valid = true
		return true
	}
	it.valid = false
	return false
}

// first returns block b's first name without copying it.
func (r *run) first(b int) []byte {
	off := r.blocks[b] + 1 // shared length is always 0
	size, k := binary.Uvarint(r.data[off:])
	return r.data[off+k : off+k+int(size)]
}

// at appends name i to dst[:0]. Blocks hold a fixed number of names, so a
// position decodes at most one block (no scan), like a mergeset item lookup.
func (r *run) at(i int, dst []byte) []byte {
	it := levelIter{l: &level{run: r}, off: r.blocks[i/blockNames], i: i / blockNames * blockNames, key: dst[:0]}
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
	if start == 0 {
		hi = len(it.l.blocks) // a fresh seek binary-searches every block
	}
	for step := 1; hi < len(it.l.blocks) && string(it.l.first(hi)) <= key; step <<= 1 {
		lo, hi = hi+1, hi+step
	}
	hi = min(hi, len(it.l.blocks))
	b := lo + sort.Search(hi-lo, func(j int) bool { return string(it.l.first(lo+j)) > key }) - 1
	if b >= start && b*blockNames > it.i {
		it.off, it.i = it.l.blocks[b], b*blockNames
		p := uint32(it.i)
		it.d = sort.Search(len(it.l.dead), func(j int) bool { return it.l.dead[j] >= p })
	}
	for it.next() {
		if string(it.key) >= key {
			return true
		}
	}
	return false
}

// find reports, for each sorted key the run holds (removed or not), the key
// index and the name position. Each search starts at the previous match.
func (r *run) find(keys []string, found func(i, pos int)) {
	it := levelIter{l: &level{run: r}}
	for i, key := range keys {
		if !it.skipTo(key) {
			return
		}
		if string(it.key) == key {
			found(i, it.i-1)
		}
	}
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

// Catalog is a single-writer projection; Snapshot is safe concurrently.
// Publish, PublishCore, Apply* and Close must not overlap: the writer state
// (the background merge job and file sequence) has no lock, and an
// overlapping writer panics like a concurrent Go map write. Calls from
// different goroutines are fine when they are ordered (one owner at a time).
// Publish/Apply belong at the catalog publication boundary, never per point.
// Recovery may publish its first view after discovery is attached.
//
// The largest (base) level is rewritten only by one background merge, never
// inside Publish's successor Apply calls, so publication latency does not
// grow with the host's name count. Apply installs a finished merge.
type Catalog struct {
	current  atomic.Pointer[Snapshot]
	writer   atomic.Bool // a Publish, Apply or Close is running
	dir      string      // base files; empty keeps the base on the heap
	seq      int
	log      *os.File // name log of base-seq; nil when not persisted
	logged   int      // bytes of changes in log
	job      *baseMerge
	err      atomic.Pointer[error] // the failure of the heap base's file write
	failures atomic.Uint64
	retryAt  time.Time // the next spill of a heap base after a failed write
}

// own asserts the single-writer contract and returns its release.
func (c *Catalog) own() func() {
	if !c.writer.CompareAndSwap(false, true) {
		panic("v2 index: concurrent catalog writers")
	}
	return func() { c.writer.Store(false) }
}

// baseMerge rewrites inputs (the base, then level 1 if any) into one run.
// Inputs are captured with their removed names at start.
type baseMerge struct {
	inputs []*level
	done   chan struct{}
	cancel context.CancelFunc
	path   string
	out    *run
	err    error // the file write failed; out is on the heap
	seq    int   // the base file's sequence number
}

func (c *Catalog) Snapshot() *Snapshot {
	if s := c.current.Load(); s != nil {
		return s
	}
	return &Snapshot{}
}

// Err returns the base file failure of the current streak. The index stays
// correct: the base is kept on the heap until the next file write succeeds.
func (c *Catalog) Err() error {
	if p := c.err.Load(); p != nil {
		return *p
	}
	return nil
}

// sortedNames validates names once and returns them sorted and unique.
// Sorted, unique input (the startup publication) is returned as is: no copy,
// no sort. Callers must not modify the result.
func sortedNames(names []string) ([]string, error) {
	sorted := true
	for i, name := range names {
		// Slash/NUL and empty components do not have an unambiguous classic path.
		if name == "" || strings.ContainsAny(name, "/\x00") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
			return nil, errors.New("v2 index: name has no classic metric path")
		}
		sorted = sorted && (i == 0 || names[i-1] < name)
	}
	if sorted {
		return names, nil
	}
	out := slices.Clone(names)
	slices.Sort(out)
	return slices.Compact(out), nil
}

// Publish replaces all names atomically. Stale publications fail closed.
// Names and the input slice may be reused after return.
func (c *Catalog) Publish(revision uint64, names []string) error {
	defer c.own()()
	old := c.Snapshot()
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	next, err := sortedNames(names)
	if err != nil {
		return err
	}
	snapshot := &Snapshot{revision: revision, n: len(next)}
	if len(next) > 0 {
		r, err := c.writeBase(context.Background(), func(w *levelWriter) error {
			for _, name := range next {
				w.add([]byte(name))
			}
			return nil
		})
		if err != nil {
			return err
		}
		snapshot.levels = []*level{{run: r}}
	}
	c.current.Store(snapshot)
	for _, l := range old.levels {
		retire(l.run)
	}
	c.dropLogs(c.seq)
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

// ApplyBeforePublishContext also cancels a large merge during daemon shutdown.
// Its cost is O(changed names * levels * log N) plus merges of the small
// heap levels; the base is merged in the background. Removed names become
// positions in their level, so no run is rewritten for a removal.
func (c *Catalog) ApplyBeforePublishContext(ctx context.Context, revision uint64, add, remove []string, before func(*Snapshot) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	defer c.own()()
	old := c.install(c.Snapshot(), false)
	if revision <= old.revision {
		return errors.New("v2 index: stale catalog revision")
	}
	snapshot, added, removed, err := c.apply(ctx, old, revision, add, remove)
	if err != nil || snapshot == nil {
		return err
	}
	if before != nil {
		if err := before(snapshot); err != nil {
			return err
		}
	}
	c.appendLog(added, removed)
	c.current.Store(snapshot)
	c.startBaseMerge(snapshot.levels)
	return nil
}

// apply returns old plus add minus remove (nil without a membership change)
// and the names it added and removed, sorted.
func (c *Catalog) apply(ctx context.Context, old *Snapshot, revision uint64, add, remove []string) (*Snapshot, []string, []string, error) {
	additions, err := sortedNames(add)
	if err != nil {
		return nil, nil, nil, err
	}
	deletions := slices.Clone(remove)
	slices.Sort(deletions)
	deletions = slices.Compact(deletions)
	// Removal wins within a batch; names already present are not new.
	removed := make([]bool, len(additions))
	k := 0
	for i, name := range additions {
		k += sort.SearchStrings(deletions[k:], name)
		removed[i] = k < len(deletions) && deletions[k] == name
	}
	drop := slices.Clone(removed)
	levels := make([]*level, 0, len(old.levels)+1)
	n, changed := old.n, false
	var added, gone []string
	for _, l := range old.levels {
		// A name is in at most one run, live or removed.
		var revive, kill []uint32
		l.find(additions, func(i, pos int) {
			drop[i] = true
			if !removed[i] && isDead(l.dead, pos) {
				revive = append(revive, uint32(pos))
				added = append(added, additions[i])
			}
		})
		l.find(deletions, func(i, pos int) {
			if !isDead(l.dead, pos) {
				kill = append(kill, uint32(pos))
				gone = append(gone, deletions[i])
			}
		})
		if len(revive)+len(kill) > 0 {
			l = &level{run: l.run, dead: mergeDead(l.dead, kill, revive)}
			n, changed = n+len(revive)-len(kill), true
		}
		levels = append(levels, l)
	}
	var fresh []string
	for i, name := range additions {
		if !drop[i] {
			fresh = append(fresh, name)
		}
	}
	if len(fresh) == 0 && !changed {
		return nil, nil, nil, nil
	}
	if len(fresh) > 0 {
		levels, n = append(levels, newLevel(fresh)), n+len(fresh)
	}
	floor := 1
	if c.job != nil {
		floor = len(c.job.inputs)
	}
	if levels, err = compact(ctx, levels, floor); err != nil {
		return nil, nil, nil, err
	}
	added = append(added, fresh...)
	slices.Sort(added)
	slices.Sort(gone)
	return &Snapshot{revision: revision, levels: levels, n: n}, added, gone, nil
}

func isDead(dead []uint32, pos int) bool {
	_, ok := slices.BinarySearch(dead, uint32(pos))
	return ok
}

// mergeDead returns dead plus kill minus revive (all sorted) as a new slice:
// published levels are immutable.
func mergeDead(dead, kill, revive []uint32) []uint32 {
	out := make([]uint32, 0, len(dead)+len(kill))
	for _, p := range dead {
		if !slices.Contains(revive, p) {
			out = append(out, p)
		}
	}
	out = append(out, kill...)
	slices.Sort(out)
	return out
}

// compact merges a level into its predecessor until every level from floor
// on is more than levelFanout times smaller than the one before it. Levels
// below floor (the base, and level 1 while the background merge reads it)
// are never merged here. The input slice is private.
func compact(ctx context.Context, levels []*level, floor int) ([]*level, error) {
	for i := len(levels) - 1; i >= floor; {
		if levels[i].live() == 0 {
			levels = slices.Delete(levels, i, i+1)
			i = len(levels) - 1
			continue
		}
		if i == floor {
			break // keep the protected predecessor unchanged
		}
		if levels[i].n*levelFanout < levels[i-1].n {
			i--
			continue
		}
		var w levelWriter
		w.r.data = make([]byte, 0, len(levels[i-1].data)+len(levels[i].data))
		if err := mergeInto(ctx, &w, levels[i-1], levels[i]); err != nil {
			return nil, err
		}
		levels[i-1] = &level{run: w.finish()}
		levels = slices.Delete(levels, i, i+1)
		i = len(levels) - 1
	}
	return levels, nil
}

// mergeInto writes the live names of a and b (b may be nil) in order.
func mergeInto(ctx context.Context, w *levelWriter, a, b *level) error {
	if b == nil {
		b = &level{run: &run{}}
	}
	x, y := levelIter{l: a}, levelIter{l: b}
	x.next()
	y.next()
	for x.valid || y.valid {
		if (x.i+y.i)&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
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
	return nil
}

// startBaseMerge rewrites the base with level 1 once level 1 reaches
// 1/levelFanout of it, once that many of its names were removed, once its
// log is as large as it (names flapping), or when a heap base (the first
// names of an Apply-built view) should be a file.
func (c *Catalog) startBaseMerge(levels []*level) {
	if c.job != nil || len(levels) == 0 {
		return
	}
	base, k := levels[0], min(len(levels), 2)
	spill := c.dir != "" && base.path == "" && time.Now().After(c.retryAt) // a heap base moves to a file
	grown := k == 2 && levels[1].n*levelFanout >= base.n
	if !spill && !grown && len(base.dead)*levelFanout < base.n && (c.logged == 0 || c.logged < len(base.data)) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &baseMerge{inputs: slices.Clone(levels[:k]), done: make(chan struct{}), cancel: cancel, path: c.nextPath(levels[k:])}
	c.job = j
	j.seq = c.seq
	go func() {
		defer close(j.done)
		var b *level
		if k == 2 {
			b = j.inputs[1]
		}
		j.out, j.err = writeRun(ctx, j.path, func(w *levelWriter) error { return mergeInto(ctx, w, j.inputs[0], b) })
	}()
}

// install publishes a finished background merge with the same membership.
// Names removed or revived in the inputs while the merge ran are carried
// over: removals as positions in the new base, revivals as a fresh level.
func (c *Catalog) install(old *Snapshot, wait bool) *Snapshot {
	j := c.job
	if j == nil {
		return old
	}
	if wait {
		<-j.done
	} else {
		select {
		case <-j.done:
		default:
			return old
		}
	}
	c.job = nil
	stale := j.out == nil || len(old.levels) < len(j.inputs) // canceled or replaced
	for i := 0; !stale && i < len(j.inputs); i++ {
		stale = old.levels[i].run != j.inputs[i].run // a Publish replaced the inputs
	}
	if stale {
		retire(j.out)
		c.startBaseMerge(old.levels)
		return old
	}
	c.noteBase(j.path, j.err)
	k := len(j.inputs)
	base := &level{run: j.out}
	var revived []string
	var buf []byte
	for i, in := range j.inputs {
		was, now := in.dead, old.levels[i].dead
		for _, p := range now {
			if !isDead(was, int(p)) {
				buf = in.at(int(p), buf)
				base.run.find([]string{string(buf)}, func(_, pos int) { base.dead = append(base.dead, uint32(pos)) })
			}
		}
		for _, p := range was {
			if !isDead(now, int(p)) {
				buf = in.at(int(p), buf)
				revived = append(revived, string(buf))
			}
		}
	}
	slices.Sort(base.dead)
	levels := []*level{base}
	if len(revived) > 0 {
		slices.Sort(revived)
		levels = append(levels, newLevel(revived))
	}
	levels = append(levels, old.levels[k:]...)
	levels, _ = compact(context.Background(), levels, 1) // cannot be canceled
	s := &Snapshot{revision: old.revision, levels: levels, n: old.n}
	c.current.Store(s)
	for _, in := range j.inputs {
		retire(in.run)
	}
	if j.out.path != "" {
		c.dropLogs(j.seq)
	}
	c.startBaseMerge(levels) // names that arrived meanwhile may be due
	return s
}

// Close stops a running background merge. The catalog stays readable.
func (c *Catalog) Close() {
	defer c.own()()
	if j := c.job; j != nil {
		j.cancel()
		<-j.done
		c.job = nil
		if j.out != nil {
			retire(j.out)
		}
	}
	if c.log != nil {
		c.log.Close()
		c.log = nil
	}
}
