package v2dict

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"os"
	"slices"
	"sort"
)

// Iter walks one vshard's names in sorted order over a fixed view: the
// level snapshot and a copy of the vshard's append-level records, both
// taken by NewIter. The newest entry of each name is returned, deleted or
// not. Not safe for concurrent use.
type Iter struct {
	sk     []byte
	lv     []levelIter // oldest first
	arena  []byte      // append-level records of the vshard, key order
	recs   []uint32
	mi     int
	mvalid bool
	mkey   []byte
	mval   []byte
	cur    []byte
	val    [valueSize]byte
	valid  bool
}

// NewIter returns an iterator over (group, vshard); position it with SeekGE.
func (d *Dict) NewIter(group string, vshard uint16) *Iter {
	it := &Iter{sk: scopeKey(nil, group, vshard)}
	sh := d.shard(vshard)
	sh.mu.Lock()
	s := d.snap.Load()
	var frozen []uint32
	ft := s.frozenTable(it.sk)
	if ft != nil {
		frozen = ft.sortedRecs(it.sk)
	}
	buf := &sh.buf
	order := buf.order()
	lo := sort.Search(len(order), func(j int) bool { k, _ := buf.rec(order[j]); return bytes.Compare(k, it.sk) >= 0 })
	hi := lo
	for hi < len(order) {
		if k, _ := buf.rec(order[hi]); !bytes.HasPrefix(k, it.sk) {
			break
		}
		hi++
	}
	order = order[lo:hi]
	// The append level is newer than the frozen one: it wins on equal keys.
	for len(order) > 0 || len(frozen) > 0 {
		var k, v []byte
		switch {
		case len(frozen) == 0:
			k, v = buf.rec(order[0])
			order = order[1:]
		case len(order) == 0:
			k, v = ft.rec(frozen[0])
			frozen = frozen[1:]
		default:
			bk, bv := buf.rec(order[0])
			fk, fv := ft.rec(frozen[0])
			switch c := bytes.Compare(bk, fk); {
			case c < 0:
				k, v = bk, bv
				order = order[1:]
			case c > 0:
				k, v = fk, fv
				frozen = frozen[1:]
			default:
				k, v = bk, bv
				order, frozen = order[1:], frozen[1:]
			}
		}
		it.recs = append(it.recs, uint32(len(it.arena)))
		it.arena = binary.LittleEndian.AppendUint16(it.arena, uint16(len(k)))
		it.arena = append(it.arena, k...)
		it.arena = append(it.arena, v...)
	}
	sh.mu.Unlock()
	it.lv = make([]levelIter, len(s.levels))
	for i, l := range s.levels {
		it.lv[i].l = l
	}
	return it
}

func (it *Iter) mrec(j int) (key, val []byte) {
	off := int(it.recs[j])
	kl := int(binary.LittleEndian.Uint16(it.arena[off:]))
	return it.arena[off+2 : off+2+kl], it.arena[off+2+kl : off+2+kl+valueSize]
}

func (it *Iter) loadMem() {
	if it.mvalid = it.mi < len(it.recs); it.mvalid {
		it.mkey, it.mval = it.mrec(it.mi)
	}
}

// SeekGE positions the iterator on the first name >= name.
func (it *Iter) SeekGE(name []byte) bool {
	key := append(slices.Clip(it.sk), name...)
	for i := range it.lv {
		it.lv[i].seekGE(key)
	}
	it.mi = sort.Search(len(it.recs), func(j int) bool { k, _ := it.mrec(j); return bytes.Compare(k, key) >= 0 })
	it.loadMem()
	return it.pick()
}

// pick selects the smallest key; on a tie the newest source wins.
func (it *Iter) pick() bool {
	var best, val []byte
	if it.mvalid {
		best, val = it.mkey, it.mval
	}
	for i := len(it.lv) - 1; i >= 0; i-- {
		l := &it.lv[i]
		if l.valid && bytes.HasPrefix(l.key, it.sk) && (best == nil || bytes.Compare(l.key, best) < 0) {
			best, val = l.key, l.val
		}
	}
	if it.valid = best != nil; it.valid {
		it.cur = append(it.cur[:0], best...)
		copy(it.val[:], val)
	}
	return it.valid
}

// Next moves to the next name.
func (it *Iter) Next() bool {
	if !it.valid {
		return false
	}
	if it.mvalid && bytes.Equal(it.mkey, it.cur) {
		it.mi++
		it.loadMem()
	}
	for i := range it.lv {
		if l := &it.lv[i]; l.valid && bytes.Equal(l.key, it.cur) {
			l.next()
		}
	}
	return it.pick()
}

// Name is valid until the next move.
func (it *Iter) Name() []byte { return it.cur[len(it.sk):] }
func (it *Iter) Entry() Entry { return decode(it.val[:]) }

// Scan calls fn for each name of (group, vshard) with prefix, in sorted
// order, until fn returns false: the find/glob seek and the export of a
// vshard move.
func (d *Dict) Scan(group string, vshard uint16, prefix []byte, fn func(name []byte, e Entry) bool) {
	it := d.NewIter(group, vshard)
	for ok := it.SeekGE(prefix); ok && bytes.HasPrefix(it.Name(), prefix); ok = it.Next() {
		if !fn(it.Name(), it.Entry()) {
			return
		}
	}
}

// Entries is Scan as an iterator over the whole vshard (the export side of
// a move).
func (d *Dict) Entries(group string, vshard uint16) iter.Seq2[[]byte, Entry] {
	return func(yield func([]byte, Entry) bool) { d.Scan(group, vshard, nil, yield) }
}

// Import loads a vshard exported by another dictionary into this one,
// keeping its IDs: the target side of a vshard move. The vshard must be
// empty here. Names must be strictly increasing and IDs unique. The entries
// are durable after the next successful Sync.
func (d *Dict) Import(group string, vshard uint16, entries iter.Seq2[[]byte, Entry]) error {
	if err := checkGroup(group); err != nil {
		return err
	}
	if d.NextID(group, vshard) != 0 {
		return fmt.Errorf("v2dict: import into non-empty vshard %d of %q", vshard, group)
	}
	sk := scopeKey(nil, group, vshard)
	var prev, key []byte
	var seen []uint64
	var err error
	entries(func(name []byte, e Entry) bool {
		if prev != nil && bytes.Compare(name, prev) <= 0 {
			err = fmt.Errorf("v2dict: import names out of order: %q after %q", name, prev)
			return false
		}
		if w := int(e.ID / 64); w >= len(seen) {
			seen = append(seen, make([]uint64, w+1-len(seen))...)
		}
		if seen[e.ID/64]&(1<<(e.ID%64)) != 0 || e.ID == NoID {
			err = fmt.Errorf("v2dict: import duplicate or invalid ID %d", e.ID)
			return false
		}
		seen[e.ID/64] |= 1 << (e.ID % 64)
		prev = append(prev[:0], name...)
		key = append(append(key[:0], sk...), name...)
		sh := d.shard(vshard)
		sh.mu.Lock()
		d.put(sh, key, e)
		err = d.commit(sh)
		sh.mu.Unlock()
		d.maybeFreeze()
		return err == nil
	})
	return err
}

// Rekey maps a donor vshard's IDs to this dictionary's IDs for the same
// names, appending names this dictionary lacks: the re-key step for copying
// a replica's parts into a live owner of the vshard. The result is indexed
// by donor ID; donor IDs that are deleted or unassigned map to NoID.
func (d *Dict) Rekey(group string, vshard uint16, donor iter.Seq2[[]byte, Entry]) ([]uint32, error) {
	var m []uint32
	var err error
	donor(func(name []byte, de Entry) bool {
		if int(de.ID) >= len(m) {
			n := len(m)
			m = append(m, make([]uint32, int(de.ID)+1-n)...)
			for i := n; i < len(m); i++ {
				m[i] = NoID
			}
		}
		if de.Deleted() {
			return true
		}
		var e Entry
		if e, _, err = d.Append(group, vshard, name, de.Policy); err != nil {
			return false
		}
		if e.Policy != de.Policy {
			err = fmt.Errorf("v2dict: rekey %q: policy %d here, %d in donor", name, e.Policy, de.Policy)
			return false
		}
		m[de.ID] = e.ID
		return true
	})
	return m, err
}

// pickMerge returns the first of the newest levels to merge: the oldest
// level i that the levels after it together reach 1/fanout of. -1: none.
func pickMerge(levels []*level, fanout int) int {
	sum := 0
	for i := len(levels) - 2; i >= 0; i-- {
		sum += levels[i+1].n
		if sum*fanout >= levels[i].n {
			// Keep extending while the merged run reaches the older level.
			j := i
			for j > 0 && (sum+levels[j].n)*fanout >= levels[j-1].n {
				sum += levels[j].n
				j--
			}
			return j
		}
	}
	return -1
}

// mergeStart preserves compact prefixes unless the normal fanout requires them.
// With no fanout merge due, compress only the uncompressed suffix. Selecting
// L2 before writing avoids a second full L1 output and needs no admission timer.
func mergeStart(levels []*level, fanout int) int {
	if i := pickMerge(levels, fanout); i >= 0 {
		return i
	}
	for i, l := range levels {
		if !l.compact {
			return i
		}
	}
	return -1
}

func (d *Dict) maybeMerge() {
	if d.opt.NoMerge || !d.merging.CompareAndSwap(false, true) {
		return
	}
	d.mergeWG.Add(1)
	go func() {
		defer d.mergeWG.Done()
		for {
			for d.mergeOnce() {
			}
			d.merging.Store(false)
			d.mu.RLock()
			stop := d.closed || d.err != nil // a failed merge is not retried in a loop
			d.mu.RUnlock()
			if stop || mergeStart(d.snap.Load().levels, d.opt.Fanout) < 0 || !d.merging.CompareAndSwap(false, true) {
				return
			}
		}
	}()
}

// Merge runs merges until none is due and waits for them (tests, benches).
func (d *Dict) Merge() {
	d.flushWG.Wait()
	d.mergeWG.Wait()
	for d.mergeOnce() {
	}
}

// MergeAll merges every level into one.
func (d *Dict) MergeAll() error {
	d.flushWG.Wait() // a running flush installs its level first
	d.mergeWG.Wait()
	levels := d.snap.Load().levels
	if len(levels) < 2 {
		return nil
	}
	return d.merge(levels)
}

func (d *Dict) mergeOnce() bool {
	levels := d.snap.Load().levels
	i := mergeStart(levels, d.opt.Fanout)
	if i < 0 {
		return false
	}
	return d.mergeKind(levels[i:], true) == nil
}

// merge writes the newest entry of every key of in (sequence-adjacent
// levels) to one level, installs it in their place and removes them.
func (d *Dict) merge(in []*level) error { return d.mergeKind(in, true) }

func (d *Dict) mergeKind(in []*level, compact bool) error {
	if len(in) == 0 {
		return nil
	}
	d.mergeMu.Lock()
	defer d.mergeMu.Unlock()
	d.mu.RLock()
	closed := d.closed
	d.mu.RUnlock()
	if closed || slices.Index(d.snap.Load().levels, in[0]) < 0 {
		return fmt.Errorf("v2dict: merge inputs changed") // an earlier merge took them
	}
	its := make([]levelIter, len(in))
	for i, l := range in {
		its[i].l = l
		its[i].next()
	}
	var cur []byte
	out, err := writeLevelKind(levelPath(d.dir, in[0].minSeq, in[len(in)-1].maxSeq), in[0].minSeq, in[len(in)-1].maxSeq, compact, func(w *levelWriter) {
		n := 0
		for _, l := range in {
			n += l.n
		}
		w.reserve(n)
		for {
			best := -1
			for i := len(its) - 1; i >= 0; i-- {
				if its[i].valid && (best < 0 || bytes.Compare(its[i].key, its[best].key) < 0) {
					best = i
				}
			}
			if best < 0 {
				return
			}
			w.add(its[best].key, its[best].val)
			cur = append(cur[:0], its[best].key...)
			for i := range its {
				if its[i].valid && bytes.Equal(its[i].key, cur) {
					its[i].next()
				}
			}
		}
	})
	if err != nil {
		d.mu.Lock()
		if d.err == nil && !d.closed {
			d.err = fmt.Errorf("v2dict: merge: %w", err)
		}
		d.mu.Unlock()
		return err
	}
	d.mu.Lock()
	cur2 := d.snap.Load().levels
	at := slices.Index(cur2, in[0])
	if d.closed || at < 0 || len(cur2) < at+len(in) || !slices.Equal(cur2[at:at+len(in)], in) {
		d.mu.Unlock()
		// A single-level rewrite has already replaced its input at this path.
		// Preserve that equivalent durable file if Close wins publication.
		if out.path != in[0].path {
			os.Remove(out.path)
		}
		return fmt.Errorf("v2dict: merge inputs changed")
	}
	next := slices.Concat(cur2[:at], []*level{out}, cur2[at+len(in):])
	d.snap.Store(&snapshot{frozen: d.snap.Load().frozen, levels: next})
	d.mu.Unlock()
	if err := hook("merge-installed"); err != nil {
		return err
	}
	for _, l := range in {
		if l.path != out.path {
			os.Remove(l.path)
		}
	}
	return nil
}
