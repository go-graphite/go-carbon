package chunkstore

import (
	"bytes"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/helper"
)

// stateCache keeps, for recently written or read metrics, the committed state
// that each update and fetch otherwise reads back from Pebble: the catalog
// Metadata (catalog and revision keys) and the continuation keys describing
// the generation's virtual files (see ContinuationStateKeys) — names,
// manifests and header pages. Each of those reads positions an iterator or
// opens a table in every LSM level; on dense they were about a quarter of all
// CPU, re-reading what the same process had just committed.
//
// Consistency rests on the rule that every mutation of a metric holds its
// metricMu stripe (lockMetricMutation, or the purge loop). Under that stripe,
// beginMutation marks the name pending before anything commits, and
// endMutation either installs the state the mutation committed (an update's
// write-through, see installMutation) or forgets the name.
//
// Checkpoint fills additionally exclude every name mutated since open,
// including revision-neutral relayout. Restored records stay provisional
// until their revision is validated by the loader or first use.
//
// A writer holding the stripe may use the entry's state as the latest
// committed state: no other mutation can commit meanwhile.
//
// A reader without the stripe (Fetch) captures its shard's epoch before
// opening its read view, and uses an entry only if it is not pending and was
// installed at or before that epoch. Its commit then preceded the view, and
// any later mutation began before the reader looked (it commits only after
// beginning), so it would show as pending or as a newer install. A reader
// that missed fills the entry with what its view read only while the shard's
// epoch still equals the captured one and the name has no entry: no mutation
// of the shard began or ended since, so the view shows the latest state.
//
// States are stored sealed, as records in pointer-free memory (see
// stateShard and state_slab.go); a hit decodes the metadata and reads the
// keys in place. Optional state slots retain one exact descriptor and tail
// group per file/archive, using the same epochs and byte budget.
type stateCache struct {
	validate          func(string, *metricState) bool
	checkpointChanges *catalogMembership
	shardBudget       int64
	keys              ContinuationStateKeys
	// hash keys the shards' indexes (helper.HashString; tests collide it).
	hash func(string) uint64
	// chunk is the size of the shards' arena chunks.
	chunk  int
	shards [metricStripes]stateShard

	hits, misses, fills, installs, evictions, compactedBytes atomic.Uint64
	bytes, entries, arenaBytes                               atomic.Int64
	readerMisses, lockedMisses, pendingMisses, staleMisses   atomic.Uint64
	invalidations, oversized, slotRefusals                   atomic.Uint64
	// prewarmed, prewarmSkipped and prewarmFailed count the names
	// PrewarmMetadata filled, passed over and could not read.
	prewarmed, prewarmSkipped, prewarmFailed     atomic.Uint64
	prewarmBytes, prewarmReadBytes, prewarmNanos atomic.Uint64
}

type stateShard struct {
	mu sync.Mutex
	// epoch advances, under mu, on every mutation begin and end of a name in
	// the shard. Readers load it without mu.
	epoch atomic.Uint64
	// entries holds the names pending a mutation, a few at a time; a pending
	// name may also have a state in a slot.
	entries map[string]*stateEntry
	// table indexes the slots holding a state by hash, with linear probing;
	// it holds slot numbers plus one, 0 for an empty bucket.
	table []int32
	slots []stateSlot
	// used counts indexed slots; freeSlot heads the free slot list.
	used     int
	freeSlot int32
	// lruHead and lruTail are the most and least recently used slots.
	lruHead, lruTail int32
	arena            stateArena
	// bytes is the occupancy of the shard's states: their records and slots.
	bytes int64
	// last is the state put in slot lastSlot, served without decoding while
	// it stays there.
	last     *metricState
	lastSlot int32
}

// stateEntry marks a name pending a mutation.
type stateEntry struct {
	name    string
	pending int
	// next is installed by endMutation; set only after a successful commit.
	next *metricState
}

// metricState is one metric's committed catalog metadata and the subset of its
// continuation state keys known for it. A mutable state holds its keys in
// maps; a sealed one in kenc (see encodeStateKeys), and methods that change
// it first decode them into maps that alias kenc's immutable bytes.
type metricState struct {
	meta Metadata
	// keys maps a known state key (a continuation suffix) to its value; a nil
	// value records the key as absent. Nil when sealed.
	keys map[string][]byte
	// complete lists prefixes whose every present key is in keys: any other
	// key under them is absent.
	complete map[string]bool
	// kenc encodes a sealed state's keys and complete prefixes.
	kenc []byte
	// packed is kenc's smaller, lossless representation in arena records.
	// Decoded states keep kenc ready for reads without another store lookup.
	packed []byte
	// size is the cache memory of a sealed state: its record and slot.
	size int64
}

// ContinuationStateKeys is implemented by a codec whose continuation keys
// include small, frequently read state that the store may cache in memory
// (see stateCache). Values of other keys are always read from the database.
type ContinuationStateKeys interface {
	// StateKey reports whether a continuation suffix is cacheable state.
	StateKey(suffix []byte) bool
	// StatePrefix reports whether a prefix of state keys may be cached whole,
	// so that a walk of it can be answered from memory.
	StatePrefix(prefix []byte) bool
}

// metadataCacheTestSize, when GO_CARBON_TEST_METADATA_CACHE holds a size,
// enables the cache in stores opened without one, so whole test suites can run
// both ways.
var metadataCacheTestSize, _ = strconv.ParseInt(os.Getenv("GO_CARBON_TEST_METADATA_CACHE"), 10, 64)

// unpackedStates keeps newly sealed states' keys raw (see SetStatePacking).
var unpackedStates atomic.Bool

// SetStatePacking chooses whether states sealed from now on Snappy-pack their
// keys in the metadata cache. Cached states keep their form until rewritten;
// both decode, so it can change on a running store.
func SetStatePacking(on bool) { unpackedStates.Store(!on) }

func newStateCache(budget int64, keys ContinuationStateKeys) *stateCache {
	if budget <= 0 {
		return nil
	}
	c := &stateCache{shardBudget: max(budget/metricStripes, 1), keys: keys, hash: helper.HashString}
	c.chunk = stateChunkSize(c.shardBudget)
	for i := range c.shards {
		sh := &c.shards[i]
		sh.entries = make(map[string]*stateEntry)
		sh.freeSlot, sh.lruHead, sh.lruTail, sh.lastSlot, sh.arena.head = -1, -1, -1, -1, -1
	}
	return c
}

// epoch returns the shard epoch a reader of name captures before its view.
func (c *stateCache) epoch(stripe uint64) uint64 {
	return c.shards[stripe].epoch.Load()
}

// get returns name's state for a reader that captured epoch before opening
// its view, or nil.
func (c *stateCache) get(stripe uint64, name string, epoch uint64) *metricState {
	sh := &c.shards[stripe]
	hash := c.hash(name)
	var st *metricState
	var rec []byte
	var recordEpoch uint64
	sh.mu.Lock()
	if len(sh.entries) == 0 || sh.entries[name] == nil {
		if _, s := sh.find(hash, name); s >= 0 && sh.slots[s].epoch&^provisionalState <= epoch {
			recordEpoch = sh.slots[s].epoch
			st, rec = sh.stateAt(s)
			sh.touch(s)
		} else if s >= 0 {
			c.staleMisses.Add(1)
		}
	} else {
		c.pendingMisses.Add(1)
	}
	sh.mu.Unlock()
	if st == nil && rec == nil {
		c.readerMisses.Add(1)
	}
	st = c.hit(st, rec)
	if st != nil && recordEpoch&provisionalState != 0 {
		return c.validateCheckpoint(stripe, name, st, recordEpoch)
	}
	return st
}

// locked returns name's latest committed state for a writer holding the
// name's stripe in a mutation, or nil.
func (c *stateCache) locked(stripe uint64, name string) *metricState {
	sh := &c.shards[stripe]
	hash := c.hash(name)
	var st *metricState
	var rec []byte
	var recordEpoch uint64
	sh.mu.Lock()
	if _, s := sh.find(hash, name); s >= 0 {
		recordEpoch = sh.slots[s].epoch
		st, rec = sh.stateAt(s)
		sh.touch(s)
	}
	sh.mu.Unlock()
	if st == nil && rec == nil {
		c.lockedMisses.Add(1)
	}
	st = c.hit(st, rec)
	if st != nil && recordEpoch&provisionalState != 0 {
		return c.validateCheckpoint(stripe, name, st, recordEpoch)
	}
	return st
}

// hit counts a lookup and decodes a record it found, outside the shard lock.
func (c *stateCache) hit(st *metricState, rec []byte) *metricState {
	if st == nil && rec != nil {
		st = decodeStateRecord(rec)
	}
	if st == nil {
		c.misses.Add(1)
	} else {
		c.hits.Add(1)
	}
	return st
}

// beginMutation marks name pending; call it after taking the name's stripe and
// before anything commits.
func (c *stateCache) beginMutation(stripe uint64, name string) {
	if c.checkpointChanges != nil {
		c.checkpointChanges.add(name)
	}
	sh := &c.shards[stripe]
	hash := c.hash(name)
	sh.mu.Lock()
	e := sh.entries[name]
	if e == nil {
		e = &stateEntry{name: strings.Clone(name)}
		sh.entries[e.name] = e
		if _, s := sh.find(hash, name); s < 0 {
			c.entries.Add(1)
		}
	}
	e.pending++
	sh.epoch.Add(1)
	sh.mu.Unlock()
}

// installMutation records st as the state the current mutation of name left
// committed. Call it under the stripe, after the commit succeeded or when the
// mutation committed nothing.
func (c *stateCache) installMutation(stripe uint64, name string, st *metricState) {
	sh := &c.shards[stripe]
	sh.mu.Lock()
	if e := sh.entries[name]; e != nil && e.pending > 0 {
		e.next = st
	}
	sh.mu.Unlock()
}

// endMutation installs what installMutation recorded, or forgets name; call it
// before releasing the stripe, whether the mutation committed or not.
func (c *stateCache) endMutation(stripe uint64, name string) {
	sh := &c.shards[stripe]
	hash := c.hash(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e := sh.entries[name]
	if e == nil {
		return
	}
	e.pending--
	next := e.next
	e.next = nil
	epoch := sh.epoch.Add(1)
	next = c.fitSlots(sh, hash, name, next)
	if next != nil && next.size <= c.shardBudget && e.pending == 0 {
		// The name keeps one entry: its state replaces its pending marker.
		delete(sh.entries, name)
		c.put(sh, hash, name, next, epoch)
		c.installs.Add(1)
		c.evict(sh)
		return
	}
	if next != nil && next.size > c.shardBudget {
		c.oversized.Add(1)
	}
	pos, s := sh.find(hash, name)
	if s >= 0 {
		c.invalidations.Add(1)
		c.remove(sh, pos, s)
		c.compact(sh)
	}
	if e.pending == 0 {
		delete(sh.entries, name)
		c.entries.Add(-1)
	}
}

// fill records st, read by a view opened after capturing epoch, unless the
// shard changed since or name already has an entry.
func (c *stateCache) fill(stripe uint64, name string, epoch uint64, st *metricState) {
	if st == nil {
		return
	}
	sh := &c.shards[stripe]
	hash := c.hash(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.epoch.Load() != epoch || sh.entries[name] != nil {
		return
	}
	if _, s := sh.find(hash, name); s >= 0 {
		return
	}
	st = c.fitSlots(sh, hash, name, st)
	if st.size > c.shardBudget {
		return
	}
	c.put(sh, hash, name, st, epoch)
	c.entries.Add(1)
	c.fills.Add(1)
	c.evict(sh)
}

// prewarmResult is the outcome of a prewarm fill.
type prewarmResult int

const (
	prewarmFilled prewarmResult = iota
	// prewarmCached: the name has an entry, installed, filled or pending.
	prewarmCached
	// prewarmRaced: a mutation of the shard began or ended since epoch.
	prewarmRaced
	// prewarmFull: the shard has no room left for st.
	prewarmFull
)

// has reports whether name has an entry: a state, or a pending mutation.
func (c *stateCache) has(stripe uint64, name string) bool {
	sh := &c.shards[stripe]
	hash := c.hash(name)
	sh.mu.Lock()
	_, pending := sh.entries[name]
	_, s := sh.find(hash, name)
	sh.mu.Unlock()
	return pending || s >= 0
}

// prewarm records st like fill, for a reader that captured epoch before
// opening its view, but only into room the shard has free: it never evicts.
func (c *stateCache) prewarm(stripe uint64, name string, epoch uint64, st *metricState) prewarmResult {
	sh := &c.shards[stripe]
	hash := c.hash(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.entries[name] != nil {
		return prewarmCached
	}
	if _, s := sh.find(hash, name); s >= 0 {
		return prewarmCached
	}
	if sh.epoch.Load() != epoch {
		return prewarmRaced
	}
	if sh.bytes+int64(stateRecordLen(name, st)+stateSlotOverhead) > c.shardBudget {
		return prewarmFull
	}
	c.put(sh, hash, name, st, epoch)
	c.entries.Add(1)
	return prewarmFilled
}

// budget is the cache's size in bytes.
func (c *stateCache) budget() int64 { return c.shardBudget * metricStripes }

// fillLocked records st, read while holding name's stripe outside any
// mutation, unless name already has an entry. It is the latest committed
// state, but a reader whose view predates the latest commit must not use it:
// the current epoch is at least that commit's end.
func (c *stateCache) fillLocked(stripe uint64, name string, st *metricState) {
	sh := &c.shards[stripe]
	hash := c.hash(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.entries[name] != nil {
		return
	}
	if _, s := sh.find(hash, name); s >= 0 {
		return
	}
	st = c.fitSlots(sh, hash, name, st)
	if st.size > c.shardBudget {
		return
	}
	c.put(sh, hash, name, st, sh.epoch.Load())
	c.entries.Add(1)
	c.fills.Add(1)
	c.evict(sh)
}

// evict drops least recently used states over the shard's budget. A pending
// entry keeps its marker; only its state may go.
func (c *stateCache) evict(sh *stateShard) {
	for sh.bytes > c.shardBudget && sh.lruTail >= 0 {
		s := sh.lruTail
		pending := sh.entries[string(recordKey(sh.record(s)))] != nil
		c.remove(sh, sh.position(s), s)
		c.evictions.Add(1)
		if !pending {
			c.entries.Add(-1)
		}
	}
	c.compact(sh)
}

func newMetricState(meta Metadata) *metricState {
	meta.MetricConfig = cloneConfig(meta.MetricConfig)
	meta.Name = strings.Clone(meta.Name)
	return &metricState{meta: meta, kenc: emptyStateKeys}
}

// clone returns a mutable copy of st with meta replacing its metadata. A
// sealed st's copy shares its keys until changed.
func (st *metricState) clone(meta Metadata) *metricState {
	c := newMetricState(meta)
	if st == nil {
		return c
	}
	if st.keys == nil {
		c.kenc = st.kenc
		c.packed = st.packed
		return c
	}
	c.keys = make(map[string][]byte, len(st.keys))
	c.complete = make(map[string]bool, len(st.complete))
	c.kenc = nil
	for k, v := range st.keys {
		c.keys[k] = v
	}
	for p := range st.complete {
		c.complete[p] = true
	}
	return c
}

// seal encodes st's keys and computes its size; st must not change afterwards.
func (st *metricState) seal() *metricState {
	if st.keys != nil {
		st.kenc = encodeStateKeys(st.keys, st.complete)
		st.keys, st.complete = nil, nil
	}
	if st.packed == nil && len(st.kenc) >= 256 && !unpackedStates.Load() {
		st.packed = packStateKeys(st.kenc)
	}
	st.size = int64(stateRecordLen(st.meta.Name, st) + stateSlotOverhead)
	return st
}

// thaw decodes a sealed st's keys into maps before a change.
func (st *metricState) thaw() {
	if st.keys != nil {
		return
	}
	kenc := st.kenc
	n := stateKeyCount(kenc)
	st.keys = make(map[string][]byte, n)
	st.complete = make(map[string]bool)
	for i := range n {
		k, p := stateKeyAt(kenc, i)
		st.keys[unsafeString(k)] = stateValueAt(kenc, p)
	}
	for p, end := statePrefixes(kenc); p < end; {
		var prefix []byte
		prefix, p = nextStatePrefix(kenc, p)
		st.complete[unsafeString(prefix)] = true
	}
	st.kenc = nil
	st.packed = nil
}

// lookup reports whether key's value is known: present with value, or absent.
func (st *metricState) lookup(key []byte) (value []byte, known bool) {
	if st.keys != nil {
		if v, ok := st.keys[string(key)]; ok {
			return v, true
		}
		for p := range st.complete {
			if len(key) >= len(p) && string(key[:len(p)]) == p {
				return nil, true
			}
		}
		return nil, false
	}
	kenc := st.kenc
	if i := stateSearch(kenc, key); i < stateKeyCount(kenc) {
		if k, p := stateKeyAt(kenc, i); bytes.Equal(k, key) {
			return stateValueAt(kenc, p), true
		}
	}
	for p, end := statePrefixes(kenc); p < end; {
		var prefix []byte
		prefix, p = nextStatePrefix(kenc, p)
		if bytes.HasPrefix(key, prefix) {
			return nil, true
		}
	}
	return nil, false
}

// isComplete reports whether every present key under prefix is known.
func (st *metricState) isComplete(prefix []byte) bool {
	if st.keys != nil {
		return st.complete[string(prefix)]
	}
	for p, end := statePrefixes(st.kenc); p < end; {
		var complete []byte
		complete, p = nextStatePrefix(st.kenc, p)
		if bytes.Equal(complete, prefix) {
			return true
		}
	}
	return false
}

// visit lists the present keys under a complete prefix in key order.
func (st *metricState) visit(prefix []byte) (keys []string) {
	if st.keys == nil {
		found, _ := st.walk(prefix)
		for _, k := range found {
			keys = append(keys, string(k))
		}
		return keys
	}
	for k, v := range st.keys {
		if v != nil && strings.HasPrefix(k, string(prefix)) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// walk returns copies of the present keys under a complete prefix, in key
// order, and of their values. They share one allocation, each slice capped
// at its own bytes.
func (st *metricState) walk(prefix []byte) (keys, values [][]byte) {
	var names []string
	count, size := 0, 0
	if st.keys == nil {
		kenc := st.kenc
		for i, n := stateSearch(kenc, prefix), stateKeyCount(kenc); i < n; i++ {
			k, p := stateKeyAt(kenc, i)
			if !bytes.HasPrefix(k, prefix) {
				break
			}
			if v := stateValueAt(kenc, p); v != nil {
				count++
				size += len(k) + len(v)
			}
		}
	} else {
		for k, v := range st.keys {
			if v != nil && strings.HasPrefix(k, string(prefix)) {
				names = append(names, k)
				size += len(k) + len(v)
			}
		}
		slices.Sort(names)
		count = len(names)
	}
	if count == 0 {
		return nil, nil
	}
	slots := make([][]byte, 2*count)
	keys, values = slots[:count:count], slots[count:]
	buf := make([]byte, 0, size)
	add := func(i int, k, v []byte) {
		start := len(buf)
		buf = append(buf, k...)
		keys[i] = buf[start:len(buf):len(buf)]
		start = len(buf)
		buf = append(buf, v...)
		values[i] = buf[start:len(buf):len(buf)]
	}
	if st.keys == nil {
		kenc := st.kenc
		for i, j := stateSearch(kenc, prefix), 0; j < count; i++ {
			k, p := stateKeyAt(kenc, i)
			if v := stateValueAt(kenc, p); v != nil {
				add(j, k, v)
				j++
			}
		}
	} else {
		for i, k := range names {
			add(i, []byte(k), st.keys[k])
		}
	}
	return keys, values
}

// set records key's value; nil records it absent.
func (st *metricState) set(key []byte, value []byte) {
	st.thaw()
	st.keys[string(key)] = value
}

// deletePrefix records every known key under prefix as absent. A complete
// prefix that prefix covers stays complete, now holding no present key.
func (st *metricState) deletePrefix(prefix []byte) {
	st.thaw()
	for k := range st.keys {
		if strings.HasPrefix(k, string(prefix)) {
			st.keys[k] = nil
		}
	}
}

// replacePrefix records a walk's result: the present keys under a cacheable
// prefix are exactly found.
func (st *metricState) replacePrefix(prefix []byte, found map[string][]byte) {
	st.thaw()
	for k := range st.keys {
		if strings.HasPrefix(k, string(prefix)) {
			delete(st.keys, k)
		}
	}
	for k, v := range found {
		st.keys[k] = v
	}
	st.complete[string(prefix)] = true
}

// StateCacheStats reports the metadata cache's counters (zero when disabled).
// Bytes is the occupancy of the cached states, records and slots, which the
// budget bounds; ArenaBytes all the memory the cache holds for them,
// including dead records not yet compacted, unwritten chunk space, slot
// arrays and indexes. CompactedBytes counts record bytes compaction moved.
type StateCacheStats struct {
	Bytes, Entries, ArenaBytes                               int64
	Hits, Misses, Fills, Installs, Evictions, CompactedBytes uint64
	// Reader misses include pending mutations and states newer than the read view.
	ReaderMisses, LockedMisses, PendingMisses, StaleMisses uint64
	// Invalidations drop existing states without an install; Oversized counts refused installs.
	Invalidations, Oversized, SlotRefusals uint64
	// Prewarmed counts states PrewarmMetadata loaded; PrewarmSkipped names
	// it passed over (cached or pending already, not in the catalog, raced by
	// a mutation, or no room left); PrewarmFailed names it could not read.
	Prewarmed, PrewarmSkipped, PrewarmFailed uint64
	// PrewarmBytes counts installed state bytes; PrewarmReadBytes counts
	// logical key/value reads, even refused fills; PrewarmNanos includes pacing.
	PrewarmBytes, PrewarmReadBytes, PrewarmNanos uint64
}

func (c *stateCache) stats() StateCacheStats {
	if c == nil {
		return StateCacheStats{}
	}
	return StateCacheStats{Bytes: c.bytes.Load(), Entries: c.entries.Load(), ArenaBytes: c.arenaBytes.Load(), Hits: c.hits.Load(), Misses: c.misses.Load(),
		ReaderMisses: c.readerMisses.Load(), LockedMisses: c.lockedMisses.Load(), PendingMisses: c.pendingMisses.Load(), StaleMisses: c.staleMisses.Load(),
		Invalidations: c.invalidations.Load(), Oversized: c.oversized.Load(), SlotRefusals: c.slotRefusals.Load(),
		Fills: c.fills.Load(), Installs: c.installs.Load(), Evictions: c.evictions.Load(), CompactedBytes: c.compactedBytes.Load(),
		PrewarmBytes: c.prewarmBytes.Load(), PrewarmReadBytes: c.prewarmReadBytes.Load(), PrewarmNanos: c.prewarmNanos.Load(),
		Prewarmed: c.prewarmed.Load(), PrewarmSkipped: c.prewarmSkipped.Load(), PrewarmFailed: c.prewarmFailed.Load()}
}
