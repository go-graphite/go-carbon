package chunkstore

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// randomStateKey returns a key under one of a few prefixes, some binary.
func randomStateKey(rng *rand.Rand) string {
	prefixes := []string{"n/", "f\x00\x01H", "f\x00\x02H", "m", ""}
	key := prefixes[rng.Intn(len(prefixes))]
	for range 1 + rng.Intn(6) {
		key += string(rune('a' + rng.Intn(4)))
	}
	if rng.Intn(4) == 0 {
		key += "\x00\xff"
	}
	return key
}

// randomMutableState returns a mutable state with random metadata, keys
// (present, empty or absent) and complete prefixes.
func randomMutableState(rng *rand.Rand, name string) *metricState {
	meta := Metadata{MetricConfig: MetricConfig{Name: name, AggregationMethod: AggregationMethod(rng.Uint32()),
		XFilesFactor: rng.Float32()}, ID: rng.Uint64(), Generation: rng.Uint64(), Revision: rng.Uint64(),
		FileMigration: FileMigrationState(rng.Intn(3)), Continuation: rng.Intn(2) == 0}
	if rng.Intn(5) == 0 {
		meta.XFilesFactor = float32(math.NaN())
	}
	if rng.Intn(3) > 0 {
		meta.LastUpdate = time.Unix(0, rng.Int63())
	}
	for range rng.Intn(6) {
		meta.Retentions = append(meta.Retentions, Retention{Step: rng.Intn(1<<20) - 100, Count: rng.Int() >> rng.Intn(60)})
	}
	st := newMetricState(meta)
	st.thaw()
	for range rng.Intn(30) {
		var v []byte
		switch rng.Intn(5) {
		case 0: // absent
		case 1:
			v = []byte{}
		default:
			v = make([]byte, rng.Intn(300))
			rng.Read(v)
		}
		st.keys[randomStateKey(rng)] = v
	}
	for _, p := range []string{"n/", "f\x00\x01H", "m"} {
		if rng.Intn(2) == 0 {
			st.complete[p] = true
		}
	}
	return st
}

// cloneMutable copies a mutable state's maps.
func cloneMutable(st *metricState) *metricState {
	c := st.clone(st.meta)
	if c.keys == nil {
		panic("clone of a mutable state is sealed")
	}
	return c
}

func probeKeys(st *metricState, rng *rand.Rand) [][]byte {
	var keys [][]byte
	for k := range st.keys {
		keys = append(keys, []byte(k), append([]byte(k), 'z'), []byte(k)[:len(k)/2])
	}
	for range 20 {
		keys = append(keys, []byte(randomStateKey(rng)))
	}
	return keys
}

// sameStateKeys checks that sealed answers every read as mutable does.
func sameStateKeys(t *testing.T, mutable, sealed *metricState, probes [][]byte) {
	t.Helper()
	for _, key := range probes {
		wv, wk := mutable.lookup(key)
		gv, gk := sealed.lookup(key)
		if wk != gk || (wv == nil) != (gv == nil) || !bytes.Equal(wv, gv) {
			t.Fatalf("lookup %q: want %v %q, got %v %q", key, wk, wv, gk, gv)
		}
	}
	for _, prefix := range []string{"n/", "f\x00\x01H", "f\x00\x02H", "m", "", "n/a"} {
		if mutable.isComplete([]byte(prefix)) != sealed.isComplete([]byte(prefix)) {
			t.Fatalf("isComplete %q differs", prefix)
		}
		wk, wv := mutable.walk([]byte(prefix))
		gk, gv := sealed.walk([]byte(prefix))
		if !reflect.DeepEqual(wk, gk) || !reflect.DeepEqual(wv, gv) {
			t.Fatalf("walk %q: want %q %q, got %q %q", prefix, wk, wv, gk, gv)
		}
		if w, g := mutable.visit([]byte(prefix)), sealed.visit([]byte(prefix)); !reflect.DeepEqual(w, g) {
			t.Fatalf("visit %q: want %q, got %q", prefix, w, g)
		}
	}
}

// decodedMeta is meta as the store decodes it (see decodeRevision).
func decodedMeta(m Metadata) Metadata {
	m.MetricConfig = cloneConfig(m.MetricConfig)
	if !m.LastUpdate.IsZero() {
		m.LastUpdate = time.Unix(0, m.LastUpdate.UnixNano())
	}
	return m
}

func sameMeta(a, b Metadata) bool {
	a.XFilesFactor, b.XFilesFactor = 0, 0
	return reflect.DeepEqual(a, b)
}

func TestStateRecordRoundTrip(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(1))
	for i := range 2000 {
		name := fmt.Sprintf("metric.%d.%s", i, strings.Repeat("x", rng.Intn(200)))
		mutable := randomMutableState(rng, name)
		want := cloneMutable(mutable)
		sealed := mutable.seal()
		if sealed.keys != nil || sealed.complete != nil {
			t.Fatal("sealed state kept its maps")
		}
		probes := probeKeys(want, rng)
		sameStateKeys(t, want, sealed, probes)

		key := name
		if i%7 == 0 {
			key = "cached-under-another-name"
		}
		n := stateRecordLen(key, sealed)
		if key == name && int64(n+stateSlotOverhead) != sealed.size {
			t.Fatalf("size %d, record %d", sealed.size, n)
		}
		rec := appendStateRecord(make([]byte, 0, n), key, sealed)
		if len(rec) != n || string(recordKey(rec)) != key {
			t.Fatalf("record length %d want %d, key %q", len(rec), n, recordKey(rec))
		}
		got := decodeStateRecord(rec)
		wantMeta := decodedMeta(sealed.meta)
		if !sameMeta(got.meta, wantMeta) || math.Float32bits(got.meta.XFilesFactor) != math.Float32bits(wantMeta.XFilesFactor) {
			t.Fatalf("metadata round trip:\nwant %+v\n got %+v", wantMeta, got.meta)
		}
		if got.size != int64(n+stateSlotOverhead) {
			t.Fatalf("decoded size %d", got.size)
		}
		sameStateKeys(t, want, got, probes)

		// A copy shares the keys until changed, and changing it leaves the
		// original alone.
		c := got.clone(got.meta)
		if c.keys != nil || &c.kenc[0] != &got.kenc[0] {
			t.Fatal("a sealed state's clone copied its keys")
		}
		c.set([]byte("n/new"), []byte{1})
		c.deletePrefix([]byte("f\x00\x01H"))
		ref := cloneMutable(want)
		ref.set([]byte("n/new"), []byte{1})
		ref.deletePrefix([]byte("f\x00\x01H"))
		sameStateKeys(t, ref, c, probes)
		sameStateKeys(t, want, got, probes)
		resealed := c.seal()
		sameStateKeys(t, ref, resealed, probes)
	}
}

// collisionHash maps names onto a few hashes, some near the index's end so
// probes wrap around.
func collisionHash(distinct uint64) func(string) uint64 {
	return func(name string) uint64 {
		var h uint64
		for i := 0; i < len(name); i++ {
			h = h*31 + uint64(name[i])
		}
		return math.MaxUint64 - h%distinct
	}
}

func cachedState(name string, id, revision uint64) *metricState {
	st := newMetricState(Metadata{MetricConfig: MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 1440}}}, ID: id, Revision: revision})
	st.set([]byte("n/main.wsp"), []byte(fmt.Sprintf("%s@%d", name, revision)))
	st.replacePrefix([]byte("f\x00\x01H"), map[string][]byte{"f\x00\x01H\x00\x00": bytes.Repeat([]byte{byte(revision)}, int(id%300))})
	return st.seal()
}

// cachedStateError checks a served state against the one cached for name.
func cachedStateError(st *metricState, name string, id, revision uint64) error {
	if st == nil {
		return fmt.Errorf("%s: no state", name)
	}
	if st.meta.Name != name || st.meta.ID != id || st.meta.Revision != revision {
		return fmt.Errorf("%s: got %s id %d revision %d, want id %d revision %d", name, st.meta.Name, st.meta.ID, st.meta.Revision, id, revision)
	}
	if v, _ := st.lookup([]byte("n/main.wsp")); string(v) != fmt.Sprintf("%s@%d", name, revision) {
		return fmt.Errorf("%s: name key %q", name, v)
	}
	if v, _ := st.lookup([]byte("f\x00\x01H\x00\x00")); !bytes.Equal(v, bytes.Repeat([]byte{byte(revision)}, int(id%300))) {
		return fmt.Errorf("%s: page %q", name, v)
	}
	if keys, values := st.walk([]byte("f\x00\x01H")); len(keys) != 1 || !bytes.Equal(values[0], bytes.Repeat([]byte{byte(revision)}, int(id%300))) {
		return errors.New(name + ": header walk differs")
	}
	return nil
}

func checkCachedState(t testing.TB, st *metricState, name string, id, revision uint64) {
	t.Helper()
	if err := cachedStateError(st, name, id, revision); err != nil {
		t.Fatal(err)
	}
}

// checkStateCacheInvariants recomputes every shard's accounting.
func checkStateCacheInvariants(t testing.TB, c *stateCache) {
	t.Helper()
	var bytesTotal, entries, arena int64
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		a := &sh.arena
		var live, occupancy int64
		used := 0
		live32 := make([]uint32, len(a.chunks))
		for s := range sh.slots {
			slot := &sh.slots[s]
			if slot.n == 0 {
				continue
			}
			used++
			live += int64(slot.n)
			live32[slot.chunk] += slot.n
			occupancy += int64(slot.n) + int64(stateSlotOverhead)
			rec := sh.record(int32(s))
			if pos, found := sh.find(slot.hash, string(recordKey(rec))); found != int32(s) || sh.table[pos] != int32(s)+1 {
				t.Fatalf("shard %d slot %d not found through the index", i, s)
			}
			if sh.entries[string(recordKey(rec))] == nil {
				entries++
			}
		}
		for name, e := range sh.entries {
			if e.pending <= 0 {
				t.Fatalf("shard %d: entry %q with pending %d", i, name, e.pending)
			}
			entries++
		}
		lru := 0
		for s := sh.lruHead; s >= 0; s = sh.slots[s].next {
			lru++
		}
		indexed := 0
		for _, s := range sh.table {
			if s != 0 {
				indexed++
			}
		}
		var written, capacity int64
		for ci, chunk := range a.chunks {
			if chunk == nil {
				continue
			}
			written += int64(len(chunk))
			capacity += int64(cap(chunk))
			if a.live[ci] != live32[ci] {
				t.Fatalf("shard %d chunk %d live %d, slots hold %d", i, ci, a.live[ci], live32[ci])
			}
			if a.live[ci] == 0 && int32(ci) != a.head {
				t.Fatalf("shard %d chunk %d holds nothing live but is kept", i, ci)
			}
		}
		if used != sh.used || used != lru || used != indexed || occupancy != sh.bytes || live != a.liveBytes || written != a.written || capacity != a.capacity {
			t.Fatalf("shard %d: used %d/%d/%d/%d bytes %d/%d live %d/%d written %d/%d capacity %d/%d", i, used, sh.used, lru, indexed,
				occupancy, sh.bytes, live, a.liveBytes, written, a.written, capacity, a.capacity)
		}
		if sh.bytes > c.shardBudget {
			t.Fatalf("shard %d holds %d bytes over its %d budget", i, sh.bytes, c.shardBudget)
		}
		if dead := a.written - a.liveBytes; dead > a.liveBytes/8 && dead > 2*int64(c.chunk) {
			t.Fatalf("shard %d: %d dead bytes for %d live", i, dead, a.liveBytes)
		}
		bytesTotal += sh.bytes
		arena += a.capacity + int64(cap(sh.slots))*int64(unsafe.Sizeof(stateSlot{})) + 4*int64(len(sh.table))
		sh.mu.Unlock()
	}
	s := c.stats()
	if s.Bytes != bytesTotal || s.Entries != entries || s.ArenaBytes != arena {
		t.Fatalf("stats %+v, recomputed bytes %d entries %d arena %d", s, bytesTotal, entries, arena)
	}
}

func TestStateCacheHashCollisions(t *testing.T) {
	t.Parallel()
	for _, distinct := range []uint64{1, 3, 64} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			c := newStateCache(1<<40, nil)
			c.hash = collisionHash(distinct)
			const stripe = 5
			revision := map[string]uint64{}
			names := make([]string, 300)
			for i := range names {
				names[i] = fmt.Sprintf("collide.%d", i)
				revision[names[i]] = 1
				c.fill(stripe, names[i], c.epoch(stripe), cachedState(names[i], uint64(i), 1))
			}
			check := func() {
				t.Helper()
				for i, name := range names {
					if r, ok := revision[name]; ok {
						checkCachedState(t, c.get(stripe, name, c.epoch(stripe)), name, uint64(i), r)
					} else if c.locked(stripe, name) != nil {
						t.Fatalf("%s: forgotten state served", name)
					}
				}
				checkStateCacheInvariants(t, c)
			}
			check()
			rng := rand.New(rand.NewSource(int64(distinct)))
			for round := range 2000 {
				i := rng.Intn(len(names))
				name := names[i]
				c.beginMutation(stripe, name)
				switch rng.Intn(3) {
				case 0: // forget: removal shifts colliding entries back
					delete(revision, name)
				default:
					revision[name]++
					c.installMutation(stripe, name, cachedState(name, uint64(i), revision[name]))
				}
				c.endMutation(stripe, name)
				if round%100 == 0 {
					check()
				}
			}
			check()
		})
	}
}

func TestStateCacheEvictionAtBudget(t *testing.T) {
	c := newStateCache(64<<20, nil)
	const stripe = 9
	var names []string
	for i := range 2000 {
		name := fmt.Sprintf("budget.%d", i)
		names = append(names, name)
		c.fill(stripe, name, c.epoch(stripe), cachedState(name, uint64(i), 1))
		// Keep the first name hot: it must survive every eviction.
		checkCachedState(t, c.get(stripe, names[0], c.epoch(stripe)), names[0], 0, 1)
	}
	checkStateCacheInvariants(t, c)
	sh := &c.shards[stripe]
	if sh.bytes > c.shardBudget || sh.bytes < c.shardBudget-int64(c.chunk) {
		t.Fatalf("shard holds %d bytes for a %d budget", sh.bytes, c.shardBudget)
	}
	// What survives is the most recently used states.
	kept := 0
	for i := len(names) - 1; i > 0; i-- {
		st := c.locked(stripe, names[i])
		if st == nil {
			break
		}
		kept++
	}
	if s := c.stats(); s.Evictions == 0 || int64(kept+1) != s.Entries {
		t.Fatalf("kept %d newest states, stats %+v", kept, s)
	}
	for i := 1; i < len(names)-kept; i++ {
		if c.locked(stripe, names[i]) != nil {
			t.Fatalf("%s survived although less recently used", names[i])
		}
	}
}

func TestStateCacheCompaction(t *testing.T) {
	t.Parallel()
	c := newStateCache(64<<20, nil)
	const stripe = 21
	sh := &c.shards[stripe]
	type held struct {
		st       *metricState
		name     string
		id, rev  uint64
		released bool
	}
	var borrowed []held
	revision := map[string]uint64{}
	var names []string
	rng := rand.New(rand.NewSource(3))
	for i := range 40 {
		name := fmt.Sprintf("compact.%d", i)
		names = append(names, name)
		revision[name] = 1
		c.fill(stripe, name, c.epoch(stripe), cachedState(name, uint64(i+100), 1))
	}
	if c.stats().Evictions != 0 {
		t.Fatal("states evicted before compaction could be observed")
	}
	chunksSeen := map[uintptr]bool{}
	for round := range 20000 {
		i := rng.Intn(len(names))
		name := names[i]
		if round%50 == 0 {
			// A decoded state keeps its record readable whatever happens to
			// the chunk it came from.
			c.lastSlotReset(stripe)
			st := c.get(stripe, name, c.epoch(stripe))
			borrowed = append(borrowed, held{st: st, name: name, id: uint64(i + 100), rev: revision[name]})
		}
		c.beginMutation(stripe, name)
		revision[name]++
		c.installMutation(stripe, name, cachedState(name, uint64(i+100), revision[name]))
		c.endMutation(stripe, name)
		for _, chunk := range sh.arena.chunks {
			if chunk != nil {
				chunksSeen[uintptr(unsafe.Pointer(unsafe.SliceData(chunk)))] = true
			}
		}
		if round%500 == 0 {
			checkStateCacheInvariants(t, c)
		}
	}
	checkStateCacheInvariants(t, c)
	for i, name := range names {
		checkCachedState(t, c.get(stripe, name, c.epoch(stripe)), name, uint64(i+100), revision[name])
	}
	for _, h := range borrowed {
		checkCachedState(t, h.st, h.name, h.id, h.rev)
	}
	s := c.stats()
	if s.CompactedBytes == 0 || s.Evictions != 0 || s.Entries != int64(len(names)) {
		t.Fatalf("stats %+v", s)
	}
	live := sh.arena.liveBytes
	if a := sh.arena.capacity; a > live+live/4+3*int64(c.chunk) {
		t.Fatalf("arena holds %d bytes for %d live", a, live)
	}
	if len(chunksSeen) < 3*len(sh.arena.chunks) {
		t.Fatalf("chunks not released and replaced: %d seen, %d slots", len(chunksSeen), len(sh.arena.chunks))
	}
	// Forgetting everything releases every chunk but the head.
	for _, name := range names {
		c.beginMutation(stripe, name)
		c.endMutation(stripe, name)
	}
	checkStateCacheInvariants(t, c)
	if s := c.stats(); s.Bytes != 0 || s.Entries != 0 || sh.arena.capacity > int64(c.chunk) {
		t.Fatalf("after forgetting every name: %+v, arena %d", s, sh.arena.capacity)
	}
}

// lastSlotReset drops a shard's undecoded state so the next hit decodes its
// record (tests only).
func (c *stateCache) lastSlotReset(stripe uint64) {
	sh := &c.shards[stripe]
	sh.mu.Lock()
	sh.last, sh.lastSlot = nil, -1
	sh.mu.Unlock()
}

// refStateCache is the cache's previous implementation, a map of pointerful
// entries with a container/list LRU, kept as the reference its replacement
// must match decision for decision (sizes are the states' own).
type refStateCache struct {
	shardBudget                                                                      int64
	shards                                                                           map[uint64]*refShard
	hits, misses, fills, installs, evictions                                         uint64
	readerMisses, lockedMisses, pendingMisses, staleMisses, invalidations, oversized uint64
	bytes, entries                                                                   int64
}

type refShard struct {
	epoch   uint64
	entries map[string]*refEntry
	lru     list.List
	bytes   int64
}

type refEntry struct {
	name        string
	pending     int
	state, next *metricState
	epoch       uint64
	elem        *list.Element
}

func (c *refStateCache) shard(stripe uint64) *refShard {
	sh := c.shards[stripe]
	if sh == nil {
		sh = &refShard{entries: map[string]*refEntry{}}
		c.shards[stripe] = sh
	}
	return sh
}

func (c *refStateCache) count(st *metricState) *metricState {
	if st == nil {
		c.misses++
	} else {
		c.hits++
	}
	return st
}

func (c *refStateCache) get(stripe uint64, name string, epoch uint64) *metricState {
	sh := c.shard(stripe)
	e := sh.entries[name]
	if e != nil && e.pending == 0 && e.state != nil && e.epoch <= epoch {
		sh.lru.MoveToFront(e.elem)
		return c.count(e.state)
	}
	c.readerMisses++
	if e != nil && e.pending > 0 {
		c.pendingMisses++
	} else if e != nil && e.state != nil && e.epoch > epoch {
		c.staleMisses++
	}
	return c.count(nil)
}

func (c *refStateCache) locked(stripe uint64, name string) *metricState {
	sh := c.shard(stripe)
	if e := sh.entries[name]; e != nil && e.state != nil {
		sh.lru.MoveToFront(e.elem)
		return c.count(e.state)
	}
	c.lockedMisses++
	return c.count(nil)
}

func (c *refStateCache) beginMutation(stripe uint64, name string) {
	sh := c.shard(stripe)
	e := sh.entries[name]
	if e == nil {
		e = &refEntry{name: name}
		sh.entries[name] = e
		c.entries++
	}
	e.pending++
	sh.epoch++
}

func (c *refStateCache) installMutation(stripe uint64, name string, st *metricState) {
	if e := c.shard(stripe).entries[name]; e != nil && e.pending > 0 {
		e.next = st
	}
}

func (c *refStateCache) endMutation(stripe uint64, name string) {
	sh := c.shard(stripe)
	e := sh.entries[name]
	if e == nil {
		return
	}
	e.pending--
	next := e.next
	e.next = nil
	sh.epoch++
	if next != nil && next.size <= c.shardBudget && e.pending == 0 {
		c.setState(sh, e, next, sh.epoch)
		c.installs++
		c.evict(sh)
		return
	}
	if next != nil && next.size > c.shardBudget {
		c.oversized++
	}
	if e.state != nil {
		c.invalidations++
	}
	c.setState(sh, e, nil, sh.epoch)
	if e.pending == 0 {
		delete(sh.entries, name)
		c.entries--
	}
}

func (c *refStateCache) fill(stripe uint64, name string, epoch uint64, st *metricState) {
	if st == nil || st.size > c.shardBudget {
		return
	}
	sh := c.shard(stripe)
	if sh.epoch != epoch || sh.entries[name] != nil {
		return
	}
	c.add(sh, name, st, epoch)
}

func (c *refStateCache) fillLocked(stripe uint64, name string, st *metricState) {
	if st.size > c.shardBudget {
		return
	}
	sh := c.shard(stripe)
	if sh.entries[name] != nil {
		return
	}
	c.add(sh, name, st, sh.epoch)
}

func (c *refStateCache) add(sh *refShard, name string, st *metricState, epoch uint64) {
	e := &refEntry{name: name}
	sh.entries[name] = e
	c.entries++
	c.setState(sh, e, st, epoch)
	c.fills++
	c.evict(sh)
}

func (c *refStateCache) setState(sh *refShard, e *refEntry, st *metricState, epoch uint64) {
	if e.state != nil {
		sh.bytes -= e.state.size
		c.bytes -= e.state.size
		sh.lru.Remove(e.elem)
		e.elem = nil
	}
	e.state, e.epoch = st, epoch
	if st != nil {
		sh.bytes += st.size
		c.bytes += st.size
		e.elem = sh.lru.PushFront(e)
	}
}

func (c *refStateCache) evict(sh *refShard) {
	for sh.bytes > c.shardBudget {
		back := sh.lru.Back()
		if back == nil {
			return
		}
		e := back.Value.(*refEntry)
		c.setState(sh, e, nil, e.epoch)
		c.evictions++
		if e.pending == 0 {
			delete(sh.entries, e.name)
			c.entries--
		}
	}
}

func sameServedState(a, b *metricState) bool {
	if a == nil || b == nil {
		return a == b
	}
	return sameMeta(a.meta, decodedMeta(b.meta)) && bytes.Equal(a.kenc, b.kenc)
}

// TestStateCacheMatchesReference drives the cache and its previous
// implementation through the same random operations on a few shards with
// small budgets, comparing every served state, every counter and every
// shard's bytes.
func TestStateCacheMatchesReference(t *testing.T) {
	t.Parallel()
	for _, seed := range []int64{1, 2, 3, 4} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			budget := int64(4+rng.Intn(60)) << 20
			c := newStateCache(budget, nil)
			if seed%2 == 0 {
				c.hash = collisionHash(16)
			}
			ref := &refStateCache{shardBudget: c.shardBudget, shards: map[uint64]*refShard{}}
			stripes := []uint64{0, 1, metricStripes - 1}
			type key struct {
				stripe uint64
				name   string
			}
			revision := map[key]uint64{}
			epochs := map[uint64][]uint64{}
			for op := range 60000 {
				stripe := stripes[rng.Intn(len(stripes))]
				name := fmt.Sprintf("m%d", rng.Intn(200))
				k := key{stripe, name}
				epoch := c.epoch(stripe)
				if epoch != ref.shard(stripe).epoch {
					t.Fatalf("op %d: epoch %d, reference %d", op, epoch, ref.shard(stripe).epoch)
				}
				if rng.Intn(3) == 0 {
					epochs[stripe] = append(epochs[stripe], epoch)
				}
				if past := epochs[stripe]; len(past) > 0 && rng.Intn(4) == 0 {
					epoch = past[rng.Intn(len(past))]
				}
				revision[k]++
				st := func() *metricState {
					id := uint64(rng.Intn(400))
					if rng.Intn(10) == 0 {
						id = 9000 + uint64(rng.Intn(3000)) // big enough to evict several
					}
					return cachedState(name, id, revision[k])
				}
				switch rng.Intn(9) {
				case 0, 1:
					got, want := c.get(stripe, name, epoch), ref.get(stripe, name, epoch)
					if !sameServedState(got, want) {
						t.Fatalf("op %d get %v: got %+v, want %+v", op, k, got, want)
					}
				case 2:
					got, want := c.locked(stripe, name), ref.locked(stripe, name)
					if !sameServedState(got, want) {
						t.Fatalf("op %d locked %v differs", op, k)
					}
				case 3:
					s := st()
					c.fill(stripe, name, epoch, s)
					ref.fill(stripe, name, epoch, s)
				case 4:
					s := st()
					c.fillLocked(stripe, name, s)
					ref.fillLocked(stripe, name, s)
				case 5:
					c.beginMutation(stripe, name)
					ref.beginMutation(stripe, name)
				case 6:
					s := st()
					c.installMutation(stripe, name, s)
					ref.installMutation(stripe, name, s)
				default:
					// Mutations end only once begun: their callers hold the stripe
					// in between.
					if e := ref.shard(stripe).entries[name]; e != nil && e.pending > 0 {
						c.endMutation(stripe, name)
						ref.endMutation(stripe, name)
					}
				}
				s := c.stats()
				want := StateCacheStats{Bytes: ref.bytes, Entries: ref.entries, Hits: ref.hits, Misses: ref.misses, Fills: ref.fills,
					Installs: ref.installs, Evictions: ref.evictions, ReaderMisses: ref.readerMisses, LockedMisses: ref.lockedMisses, PendingMisses: ref.pendingMisses, StaleMisses: ref.staleMisses, Invalidations: ref.invalidations, Oversized: ref.oversized, ArenaBytes: s.ArenaBytes, CompactedBytes: s.CompactedBytes}
				if s != want {
					t.Fatalf("op %d: stats %+v, reference %+v", op, s, want)
				}
				if c.shards[stripe].bytes != ref.shard(stripe).bytes {
					t.Fatalf("op %d: shard bytes %d, reference %d", op, c.shards[stripe].bytes, ref.shard(stripe).bytes)
				}
				if op%2000 == 0 {
					checkStateCacheInvariants(t, c)
				}
			}
			checkStateCacheInvariants(t, c)
			if s := c.stats(); s.Evictions == 0 || s.Installs == 0 || s.Fills == 0 || s.Hits == 0 {
				t.Fatalf("operations not exercised: %+v", s)
			}
		})
	}
}

// TestStateCacheConcurrentRecords races readers, which check every state they
// decode, with writers whose installs keep compaction moving records.
func TestStateCacheConcurrentRecords(t *testing.T) {
	t.Parallel()
	c := newStateCache(16<<20, nil)
	const shards = 4
	names := make([]string, 64)
	for i := range names {
		names[i] = fmt.Sprintf("race.%d", i)
	}
	var mu [shards]sync.Mutex
	var revision [64]atomic.Uint64
	var stop atomic.Bool
	var writers, readers sync.WaitGroup
	for w := range 4 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for range 20000 {
				i := rng.Intn(len(names))
				stripe := uint64(i % shards)
				mu[stripe].Lock()
				c.beginMutation(stripe, names[i])
				if st := c.locked(stripe, names[i]); st != nil {
					if err := cachedStateError(st, names[i], uint64(i), revision[i].Load()); err != nil {
						t.Error(err)
					}
				}
				r := revision[i].Add(1)
				if rng.Intn(10) > 0 {
					c.installMutation(stripe, names[i], cachedState(names[i], uint64(i), r))
				}
				c.endMutation(stripe, names[i])
				mu[stripe].Unlock()
			}
		}()
	}
	var reads atomic.Int64
	for r := range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			rng := rand.New(rand.NewSource(int64(100 + r)))
			for !stop.Load() {
				i := rng.Intn(len(names))
				stripe := uint64(i % shards)
				epoch := c.epoch(stripe)
				if st := c.get(stripe, names[i], epoch); st != nil {
					// The revision may have moved on; the state must be whole.
					if st.meta.Name != names[i] || st.meta.ID != uint64(i) {
						t.Errorf("served %s id %d for %s", st.meta.Name, st.meta.ID, names[i])
						return
					}
					if err := cachedStateError(st, names[i], uint64(i), st.meta.Revision); err != nil {
						t.Error(err)
						return
					}
					reads.Add(1)
				} else if rng.Intn(2) == 0 {
					c.fill(stripe, names[i], epoch, cachedState(names[i], uint64(i), revision[i].Load()))
				}
			}
		}()
	}
	writers.Wait()
	stop.Store(true)
	readers.Wait()
	checkStateCacheInvariants(t, c)
	if reads.Load() == 0 || c.stats().CompactedBytes == 0 {
		t.Fatalf("reads %d, stats %+v", reads.Load(), c.stats())
	}
}

func TestStateChunkSize(t *testing.T) {
	for _, tc := range []struct{ budget, want int64 }{{1, 4 << 10}, {256 << 10, 4 << 10}, {1 << 20, 16 << 10}, {8 << 20, 64 << 10}, {1 << 40, 64 << 10}} {
		if got := stateChunkSize(tc.budget); int64(got) != tc.want {
			t.Errorf("stateChunkSize(%d) = %d, want %d", tc.budget, got, tc.want)
		}
	}
	// A record larger than a chunk gets one of its own.
	c := newStateCache(1<<40, nil)
	big := newMetricState(Metadata{MetricConfig: MetricConfig{Name: "big"}})
	big.set([]byte("n/x"), make([]byte, 3*c.chunk))
	big.seal()
	c.fill(0, "big", c.epoch(0), big)
	c.fill(0, "small", c.epoch(0), cachedState("small", 1, 1))
	c.lastSlotReset(0)
	if v, _ := c.get(0, "big", c.epoch(0)).lookup([]byte("n/x")); len(v) != 3*c.chunk {
		t.Fatalf("big value %d bytes", len(v))
	}
	checkStateCacheInvariants(t, c)
	c.beginMutation(0, "big")
	c.endMutation(0, "big")
	checkStateCacheInvariants(t, c)
}
