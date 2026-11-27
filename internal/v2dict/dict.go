// Package v2dict is the v2 name dictionary: (group, vshard, name) -> (ID,
// policy index, flags), the VictoriaMetrics indexdb/mergeset analogue scoped
// to the vshard.
//
// IDs are dense per (group, vshard), assigned at append and never reused: a
// deleted name keeps its entry with FlagDeleted, and appending it again
// assigns a new ID (revival). The name -> entry mapping is the only truth:
// the newest entry of a key wins, so a deletion or a revival is a newer
// entry of the same key. ID -> name is an index of that mapping; an ID that
// is no longer any name's newest entry (revived) is reported as unknown.
//
// Storage is an LSM of name-sorted, front-coded, mmapped level files plus a
// bounded in-memory append level with a write-ahead log. The append level
// is sharded by vshard, each shard with its own lock and next IDs, as
// VictoriaMetrics shards its raw index items: a store sets one shard per
// seal owner, so concurrent seals append without a shared lock. The log
// and the level files stay one per dictionary. Sync is the
// durability point: every append before a successful Sync survives a kill.
// When the append level is full it is flushed to a new level file, and a
// background merge combines sequence-adjacent levels (newest wins).
//
// Keys sort by group, then vshard, then name, so one vshard is one key range:
// a vshard move exports it (Scan) and imports it whole (Import, no ID
// change); copying a foreign vshard's parts into a live owner re-keys through
// Rekey.
package v2dict

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/xxh3"
)

// FlagDeleted marks a deleted (expired) name. The ID is never reused.
const FlagDeleted = 1

// NoID marks a name Rekey maps to nothing (deleted in the donor).
const NoID = math.MaxUint32

// Entry is a name's dictionary value.
type Entry struct {
	ID     uint32
	Policy uint16
	Flags  uint8
}

func (e Entry) Deleted() bool { return e.Flags&FlagDeleted != 0 }

func (e Entry) put(b []byte) []byte {
	b = binary.LittleEndian.AppendUint32(b, e.ID)
	b = binary.LittleEndian.AppendUint16(b, e.Policy)
	return append(b, e.Flags)
}

func decode(v []byte) Entry {
	return Entry{ID: binary.LittleEndian.Uint32(v), Policy: binary.LittleEndian.Uint16(v[4:]), Flags: v[6]}
}

// Options tune a Dict. Zero values select the defaults.
type Options struct {
	BufferBytes int  // append level flush threshold; default 8 MiB
	Fanout      int  // a level merges into an older one once it is >= 1/Fanout of it; default 4
	Shards      int  // append level shards, by vshard modulo; default 16
	NoMerge     bool // tests: no background merges
}

// Dict is safe for concurrent use. Reads never wait for merges.
type Dict struct {
	dir string
	opt Options

	shards  []shard      // the append level; a shard lock is taken before mu
	bufSize atomic.Int64 // the shards' memtable sizes
	// freezeMu serializes freezes; a freeze takes every shard lock.
	freezeMu sync.Mutex

	mu      sync.RWMutex // log, sequences, snapshot installs, err, closed
	log     *os.File
	logSeq  uint64 // sequence of the append level (its log and level file)
	bufMin  uint64 // lowest log sequence the append level holds
	pending []byte // log records not yet written
	logRecs uint32 // records in log logSeq
	synced  uint64 // Generation(): logSeq<<32 | logRecs at the last Sync, flush or open
	err     error  // sticky write failure
	closed  bool

	// frozenLog is the log of the frozen append level until its level file
	// is installed.
	frozenLog *os.File

	snap    atomic.Pointer[snapshot]
	flushWG sync.WaitGroup // the background write of a frozen append level
	merging atomic.Bool
	mergeMu sync.Mutex // one merge at a time
	mergeWG sync.WaitGroup

	pol policyTable // Entry.Policy -> immutable suffix (policy.go)
	grp policyTable // packed-ID group index -> placement group label
}

// shard is one shard of the append level: the scopes whose vshard maps to
// it, their entries since the last freeze and their next IDs.
type shard struct {
	mu      sync.RWMutex
	buf     memtable
	scopeIx map[string]int
	nextID  []uint32
	recs    []byte // log records not yet in the log buffer
	nrecs   uint32
	// unlogged is set while recs holds records, so Sync visits only the
	// shards that have some.
	unlogged atomic.Bool
}

func shardIx(vshard uint16, n int) int { return int(vshard) % n }

// keyShard is the shard index of a dictionary key.
func keyShard(key []byte, n int) int {
	sl := scopeLen(key)
	return shardIx(binary.BigEndian.Uint16(key[sl-2:sl]), n)
}

func (d *Dict) shard(vshard uint16) *shard { return &d.shards[shardIx(vshard, len(d.shards))] }

// snapshot is the immutable list of level files, oldest first, and the
// frozen append level, by shard, a background flush is writing to the next
// level file (nil when none). A frozen append level is never written again,
// so readers search it without a lock, as they do level files.
type snapshot struct {
	frozen []*memtable
	levels []*level
}

// frozenTable returns the frozen append level shard that holds key's scope.
func (s *snapshot) frozenTable(key []byte) *memtable {
	if s.frozen == nil {
		return nil
	}
	_ = hook("frozen-read")
	return s.frozen[keyShard(key, len(s.frozen))]
}

// frozenGet returns key's value in the frozen append level, or nil.
func (s *snapshot) frozenGet(key []byte) []byte {
	if t := s.frozenTable(key); t != nil {
		return t.get(key)
	}
	return nil
}

// get returns key's newest value below the append level.
func (s *snapshot) get(key []byte) (Entry, bool) {
	if v := s.frozenGet(key); v != nil {
		return decode(v), true
	}
	return getLevels(s.levels, key)
}

func scopeKey(dst []byte, group string, vshard uint16) []byte {
	dst = append(dst, group...)
	dst = append(dst, 0)
	return binary.BigEndian.AppendUint16(dst, vshard)
}

func checkGroup(group string) error {
	if strings.IndexByte(group, 0) >= 0 || len(group)+3 > math.MaxUint16 {
		return fmt.Errorf("v2dict: invalid group %q", group)
	}
	return nil
}

func logPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016x.log", seq))
}

// Open opens or creates the dictionary in dir. Level files left behind by
// an interrupted merge are removed; the append level is rebuilt from its
// log, without a torn last record.
func Open(dir string, opt Options) (*Dict, error) {
	if opt.BufferBytes <= 0 {
		opt.BufferBytes = 8 << 20
	}
	if opt.Fanout <= 0 {
		opt.Fanout = 4
	}
	if opt.Shards <= 0 {
		opt.Shards = 16
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type span struct{ lo, hi uint64 }
	var spans []span
	var logs []uint64
	for _, de := range des {
		name := de.Name()
		var lo, hi uint64
		switch {
		case strings.HasSuffix(name, ".tmp"):
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
		case len(name) == 37 && strings.HasSuffix(name, ".lvl"):
			if _, err := fmt.Sscanf(name, "%016x-%016x.lvl", &lo, &hi); err == nil && lo <= hi {
				spans = append(spans, span{lo, hi})
			}
		case len(name) == 20 && strings.HasSuffix(name, ".log"):
			if _, err := fmt.Sscanf(name, "%016x.log", &lo); err == nil {
				logs = append(logs, lo)
			}
		}
	}
	// A merge output covers its inputs: inputs that survived a kill go.
	slices.SortFunc(spans, func(a, b span) int {
		if a.lo != b.lo {
			return cmpU64(a.lo, b.lo)
		}
		return cmpU64(b.hi, a.hi)
	})
	d := &Dict{dir: dir, opt: opt, shards: make([]shard, opt.Shards)}
	opened := false
	defer func() {
		if !opened {
			if d.frozenLog != nil {
				_ = d.frozenLog.Close()
			}
			if d.log != nil {
				_ = d.log.Close()
			}
			_ = d.pol.close()
			_ = d.grp.close()
		}
	}()
	if err := d.pol.open(dir, policyFile); err != nil {
		return nil, err
	}
	if err := d.grp.open(dir, groupFile); err != nil {
		return nil, err
	}
	for i := range d.shards {
		d.shards[i].scopeIx = map[string]int{}
	}
	var levels []*level
	var top uint64
	for _, s := range spans {
		path := levelPath(dir, s.lo, s.hi)
		if len(levels) > 0 && s.lo <= top {
			if s.hi > top {
				return nil, fmt.Errorf("v2dict: overlapping level files at %s", path)
			}
			if err := os.Remove(path); err != nil {
				return nil, err
			}
			continue
		}
		l, err := openLevel(path, s.lo, s.hi)
		if err != nil {
			return nil, err
		}
		levels = append(levels, l)
		top = s.hi
		for _, sc := range l.scopes {
			k := []byte(sc.key)
			d.shards[keyShard(k, len(d.shards))].bumpNext(k, sc.maxID)
		}
	}
	d.snap.Store(&snapshot{levels: levels})
	slices.Sort(logs)
	d.logSeq, d.bufMin = top+1, top+1
	if len(levels) == 0 {
		d.logSeq, d.bufMin = 1, 1
	}
	replayed := false
	for _, seq := range logs {
		if len(levels) > 0 && seq <= top {
			if err := os.Remove(logPath(dir, seq)); err != nil {
				return nil, err
			}
			continue
		}
		d.logRecs = 0
		good, err := d.replay(logPath(dir, seq))
		if err != nil {
			return nil, err
		}
		if !replayed {
			d.bufMin = seq
		}
		replayed = true
		d.logSeq = seq
		if err := os.Truncate(logPath(dir, seq), int64(good)); err != nil {
			return nil, err
		}
	}
	if d.log, err = os.OpenFile(logPath(dir, d.logSeq), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644); err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	if d.bufMin != d.logSeq {
		// Several logs (a kill between a flush and the next log): fold them.
		if err := d.flush(); err != nil {
			return nil, err
		}
	}
	d.synced = d.logSeq<<32 | uint64(d.logRecs)
	d.maybeMerge()
	opened = true
	return d, nil
}

func cmpU64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// A log record is v2index's name log framing: u32 payload length, u32
// CRC-32C of the payload, payload (u16 key length, key, value).
func appendRecord(dst, key, val []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 8)...)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(key)))
	dst = append(dst, key...)
	dst = append(dst, val[:valueSize]...)
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(dst)-start-8))
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.Checksum(dst[start+8:], castagnoli))
	return dst
}

// replay folds a log's whole records into the append level and returns the
// bytes they span. A torn or corrupt tail ends the log.
func (d *Dict) replay(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	off := 0
	for off+8 <= len(data) {
		size := int(binary.LittleEndian.Uint32(data[off:]))
		if size < 2+valueSize || size > len(data)-off-8 || crc32.Checksum(data[off+8:off+8+size], castagnoli) != binary.LittleEndian.Uint32(data[off+4:]) {
			break
		}
		p := data[off+8 : off+8+size]
		kl := int(binary.LittleEndian.Uint16(p))
		if 2+kl+valueSize != size || bytes.IndexByte(p[2:2+kl], 0) < 0 || scopeLen(p[2:2+kl]) > kl {
			break
		}
		key, val := p[2:2+kl], p[2+kl:]
		d.logRecs++
		sh := &d.shards[keyShard(key, len(d.shards))]
		sh.buf.put(key, val)
		sh.bumpNext(key[:scopeLen(key)], binary.LittleEndian.Uint32(val))
		off += 8 + size
	}
	d.bufSize.Store(0)
	for i := range d.shards {
		d.bufSize.Add(int64(d.shards[i].buf.size()))
	}
	return off, nil
}

func (sh *shard) bumpNext(sk []byte, id uint32) {
	i, ok := sh.scopeIx[string(sk)]
	if !ok {
		i = len(sh.nextID)
		sh.scopeIx[string(sk)] = i
		sh.nextID = append(sh.nextID, 0)
	}
	sh.nextID[i] = max(sh.nextID[i], id+1)
}

// getLocked returns key's newest value: the shard's append level, then
// levels newest first. The caller holds sh.mu.
func (d *Dict) getLocked(sh *shard, key []byte) (Entry, bool) {
	if v := sh.buf.get(key); v != nil {
		return decode(v), true
	}
	return d.snap.Load().get(key)
}

func getLevels(levels []*level, key []byte) (Entry, bool) {
	h := xxh3.Hash(key) // one hash for every immutable level probe
	for i := len(levels) - 1; i >= 0; i-- {
		if e, ok := levels[i].getHashed(key, h); ok {
			return e, true
		}
	}
	return Entry{}, false
}

// Lookup returns the newest entry of (group, vshard, name), deleted or not.
func (d *Dict) Lookup(group string, vshard uint16, name []byte) (Entry, bool) {
	var ab [keyBuf]byte
	key := append(scopeKey(ab[:0], group, vshard), name...)
	sh := d.shard(vshard)
	sh.mu.RLock()
	if v := sh.buf.get(key); v != nil {
		e := decode(v)
		sh.mu.RUnlock()
		return e, true
	}
	s := d.snap.Load()
	sh.mu.RUnlock()
	return s.get(key)
}

// LookupID appends the name of (group, vshard, id) to dst and returns its
// entry. It reports false for an ID never assigned or no longer any name's
// newest entry (a revived name's old ID, which is always deleted).
func (d *Dict) LookupID(group string, vshard uint16, id uint32, dst []byte) ([]byte, Entry, bool) {
	var ab [256]byte
	var kb [keyBuf]byte
	sk := scopeKey(ab[:0], group, vshard)
	sh := d.shard(vshard)
	sh.mu.RLock()
	if k, v := sh.buf.getID(sk, id); k != nil {
		dst = append(dst, k[len(sk):]...)
		e := decode(v)
		sh.mu.RUnlock()
		return dst, e, true
	}
	s := d.snap.Load()
	sh.mu.RUnlock()
	if s.frozen != nil {
		if k, v := s.frozenTable(sk).getID(sk, id); k != nil {
			e := decode(v)
			sh.mu.RLock()
			nv := sh.buf.get(k)
			if nv != nil {
				e = decode(nv)
			}
			sh.mu.RUnlock()
			if e.ID != id {
				return dst, Entry{}, false
			}
			return append(dst, k[len(sk):]...), e, true
		}
	}
	for i := len(s.levels) - 1; i >= 0; i-- {
		l := s.levels[i]
		pos := l.position(sk, id)
		if pos < 0 {
			continue
		}
		key, e := l.at(pos, kb[:0])
		// Only newer storage can hold a newer entry of the same name.
		sh.mu.RLock()
		v := sh.buf.get(key)
		if v != nil {
			e = decode(v)
		}
		sh.mu.RUnlock()
		if v == nil {
			if fv := s.frozenGet(key); fv != nil {
				e = decode(fv)
			} else if ne, ok := getLevels(s.levels[i+1:], key); ok {
				e = ne
			}
		}
		if e.ID != id {
			return dst, Entry{}, false
		}
		return append(dst, key[len(sk):]...), e, true
	}
	return dst, Entry{}, false
}

// Append returns the live entry of (group, vshard, name), assigning the
// next ID of the vshard with policy when the name is new or deleted. The
// entry is durable after the next successful Sync.
func (d *Dict) Append(group string, vshard uint16, name []byte, policy uint16) (Entry, bool, error) {
	// AppendBatch copies a name and keeps no reference, so it reads name in place.
	n := [1]NewName{{Group: group, VShard: vshard, Name: unsafe.String(unsafe.SliceData(name), len(name)), Policy: policy}}
	err := d.AppendBatch(n[:])
	return n[0].Entry, n[0].Created, err
}

// NewName is one name of AppendBatch: the request, then the name's live
// entry and whether the batch assigned it.
type NewName struct {
	Group   string
	VShard  uint16
	Name    string
	Policy  uint16
	Entry   Entry
	Created bool
}

func (n *NewName) key(dst []byte) ([]byte, error) {
	if err := checkGroup(n.Group); err != nil {
		return nil, err
	}
	key := append(scopeKey(dst, n.Group, n.VShard), n.Name...)
	if len(key) > math.MaxUint16 {
		return nil, fmt.Errorf("v2dict: name too long (%d bytes)", len(n.Name))
	}
	return key, nil
}

// AppendBatch is Append of every name. Each run of names of one shard, at
// most batchNames, is looked up in the level files lock-free, then checked
// against the append level and its new names appended under the shard's
// lock: seals of different shards never wait on each other, and a lookup
// (a render) waits at most one short hold of its shard.
func (d *Dict) AppendBatch(names []NewName) error {
	n := len(d.shards)
	for len(names) > 0 {
		i, k := shardIx(names[0].VShard, n), 1
		for k < min(len(names), batchNames) && shardIx(names[k].VShard, n) == i {
			k++
		}
		if err := d.appendRun(&d.shards[i], names[:k]); err != nil {
			return err
		}
		names = names[k:]
	}
	return nil
}

// batchNames bounds one AppendBatch shard-lock hold: well under 1 ms.
const batchNames = 256

// lookupLevels resolves names below the append level, in snapshot s.
// Created marks a name with no live entry there.
func lookupLevels(names []NewName, s *snapshot) error {
	var ab [keyBuf]byte
	for i := range names {
		n := &names[i]
		key, err := n.key(ab[:0])
		if err != nil {
			return err
		}
		var ok bool
		n.Entry, ok = s.get(key)
		n.Created = !ok || n.Entry.Deleted()
	}
	return nil
}

// appendRun resolves names of one shard in the level files, lock-free, then
// against the shard's append level and appends the new ones, in one hold
// of the shard lock. A freeze since the lookup moved append-level entries
// into a newer snapshot, so the names are looked up there again, lock-free,
// first; no freeze runs during the hold.
func (d *Dict) appendRun(sh *shard, names []NewName) error {
	s := d.snap.Load()
	if err := lookupLevels(names, s); err != nil {
		return err
	}
	_ = hook("append-looked-up")
	sh.mu.Lock()
	for now := d.snap.Load(); now != s; now = d.snap.Load() {
		sh.mu.Unlock()
		s = now
		_ = lookupLevels(names, s) // keys checked already
		sh.mu.Lock()
	}
	var ab [keyBuf]byte
	var err error
	for i := range names {
		n := &names[i]
		key, _ := n.key(ab[:0])
		// The append level holds the newest entry: another append of the
		// name may have won since s, or an earlier name of this batch.
		if v := sh.buf.get(key); v != nil {
			n.Entry = decode(v)
			n.Created = n.Entry.Deleted()
		}
		if !n.Created {
			continue
		}
		sk := key[:len(key)-len(n.Name)]
		j, ok := sh.scopeIx[string(sk)]
		if ok && sh.nextID[j] == NoID {
			n.Created = false
			err = fmt.Errorf("v2dict: vshard %d of %q is out of IDs", n.VShard, n.Group)
			break
		}
		n.Entry = Entry{Policy: n.Policy}
		if ok {
			n.Entry.ID = sh.nextID[j]
		}
		d.put(sh, key, n.Entry)
	}
	err = errors.Join(err, d.commit(sh))
	sh.mu.Unlock()
	d.maybeFreeze()
	return err
}

// Delete sets FlagDeleted on the name's newest entry. It reports false when
// the name is unknown or already deleted.
func (d *Dict) Delete(group string, vshard uint16, name []byte) (bool, error) {
	var ab [keyBuf]byte
	key := append(scopeKey(ab[:0], group, vshard), name...)
	sh := d.shard(vshard)
	sh.mu.Lock()
	if err := d.writableShared(); err != nil {
		sh.mu.Unlock()
		return false, err
	}
	e, ok := d.getLocked(sh, key)
	if !ok || e.Deleted() {
		sh.mu.Unlock()
		return false, nil
	}
	e.Flags |= FlagDeleted
	d.put(sh, key, e)
	err := d.commit(sh)
	sh.mu.Unlock()
	d.maybeFreeze()
	return true, err
}

// SetPolicy records policy on the name's live entry when it has none: an
// entry written before the policy table existed, whose policy only a retired
// catalog knew. A recorded policy never changes. It reports whether it
// changed the entry; the entry is durable after the next Sync.
func (d *Dict) SetPolicy(group string, vshard uint16, name []byte, policy uint16) (bool, error) {
	var ab [keyBuf]byte
	key := append(scopeKey(ab[:0], group, vshard), name...)
	sh := d.shard(vshard)
	sh.mu.Lock()
	if err := d.writableShared(); err != nil {
		sh.mu.Unlock()
		return false, err
	}
	e, ok := d.getLocked(sh, key)
	if !ok || e.Deleted() || e.Policy != 0 || policy == 0 {
		sh.mu.Unlock()
		return false, nil
	}
	e.Policy = policy
	d.put(sh, key, e)
	err := d.commit(sh)
	sh.mu.Unlock()
	d.maybeFreeze()
	return true, err
}

func (d *Dict) writable() error {
	if d.closed {
		return errors.New("v2dict: closed")
	}
	return d.err
}

// writableShared is writable for a caller that does not hold d.mu.
func (d *Dict) writableShared() error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.writable()
}

// put sets key's entry in the shard's append level and queues its log
// record for commit. The caller holds sh.mu.
func (d *Dict) put(sh *shard, key []byte, e Entry) {
	var vb [valueSize]byte
	val := e.put(vb[:0])
	size := sh.buf.size()
	sh.buf.put(key, val)
	d.bufSize.Add(int64(sh.buf.size() - size))
	sh.bumpNext(key[:scopeLen(key)], e.ID)
	sh.recs = appendRecord(sh.recs, key, val)
	sh.nrecs++
}

// shardLogBytes bounds the log records a shard keeps between holds. Below
// it a hold takes no store-wide lock, so producers admitting new names in
// different shards never wait on each other; Sync and freeze log the rest.
const shardLogBytes = 16 << 10

// commit moves the shard's log records to the log buffer once they reach
// shardLogBytes. The caller holds sh.mu.
func (d *Dict) commit(sh *shard) error {
	if len(sh.recs) < shardLogBytes {
		if sh.nrecs > 0 && !sh.unlogged.Load() {
			sh.unlogged.Store(true)
		}
		return nil
	}
	d.mu.Lock()
	err := d.logShard(sh)
	d.mu.Unlock()
	return err
}

// logShard appends sh's records to the log buffer, so a freeze (which holds
// every shard lock) puts every record of the append level it freezes in the
// log it closes. The caller holds sh.mu and d.mu.
func (d *Dict) logShard(sh *shard) error {
	if sh.nrecs == 0 {
		return nil
	}
	err := d.writable()
	if err == nil {
		d.pending = append(d.pending, sh.recs...)
		d.logRecs += sh.nrecs
		if len(d.pending) >= 64<<10 {
			err = d.writePending()
		}
	}
	sh.recs, sh.nrecs = sh.recs[:0], 0
	sh.unlogged.Store(false)
	return err
}

// logShards logs the records every shard still keeps (Sync, Close).
func (d *Dict) logShards() error {
	var err error
	for i := range d.shards {
		sh := &d.shards[i]
		if !sh.unlogged.Load() {
			continue
		}
		sh.mu.Lock()
		d.mu.Lock()
		err = errors.Join(err, d.logShard(sh))
		d.mu.Unlock()
		sh.mu.Unlock()
	}
	return err
}

// maybeFreeze freezes the append level once it reaches BufferBytes.
func (d *Dict) maybeFreeze() {
	if d.bufSize.Load() >= int64(d.opt.BufferBytes) {
		_ = d.freeze(false) // a failure is sticky in d.err
	}
}

func (d *Dict) writePending() error {
	if len(d.pending) == 0 {
		return nil
	}
	if _, err := d.log.Write(d.pending); err != nil {
		d.err = fmt.Errorf("v2dict: log write: %w", err)
		return d.err
	}
	d.pending = d.pending[:0]
	return nil
}

// Sync makes every earlier append and delete durable: the point admission
// waits on before it writes a journal record that uses a new ID. The fsync
// runs without the lock, so appends and lookups go on meanwhile; a frozen
// append level's log is synced too until its level file is installed.
func (d *Dict) Sync() error {
	if err := d.logShards(); err != nil {
		return err
	}
	d.mu.Lock()
	if err := d.writable(); err != nil {
		d.mu.Unlock()
		return err
	}
	if err := d.writePending(); err != nil {
		d.mu.Unlock()
		return err
	}
	log, frozenLog, gen := d.log, d.frozenLog, d.logSeq<<32|uint64(d.logRecs)
	if gen <= d.synced {
		// An earlier Sync, flush or open already covers every record: the
		// producers' drains sync once per new-name batch, not per call.
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()
	err := hook("log-sync")
	if err == nil {
		err = log.Sync()
	}
	if err == nil && frozenLog != nil {
		// Closed: its level file is installed, which is durable.
		if err = frozenLog.Sync(); errors.Is(err, os.ErrClosed) {
			err = nil
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil && d.err == nil {
		d.err = fmt.Errorf("v2dict: log sync: %w", err)
	}
	if d.err == nil {
		d.synced = max(d.synced, gen)
	}
	return d.err
}

// Generation is the durable dictionary state: the log sequence and the
// records in that log at the last Sync, flush or open. It only grows, also
// across restarts. Every ID appended before the Sync that produced it
// resolves in any dictionary at that generation or later, so a part records
// the generation its IDs need and a copy target checks it has at least that.
func (d *Dict) Generation() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.synced
}

// flush writes the append level to a level file and installs it before it
// returns (open, Flush).
func (d *Dict) flush() error {
	for range 2 { // a flush already running, then this append level's
		d.flushWG.Wait()
		if err := d.freeze(true); err != nil {
			return err
		}
	}
	d.flushWG.Wait()
	return d.writableShared()
}

// freeze hands the append level, every shard of it, to a background flush
// and starts the next log; a full append level waits for the previous flush;
// force freezes a non-empty append level below BufferBytes. The next log is
// created before the shard locks are taken. The frozen level stays readable
// in the snapshot until its level file replaces it. No shard or dictionary
// lock is held while waiting for that flush. A failure is sticky: appends
// stop, reads go on.
func (d *Dict) freeze(force bool) error {
	d.freezeMu.Lock()
	defer d.freezeMu.Unlock()
	size := d.bufSize.Load()
	if size == 0 || (!force && size < int64(d.opt.BufferBytes)) {
		return nil
	}
	if d.snap.Load().frozen != nil {
		_ = hook("append-wait")
	}
	d.flushWG.Wait()
	d.mu.RLock()
	seq, err := d.logSeq+1, d.writable()
	d.mu.RUnlock()
	if err != nil {
		return err
	}
	log, err := os.OpenFile(logPath(d.dir, seq), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err == nil {
		if err = syncDir(d.dir); err != nil {
			log.Close()
		}
	}
	for i := range d.shards {
		d.shards[i].mu.Lock()
	}
	d.mu.Lock()
	if err != nil {
		if d.err == nil {
			d.err = fmt.Errorf("v2dict: new log: %w", err)
		}
		err = d.err
	} else if err = d.writable(); err == nil {
		for i := range d.shards {
			err = errors.Join(err, d.logShard(&d.shards[i]))
		}
		err = errors.Join(err, d.writePending())
	}
	if err == nil {
		s := d.snap.Load()
		frozen := make([]*memtable, len(d.shards))
		for i := range d.shards {
			sh := &d.shards[i]
			frozen[i] = new(memtable)
			*frozen[i] = sh.buf
			sh.buf = frozen[i].next()
		}
		d.bufSize.Store(0)
		minSeq, maxSeq := d.bufMin, d.logSeq
		d.frozenLog, d.log = d.log, log
		d.logSeq, d.bufMin, d.logRecs = seq, seq, 0
		d.snap.Store(&snapshot{frozen: frozen, levels: s.levels})
		d.flushWG.Add(1)
		go d.writeFrozen(frozen, minSeq, maxSeq)
	} else if log != nil {
		log.Close()
		os.Remove(logPath(d.dir, seq))
	}
	d.mu.Unlock()
	for i := range d.shards {
		d.shards[i].mu.Unlock()
	}
	return err
}

// writeFrozen writes a frozen append level to its level file, installs it in
// the frozen level's place and removes the logs it covers.
func (d *Dict) writeFrozen(frozen []*memtable, minSeq, maxSeq uint64) {
	defer d.flushWG.Done()
	l, err := writeLevelKind(levelPath(d.dir, minSeq, maxSeq), minSeq, maxSeq, false, func(w *levelWriter) {
		// Shards hold disjoint scopes: one key order over all of them.
		n := 0
		for _, t := range frozen {
			n += len(t.recs)
		}
		order := make([]uint64, 0, n) // shard<<32 | record
		for i, t := range frozen {
			for r := range t.recs {
				order = append(order, uint64(i)<<32|uint64(r))
			}
		}
		rec := func(x uint64) ([]byte, []byte) { return frozen[x>>32].rec(uint32(x)) }
		slices.SortFunc(order, func(a, b uint64) int {
			ka, _ := rec(a)
			kb, _ := rec(b)
			return bytes.Compare(ka, kb)
		})
		w.reserve(len(order))
		for _, x := range order {
			w.add(rec(x))
		}
	})
	d.mu.Lock()
	if err != nil {
		if d.err == nil {
			d.err = fmt.Errorf("v2dict: flush: %w", err)
		}
		d.mu.Unlock()
		return
	}
	cur := d.snap.Load()
	d.snap.Store(&snapshot{levels: append(slices.Clip(cur.levels), l)})
	// Everything the frozen level held is durable in its level file.
	d.synced = max(d.synced, (maxSeq+1)<<32)
	if err := hook("flush-installed"); err != nil {
		d.err = err
		d.mu.Unlock()
		return
	}
	log := d.frozenLog
	d.frozenLog = nil
	d.mu.Unlock()
	log.Close()
	for seq := minSeq; seq <= maxSeq; seq++ {
		os.Remove(logPath(d.dir, seq))
	}
	d.maybeMerge()
}

// Flush writes the append level to a level file now.
func (d *Dict) Flush() error {
	if err := d.writableShared(); err != nil {
		return err
	}
	return d.flush()
}

// NextID returns the ID the next new name of the vshard gets.
func (d *Dict) NextID(group string, vshard uint16) uint32 {
	var ab [256]byte
	sk := scopeKey(ab[:0], group, vshard)
	sh := d.shard(vshard)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if i, ok := sh.scopeIx[string(sk)]; ok {
		return sh.nextID[i]
	}
	return 0
}

// Close waits for a running merge, flushes nothing (the log holds the
// append level) and syncs the log.
func (d *Dict) Close() error {
	d.flushWG.Wait() // the frozen level's file, which also closes its log
	logErr := d.logShards()
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	err := errors.Join(d.err, logErr)
	if err == nil {
		if err = d.writePending(); err == nil {
			err = d.log.Sync()
		}
	}
	d.closed = true
	d.log.Close()
	d.mu.Unlock()
	err = errors.Join(err, d.pol.close(), d.grp.close())
	d.flushWG.Wait()
	d.mergeWG.Wait()
	return err
}

// Stats describe the dictionary's footprint.
type Stats struct {
	Levels      int
	Entries     int // level entries (a key can be in several levels until merged)
	FileBytes   int // mapped level bytes (page cache)
	BufferBytes int // append level heap bytes
	BufferNames int
}

func (d *Dict) Stats() Stats {
	var st Stats
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.RLock()
		st.BufferBytes += sh.buf.bytes() + cap(sh.recs)
		st.BufferNames += len(sh.buf.recs)
		sh.mu.RUnlock()
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	st.BufferBytes += cap(d.pending)
	s := d.snap.Load()
	for _, t := range s.frozen {
		st.BufferBytes += t.bytes()
		st.BufferNames += len(t.recs)
	}
	for _, l := range s.levels {
		st.Levels++
		st.Entries += l.n
		st.FileBytes += l.fileBytes()
	}
	return st
}
