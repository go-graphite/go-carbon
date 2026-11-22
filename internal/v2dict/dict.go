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
// bounded in-memory append level with a write-ahead log. Sync is the
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
	NoMerge     bool // tests: no background merges
}

// Dict is safe for concurrent use. Reads never wait for merges.
type Dict struct {
	dir string
	opt Options

	mu      sync.RWMutex // append level, log, next IDs, snapshot installs
	buf     memtable
	log     *os.File
	logSeq  uint64 // sequence of the append level (its log and level file)
	bufMin  uint64 // lowest log sequence the append level holds
	pending []byte // log records not yet written
	logRecs uint32 // records in log logSeq
	synced  uint64 // Generation(): logSeq<<32 | logRecs at the last Sync, flush or open
	scopeIx map[string]int
	nextID  []uint32
	err     error // sticky write failure
	closed  bool

	// frozenLog is the log of the frozen append level until its level file
	// is installed.
	frozenLog *os.File

	snap    atomic.Pointer[snapshot]
	flushWG sync.WaitGroup // the background write of a frozen append level
	merging atomic.Bool
	mergeMu sync.Mutex // one merge at a time
	mergeWG sync.WaitGroup
}

// snapshot is the immutable list of level files, oldest first, and the
// frozen append level a background flush is writing to the next level file
// (nil when none). A frozen append level is never written again, so readers
// search it without the lock, as they do level files.
type snapshot struct {
	frozen *memtable
	levels []*level
}

// frozenGet returns key's value in the frozen append level, or nil.
func (s *snapshot) frozenGet(key []byte) []byte {
	if s.frozen == nil {
		return nil
	}
	return s.frozen.get(key)
}

// get returns key's newest value below the append level.
func (s *snapshot) get(key []byte) (Entry, bool) {
	if s.frozen != nil {
		if v := s.frozen.get(key); v != nil {
			return decode(v), true
		}
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
	d := &Dict{dir: dir, opt: opt, scopeIx: map[string]int{}}
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
			d.bumpNext([]byte(sc.key), sc.maxID)
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
		d.mu.Lock()
		err = d.flushLocked()
		d.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	d.synced = d.logSeq<<32 | uint64(d.logRecs)
	d.maybeMerge()
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
		d.buf.put(key, val)
		d.bumpNext(key[:scopeLen(key)], binary.LittleEndian.Uint32(val))
		off += 8 + size
	}
	return off, nil
}

func (d *Dict) bumpNext(sk []byte, id uint32) {
	i, ok := d.scopeIx[string(sk)]
	if !ok {
		i = len(d.nextID)
		d.scopeIx[string(sk)] = i
		d.nextID = append(d.nextID, 0)
	}
	d.nextID[i] = max(d.nextID[i], id+1)
}

// getLocked returns key's newest value: append level, then levels newest
// first. The caller holds d.mu.
func (d *Dict) getLocked(key []byte) (Entry, bool) {
	if v := d.buf.get(key); v != nil {
		return decode(v), true
	}
	return d.snap.Load().get(key)
}

func getLevels(levels []*level, key []byte) (Entry, bool) {
	for i := len(levels) - 1; i >= 0; i-- {
		if v := levels[i].get(key); v != nil {
			return decode(v), true
		}
	}
	return Entry{}, false
}

// Lookup returns the newest entry of (group, vshard, name), deleted or not.
func (d *Dict) Lookup(group string, vshard uint16, name []byte) (Entry, bool) {
	var ab [keyBuf]byte
	key := append(scopeKey(ab[:0], group, vshard), name...)
	d.mu.RLock()
	if v := d.buf.get(key); v != nil {
		e := decode(v)
		d.mu.RUnlock()
		return e, true
	}
	s := d.snap.Load()
	d.mu.RUnlock()
	return s.get(key)
}

// LookupID appends the name of (group, vshard, id) to dst and returns its
// entry. It reports false for an ID never assigned or no longer any name's
// newest entry (a revived name's old ID, which is always deleted).
func (d *Dict) LookupID(group string, vshard uint16, id uint32, dst []byte) ([]byte, Entry, bool) {
	var ab [256]byte
	var kb [keyBuf]byte
	sk := scopeKey(ab[:0], group, vshard)
	d.mu.RLock()
	if k, v := d.buf.getID(sk, id); k != nil {
		dst = append(dst, k[len(sk):]...)
		e := decode(v)
		d.mu.RUnlock()
		return dst, e, true
	}
	s := d.snap.Load()
	d.mu.RUnlock()
	if s.frozen != nil {
		if k, v := s.frozen.getID(sk, id); k != nil {
			e := decode(v)
			d.mu.RLock()
			nv := d.buf.get(k)
			d.mu.RUnlock()
			if nv != nil {
				e = decode(nv)
			}
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
		key, val := l.at(pos, kb[:0])
		e := decode(val)
		// Only newer storage can hold a newer entry of the same name.
		d.mu.RLock()
		v := d.buf.get(key)
		d.mu.RUnlock()
		if v != nil {
			e = decode(v)
		} else if fv := s.frozenGet(key); fv != nil {
			e = decode(fv)
		} else if ne, ok := getLevels(s.levels[i+1:], key); ok {
			e = ne
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

// AppendBatch is Append of every name. It looks names up in the level
// files lock-free, in one snapshot, so concurrent seals do the costly part
// in parallel; it never takes the read lock, so seals do not ping-pong
// with each other's write holds. The append level is checked and new names
// appended under the write lock, held for at most batchNames names: a seal
// of many new names waits on the lock once per batchNames names, not once
// per name, and lookups (renders) wait at most one short hold.
func (d *Dict) AppendBatch(names []NewName) error {
	var ab [keyBuf]byte
	s := d.snap.Load()
	for i := range names {
		n := &names[i]
		key, err := n.key(ab[:0])
		if err != nil {
			return err
		}
		var ok bool
		n.Entry, ok = s.get(key)
		// Created marks a name with no live entry below the append level.
		n.Created = !ok || n.Entry.Deleted()
	}
	for len(names) > 0 {
		k := min(len(names), batchNames)
		if err := d.appendBatch(names[:k], s); err != nil {
			return err
		}
		names = names[k:]
	}
	return nil
}

// batchNames bounds one AppendBatch write-lock hold: well under 1 ms.
const batchNames = 1024

// appendBatch resolves names against the append level, and against the
// current snapshot when it is newer than s, and appends the new ones.
func (d *Dict) appendBatch(names []NewName, s *snapshot) error {
	var ab [keyBuf]byte
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.writable(); err != nil {
		return err
	}
	now := d.snap.Load()
	for i := range names {
		n := &names[i]
		key, _ := n.key(ab[:0])
		// The append level holds the newest entry; another append of the
		// name may have won since s, or an earlier name of this batch; a
		// flush moves the append level into a level s lacks.
		if v := d.buf.get(key); v != nil {
			n.Entry = decode(v)
			n.Created = n.Entry.Deleted()
		} else if now != s {
			e, ok := now.get(key)
			n.Entry, n.Created = e, !ok || e.Deleted()
		}
		if !n.Created {
			continue
		}
		sk := key[:len(key)-len(n.Name)]
		j, ok := d.scopeIx[string(sk)]
		if ok && d.nextID[j] == NoID {
			n.Created = false
			return fmt.Errorf("v2dict: vshard %d of %q is out of IDs", n.VShard, n.Group)
		}
		n.Entry = Entry{Policy: n.Policy}
		if ok {
			n.Entry.ID = d.nextID[j]
		}
		if err := d.putLocked(key, n.Entry); err != nil {
			return err
		}
		now = d.snap.Load() // a full append level froze into a newer snapshot
	}
	return nil
}

// Delete sets FlagDeleted on the name's newest entry. It reports false when
// the name is unknown or already deleted.
func (d *Dict) Delete(group string, vshard uint16, name []byte) (bool, error) {
	var ab [keyBuf]byte
	key := append(scopeKey(ab[:0], group, vshard), name...)
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.writable(); err != nil {
		return false, err
	}
	e, ok := d.getLocked(key)
	if !ok || e.Deleted() {
		return false, nil
	}
	e.Flags |= FlagDeleted
	return true, d.putLocked(key, e)
}

func (d *Dict) writable() error {
	if d.closed {
		return errors.New("v2dict: closed")
	}
	return d.err
}

func (d *Dict) putLocked(key []byte, e Entry) error {
	var vb [valueSize]byte
	val := e.put(vb[:0])
	d.buf.put(key, val)
	d.bumpNext(key[:scopeLen(key)], e.ID)
	d.pending = appendRecord(d.pending, key, val)
	d.logRecs++
	if len(d.pending) >= 64<<10 {
		if err := d.writePending(); err != nil {
			return err
		}
	}
	if d.buf.size() >= d.opt.BufferBytes {
		return d.freezeLocked()
	}
	return nil
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
	d.mu.Unlock()
	err := log.Sync()
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

// flushLocked writes the append level to a level file and installs it
// before it returns (open, Flush). The caller holds d.mu.
func (d *Dict) flushLocked() error {
	for range 2 { // a flush already running, then this append level's
		d.mu.Unlock()
		d.flushWG.Wait()
		d.mu.Lock()
		if d.err != nil || len(d.buf.recs) == 0 {
			return d.err
		}
		if err := d.freezeLocked(); err != nil {
			return err
		}
	}
	d.mu.Unlock()
	d.flushWG.Wait()
	d.mu.Lock()
	return d.err
}

// freezeLocked hands the append level to a background flush and starts the
// next log, so appends never wait on a level file write. The frozen level
// stays readable in the snapshot until its level file replaces it. While one
// flush runs, the append level keeps growing. A failure is sticky: appends
// stop, reads go on.
func (d *Dict) freezeLocked() error {
	s := d.snap.Load()
	if len(d.buf.recs) == 0 || s.frozen != nil {
		return nil
	}
	if err := d.writePending(); err != nil {
		return err
	}
	frozen := new(memtable)
	*frozen = d.buf
	d.buf = frozen.next()
	minSeq, maxSeq := d.bufMin, d.logSeq
	d.frozenLog = d.log
	d.logSeq++
	d.bufMin = d.logSeq
	d.logRecs = 0
	var err error
	if d.log, err = os.OpenFile(logPath(d.dir, d.logSeq), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644); err == nil {
		err = syncDir(d.dir)
	}
	if err != nil {
		d.err = fmt.Errorf("v2dict: new log: %w", err)
		return d.err
	}
	d.snap.Store(&snapshot{frozen: frozen, levels: s.levels})
	d.flushWG.Add(1)
	go d.writeFrozen(frozen, minSeq, maxSeq)
	return nil
}

// writeFrozen writes a frozen append level to its level file, installs it in
// the frozen level's place and removes the logs it covers.
func (d *Dict) writeFrozen(frozen *memtable, minSeq, maxSeq uint64) {
	defer d.flushWG.Done()
	l, err := writeLevel(levelPath(d.dir, minSeq, maxSeq), minSeq, maxSeq, func(w *levelWriter) {
		for _, r := range frozen.sortedRecs(nil) {
			w.add(frozen.rec(r))
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
	// Appends filled the next append level meanwhile: hand it on.
	if !d.closed && d.buf.size() >= d.opt.BufferBytes {
		_ = d.freezeLocked() // a failure is sticky in d.err
	}
	d.mu.Unlock()
	log.Close()
	for seq := minSeq; seq <= maxSeq; seq++ {
		os.Remove(logPath(d.dir, seq))
	}
	d.maybeMerge()
}

// Flush writes the append level to a level file now.
func (d *Dict) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.writable(); err != nil {
		return err
	}
	return d.flushLocked()
}

// NextID returns the ID the next new name of the vshard gets.
func (d *Dict) NextID(group string, vshard uint16) uint32 {
	var ab [256]byte
	sk := scopeKey(ab[:0], group, vshard)
	d.mu.RLock()
	defer d.mu.RUnlock()
	if i, ok := d.scopeIx[string(sk)]; ok {
		return d.nextID[i]
	}
	return 0
}

// Close waits for a running merge, flushes nothing (the log holds the
// append level) and syncs the log.
func (d *Dict) Close() error {
	d.flushWG.Wait() // the frozen level's file, which also closes its log
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	err := d.err
	if err == nil {
		if err = d.writePending(); err == nil {
			err = d.log.Sync()
		}
	}
	d.closed = true
	d.log.Close()
	d.mu.Unlock()
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
	d.mu.RLock()
	defer d.mu.RUnlock()
	st := Stats{BufferBytes: d.buf.bytes() + cap(d.pending), BufferNames: len(d.buf.recs)}
	s := d.snap.Load()
	if s.frozen != nil {
		st.BufferBytes += s.frozen.bytes()
		st.BufferNames += len(s.frozen.recs)
	}
	for _, l := range s.levels {
		st.Levels++
		st.Entries += l.n
		st.FileBytes += l.fileBytes()
	}
	return st
}
