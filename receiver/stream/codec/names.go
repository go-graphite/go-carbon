package codec

import (
	"bytes"
	"hash/maphash"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

const nameBlockBits = 4
const nameBlockSize = 1 << nameBlockBits
const deadName = ^uint32(0)

// Tokens cached on records must stay distinct across application reloads.
var nameGeneration atomic.Uint64 //nolint:gochecknoglobals // Process-wide token identity prevents cross-table aliasing.

type sharedName struct {
	route, a, b                    uint64
	generation                     uint64
	refs, touched, address, length uint32
}
type sharedBlock [nameBlockSize]sharedName

// NameTable gives names process-local, generation-checked IDs. It holds either
// full bytes (exact comparison), or two independently seeded 64-bit fingerprints.
// Fingerprint mode has a birthday bound n*(n-1)/2^129, assuming uniform hashes.
// No names or pointers are retained in that mode. A token is only a hint until
// acquired by a connection; expiry while queued is safe and re-resolves it.
type NameTable struct {
	shards    []nameShard
	seeds     [2]maphash.Seed
	full      bool
	lastSweep atomic.Int64
	overhead  int
}
type nameShard struct {
	mu                             sync.Mutex
	blocks                         []atomic.Pointer[sharedBlock]
	index                          []uint32
	limit, used, count, tombstones int
	free                           []uint32
	cursor                         uint32
	quantum                        int
	budget, bytes                  int
	pages                          [][]byte
	cells                          [13]uint32
	evictions                      uint64
}
type NameStats struct {
	Bytes, Names, NameBytes int
	Evictions               uint64
}

// NewNameTable bounds all table/index/arena capacities. Connection maps and
// predictors have their own byte shares. Zero bytes selects 16 MiB.
func NewNameTable(budget int, fullNames bool) (*NameTable, error) {
	if budget == 0 {
		budget = 16 << 20
	}
	if budget < 1024 {
		return nil, ErrLimit
	}
	shards := 1
	for shards < 32 && budget/shards >= 1<<20 {
		shards *= 2
	}
	t := &NameTable{shards: make([]nameShard, shards), seeds: [2]maphash.Seed{maphash.MakeSeed(), maphash.MakeSeed()}, full: fullNames}
	t.overhead = 128 + shards*256
	for i := range t.shards {
		s := &t.shards[i]
		s.budget = (budget - t.overhead) / shards
		// Full-name arenas are lazy and consume the remaining half of the budget.
		allocation := s.budget
		s.quantum = min(1<<16, 1<<max(4, bits.Len(uint(s.budget/16))-1))
		if fullNames && s.budget >= 4096 {
			allocation /= 2
		}
		capacity := max(16, allocation/72/16*16)
		if capacity >= 1<<19 {
			capacity = (1 << 19) - 1
		}
		size := 16
		for size*3/4 < capacity {
			size *= 2
		}
		for size*4+((capacity+15)/16)*16*52+((capacity+15)/16)*8 > allocation {
			capacity -= 16
			if capacity < 16 {
				return nil, ErrLimit
			}
		}
		s.limit = capacity
		s.free = make([]uint32, 0, capacity)
		s.index = make([]uint32, size)
		s.blocks = make([]atomic.Pointer[sharedBlock], (capacity+15)/16)
		s.bytes = len(s.index)*4 + len(s.blocks)*8 + cap(s.free)*4
	}
	return t, nil
}
func (t *NameTable) shard(hash uint64) *nameShard {
	return &t.shards[mixHash(hash)&uint64(len(t.shards)-1)]
}
func (s *nameShard) entry(id uint32) *sharedName {
	block := s.blocks[id>>nameBlockBits].Load()
	if block == nil {
		return nil
	}
	return &block[id&(nameBlockSize-1)]
}

// Valid requires no lock and does not acquire a reference. Only generation is
// read concurrently; held connection references prevent its slot being reused.
func (t *NameTable) Valid(token uint64, hash uint64) bool {
	if token == 0 {
		return false
	}
	s := t.shard(hash)
	id := ((uint32(token) & 0xffffff) - 1) >> 5
	if int(id) >= s.limit {
		return false
	}
	e := s.entry(id)
	return e != nil && atomic.LoadUint64(&e.generation) == token>>24
}
func (t *NameTable) Resolve(name []byte, hash uint64) uint64 {
	a, b := uint64(0), uint64(0)
	if !t.full {
		a = maphash.Bytes(t.seeds[0], name)
		b = maphash.Bytes(t.seeds[1], name)
	}
	s := t.shard(hash)
	s.mu.Lock()
	defer s.mu.Unlock()
	mask := len(s.index) - 1
	empty := -1
	for i := int(mixHash(hash>>5)) & mask; ; i = (i + 1) & mask {
		id := s.index[i]
		if id == 0 {
			if empty < 0 {
				empty = i
			}
			break
		}
		if id == deadName {
			if empty < 0 {
				empty = i
			}
			continue
		}
		e := s.entry(id - 1)
		if e.route == hash && ((!t.full && e.a == a && e.b == b) || (t.full && bytes.Equal(s.name(e), name))) {
			if e.refs == 0 {
				e.touched = uint32(time.Now().Unix())
			}
			return atomic.LoadUint64(&e.generation)<<24 | uint64(((id-1)<<5)|uint32(mixHash(hash)&uint64(len(t.shards)-1))) + 1
		}
	}
	if len(s.free) == 0 && s.used >= s.limit {
		s.reclaim(64, true)
		empty = -1
	}
	if empty < 0 {
		for i := int(mixHash(hash>>5)) & mask; ; i = (i + 1) & mask {
			if s.index[i] == 0 || s.index[i] == deadName {
				empty = i
				break
			}
		}
	}
	var id uint32
	if n := len(s.free); n > 0 {
		id = s.free[n-1]
		s.free = s.free[:n-1]
	} else {
		if s.used >= s.limit {
			return 0
		}
		id = uint32(s.used)
		if s.blocks[id>>nameBlockBits].Load() == nil {
			if s.bytes+16*48 > s.budget {
				return 0
			}
			s.blocks[id>>nameBlockBits].Store(new(sharedBlock))
			s.bytes += 16 * 48
		}
		s.used++
	}
	e := s.entry(id)
	if t.full {
		address, ok := s.allocate(name)
		if !ok {
			s.reclaim(64, true)
			address, ok = s.allocate(name)
			empty = -1
		}
		if !ok {
			s.free = append(s.free, id)
			return 0
		}
		e.address = address
		e.length = uint32(len(name))
	}
	gen := uint64(0)
	if nameGeneration.Load() < 1<<39 {
		gen = nameGeneration.Add(1)
	}
	if gen == 0 || gen >= 1<<39 {
		if e.length != 0 {
			s.releaseBytes(e.address, int(e.length))
			e.length = 0
		}
		s.free = append(s.free, id)
		return 0
	}
	e.route = hash
	e.a = a
	e.b = b
	e.refs = 0
	e.touched = uint32(time.Now().Unix())
	atomic.StoreUint64(&e.generation, gen)
	if empty < 0 {
		for i := int(mixHash(hash>>5)) & mask; ; i = (i + 1) & mask {
			if s.index[i] == 0 || s.index[i] == deadName {
				empty = i
				break
			}
		}
	}
	if s.index[empty] == deadName {
		s.tombstones--
	}
	s.index[empty] = id + 1
	s.count++
	return gen<<24 | uint64((id<<5)|uint32(mixHash(hash)&uint64(len(t.shards)-1))) + 1
}
func (t *NameTable) Acquire(token uint64, hash uint64) bool {
	if token == 0 {
		return false
	}
	s := t.shard(hash)
	s.mu.Lock()
	defer s.mu.Unlock()
	id := ((uint32(token) & 0xffffff) - 1) >> 5
	if int(id) >= s.limit {
		return false
	}
	e := s.entry(id)
	if e == nil || atomic.LoadUint64(&e.generation) != token>>24 {
		return false
	}
	e.refs++
	return true
}
func (t *NameTable) Release(token uint64) {
	if token != 0 {
		t.ReleaseKey(uint32(token) & 0xffffff)
	}
}
func (t *NameTable) ReleaseKey(key uint32) {
	if key == 0 {
		return
	}
	key--
	s := &t.shards[key&31]
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entry(key >> 5)
	if e == nil || e.refs == 0 {
		panic("unbalanced stream name release")
	}
	e.refs--
	if e.refs == 0 {
		e.touched = uint32(time.Now().Unix())
	}
}

// Sweep reclaims unreferenced names after a minute; pressure may reclaim them
// immediately. Live connection references are never evicted.
func (t *NameTable) Sweep() {
	now := time.Now().Unix()
	last := t.lastSweep.Load()
	if now-last < 60 || !t.lastSweep.CompareAndSwap(last, now) {
		return
	}
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		s.sweep(false)
		s.mu.Unlock()
	}
}
func (s *nameShard) sweep(pressure bool) { s.reclaim(len(s.index), pressure) }
func (s *nameShard) reclaim(work int, pressure bool) {
	now := uint32(time.Now().Unix())
	for range min(work, len(s.index)) {
		i := int(s.cursor) & (len(s.index) - 1)
		s.cursor++
		id := s.index[i]
		if id == 0 || id == deadName {
			continue
		}
		e := s.entry(id - 1)
		if e.refs == 0 && (pressure || now-e.touched >= 60) {
			if e.length != 0 {
				s.releaseBytes(e.address, int(e.length))
				e.length = 0
			}
			atomic.StoreUint64(&e.generation, 0)
			s.index[i] = deadName
			s.tombstones++
			s.count--
			s.free = append(s.free, id-1)
			s.evictions++
		}
	}
	// Rebuild in place: tombstones cannot accumulate with lifetime churn.
	if s.tombstones > len(s.index)/4 {
		clear(s.index)
		s.tombstones = 0
		for id := 0; id < s.used; id++ {
			e := s.entry(uint32(id))
			if atomic.LoadUint64(&e.generation) == 0 {
				continue
			}
			mask := len(s.index) - 1
			for i := int(mixHash(e.route>>5)) & mask; ; i = (i + 1) & mask {
				if s.index[i] == 0 {
					s.index[i] = uint32(id + 1)
					break
				}
			}
		}
	}
}
func (t *NameTable) Stats() NameStats {
	r := NameStats{Bytes: t.overhead}
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		r.Bytes += s.bytes
		r.Names += s.count
		r.Evictions += s.evictions
		for _, p := range s.pages {
			r.NameBytes += len(p)
		}
		s.mu.Unlock()
	}
	return r
}
func (s *nameShard) name(e *sharedName) []byte {
	p := e.address - 1
	return s.pages[p>>16][p&65535 : uint32(p&65535)+e.length]
}
func (s *nameShard) allocate(name []byte) (uint32, bool) {
	class := max(4, bits.Len(uint(len(name)-1)))
	if class > 16 {
		return 0, false
	}
	size := 1 << class
	c := class - 4
	if s.cells[c] == 0 {
		pageSize := max(size, s.quantum)
		extra := pageSize + (nextCapacity(len(s.pages)+1, cap(s.pages))-cap(s.pages))*24
		if s.bytes+extra > s.budget || len(s.pages) >= 1<<16 {
			return 0, false
		}
		if s.cells[c] == 0 {
			page := make([]byte, pageSize)
			base := uint32(len(s.pages)) << 16
			pageCapacity := nextCapacity(len(s.pages)+1, cap(s.pages))
			if pageCapacity > cap(s.pages) {
				pages := make([][]byte, len(s.pages), pageCapacity)
				copy(pages, s.pages)
				s.bytes += (pageCapacity - cap(s.pages)) * 24
				s.pages = pages
			}
			s.pages = append(s.pages, page)
			s.bytes += pageSize
			for offset := pageSize - size; offset >= 0; offset -= size {
				address := base + uint32(offset) + 1
				putCell(page[offset:], s.cells[c])
				s.cells[c] = address
			}
		}
	}
	address := s.cells[c]
	p := address - 1
	cell := s.pages[p>>16][p&65535:]
	s.cells[c] = getCell(cell)
	copy(cell, name)
	return address, true
}
func putCell(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}
func getCell(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
func (s *nameShard) releaseBytes(address uint32, n int) {
	class := max(4, bits.Len(uint(n-1))) - 4
	p := address - 1
	b := s.pages[p>>16][p&65535:]
	putCell(b, s.cells[class])
	s.cells[class] = address
}
