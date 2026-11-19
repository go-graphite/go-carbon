package partlog

import (
	"slices"

	"github.com/zeebo/xxh3"
)

// NameHash is computed once by a name-routing caller and reused by the catalog.
// A hash is only a probe hint: the complete name always decides identity.
func NameHash(key string) uint64 { return xxh3.HashString(key) }

// InternBatch resolves a borrowed batch through the same catalog index. Hashing
// the batch before probing separates name-byte reads from dependent index and
// metadata reads. Each name is hashed once; no name cache is maintained.
// Returned IDs are borrowed until the next InternBatch call. Durability still
// requires Sync, exactly as for Intern. All names use the supplied definition.
func (c *NameCatalog) InternBatch(keys []string, definition []byte) ([]uint64, error) {
	if c.failed != nil {
		return nil, c.failed
	}
	c.batchHashes = slices.Grow(c.batchHashes[:0], len(keys))[:len(keys)]
	c.batchIDs = slices.Grow(c.batchIDs[:0], len(keys))[:len(keys)]
	for i, key := range keys {
		c.batchHashes[i] = NameHash(key)
	}
	for i, key := range keys {
		id, err := c.InternHashed(key, definition, c.batchHashes[i])
		if err != nil {
			return nil, err
		}
		c.batchIDs[i] = id
	}
	return c.batchIDs, nil
}

type catalogSlot struct {
	hash, id uint64
}

// catalogIndex is the catalog's sole name index. Slots refer to the canonical
// immutable names through IDs; growth reuses stored hashes, never hashes names.
type catalogIndex struct {
	slots []catalogSlot
	count int
}

func (m *catalogIndex) lookup(key string, hash uint64, catalog *NameCatalog) uint64 {
	if len(m.slots) == 0 {
		return 0
	}
	mask := uint64(len(m.slots) - 1)
	for i := hash & mask; ; i = (i + 1) & mask {
		s := m.slots[i]
		if s.id == 0 {
			return 0
		}
		if s.hash == hash && catalog.key(s.id) == key {
			return s.id
		}
	}
}

func (m *catalogIndex) insert(hash, id uint64) {
	if (m.count+1)*4 > len(m.slots)*3 {
		old := m.slots
		m.slots = make([]catalogSlot, max(16, 2*len(old)))
		for _, s := range old {
			if s.id != 0 {
				m.place(s)
			}
		}
	}
	m.place(catalogSlot{hash: hash, id: id})
	m.count++
}

func (m *catalogIndex) place(s catalogSlot) {
	mask := uint64(len(m.slots) - 1)
	for i := s.hash & mask; ; i = (i + 1) & mask {
		if m.slots[i].id == 0 {
			m.slots[i] = s
			return
		}
	}
}
