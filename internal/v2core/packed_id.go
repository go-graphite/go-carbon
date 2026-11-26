package v2core

import (
	"errors"
	"sync"
	"sync/atomic"

	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// A packed engine ID names a metric by its store dictionary entry:
//
//	1<<63 | group<<48 | vshard<<32 | dictionary ID
//
// Bit 63 keeps the space disjoint from the catalog-derived NumericIDs that
// existing parts and L0 files carry (those stay far below 2^63). The vshard
// field is below VirtualShards, so no packed ID equals deadSlot.
const packedIDBit = 1 << 63

// maxPackedGroups bounds the group field (15 bits).
const maxPackedGroups = 1 << 15

// PackID returns the engine ID of dictionary entry (group, vshard, id).
func PackID(group, vshard uint16, id uint32) uint64 {
	return packedIDBit | uint64(group)<<48 | uint64(vshard)<<32 | uint64(id)
}

// IsPackedID reports whether id is a packed dictionary ID, not a NumericID.
func IsPackedID(id uint64) bool { return id&packedIDBit != 0 }

// UnpackID splits a packed engine ID.
func UnpackID(id uint64) (group, vshard uint16, dict uint32) {
	return uint16(id>>48) & (maxPackedGroups - 1), uint16(id >> 32), uint32(id)
}

// policyVPage holds 256 identities of one (group, vshard) dictionary range.
// Dictionary IDs are dense per vshard, so a store of N names fills
// N/256 pages plus at most one partial page per populated vshard.
type policyVPage [256]atomic.Pointer[PolicyIdentity]

// policyVDir is the dense page directory of one (group, vshard).
type policyVDir []atomic.Pointer[policyVPage]

// policyGroupPages holds every vshard directory of one group.
type policyGroupPages struct {
	group  partsformat.Group
	shards [partsformat.VirtualShards]atomic.Pointer[policyVDir]
}

// packedPages is the packed half of a PolicyCatalog: group labels get a
// process-local index on first use. Durable formats never trust it: parts
// derive identities from (group, vshard, key.Local), and L0 must carry its
// own group labels.
type packedPages struct {
	mu     sync.Mutex // creation only
	index  map[partsformat.Group]uint16
	groups atomic.Pointer[[]*policyGroupPages]
}

var errPackedGroups = errors.New("v2core: too many placement groups for packed IDs")

// GroupIndex returns the process-local packed index of group.
func (c *PolicyCatalog) GroupIndex(group partsformat.Group) (uint16, error) {
	p := &c.packed
	p.mu.Lock()
	defer p.mu.Unlock()
	if i, ok := p.index[group]; ok {
		return i, nil
	}
	var groups []*policyGroupPages
	if g := p.groups.Load(); g != nil {
		groups = *g
	}
	if len(groups) >= maxPackedGroups {
		return 0, errPackedGroups
	}
	if p.index == nil {
		p.index = make(map[partsformat.Group]uint16)
	}
	i := uint16(len(groups))
	next := append(groups[:len(groups):len(groups)], &policyGroupPages{group: group})
	p.groups.Store(&next)
	p.index[group] = i
	return i, nil
}

// packedSlot returns the identity slot of packed ID id, creating its page
// if create is set. Lookups take three atomic loads and no lock.
func (c *PolicyCatalog) packedSlot(id uint64, create bool) *atomic.Pointer[PolicyIdentity] {
	g, v, d := UnpackID(id)
	groups := c.packed.groups.Load()
	if groups == nil || int(g) >= len(*groups) || int(v) >= partsformat.VirtualShards {
		return nil
	}
	gp := (*groups)[g]
	page := d >> 8
	if dir := gp.shards[v].Load(); dir != nil && int(page) < len(*dir) {
		if p := (*dir)[page].Load(); p != nil {
			return &p[d&255]
		}
	}
	if !create {
		return nil
	}
	c.pagesMu.Lock()
	defer c.pagesMu.Unlock()
	var dir policyVDir
	if old := gp.shards[v].Load(); old != nil {
		dir = *old
	}
	if int(page) >= len(dir) {
		n := max(1, len(dir))
		for n <= int(page) {
			n *= 2
		}
		next := make(policyVDir, n)
		for i := range dir {
			next[i].Store(dir[i].Load())
		}
		gp.shards[v].Store(&next)
		dir = next
	}
	p := dir[page].Load()
	if p == nil {
		p = &policyVPage{}
		dir[page].Store(p)
	}
	return &p[d&255]
}

// packedGroup returns the group label of a packed ID's index.
func (c *PolicyCatalog) packedGroup(id uint64) (partsformat.Group, bool) {
	g, _, _ := UnpackID(id)
	groups := c.packed.groups.Load()
	if groups == nil || int(g) >= len(*groups) {
		return "", false
	}
	return (*groups)[g].group, true
}

// packedMatches checks that a packed ID names the identity's own group and
// vshard, so no identity is published under another dictionary range.
func (c *PolicyCatalog) packedMatches(h *PolicyIdentity) bool {
	group, ok := c.packedGroup(h.id)
	_, v, _ := UnpackID(h.id)
	return ok && group == h.policy.Group && v == h.vshard
}

// PackedName resolves a packed ID's name: from its registered identity, else
// from the store dictionary. L0 replay routes a batch to its producer by it.
func (e *coreEngine) PackedName(id uint64) (string, bool) {
	if h := e.identities.lookup(id); h != nil {
		return h.Name(), true
	}
	group, ok := e.identities.packedGroup(id)
	d := e.identities.partDict()
	if !ok || d == nil {
		return "", false
	}
	_, v, local := UnpackID(id)
	name, entry, ok := d.LookupID(string(group), v, local, nil)
	if !ok || entry.Deleted() {
		return "", false
	}
	return string(name), true
}

// PackedID returns the engine ID of dictionary entry (group, vshard, id).
func (e *coreEngine) PackedID(group partsformat.Group, vshard uint16, id uint32) (uint64, error) {
	g, err := e.identities.GroupIndex(group)
	if err != nil {
		return 0, err
	}
	return PackID(g, vshard, id), nil
}
