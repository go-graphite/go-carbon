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

// packedPages is the packed half of a PolicyCatalog, indexed by the store
// dictionary's durable group index (v2dict InternGroup, from 1), so a packed
// ID means the same name in every process of the store.
type packedPages struct {
	mu     sync.Mutex // growth only
	groups atomic.Pointer[[]*policyGroupPages]
}

var (
	errPackedGroups = errors.New("v2core: too many placement groups for packed IDs")
	errPackedDict   = errors.New("v2core: packed IDs need the store dictionary")
)

// GroupIndex returns the store's durable packed index of group (from 1),
// recording a new group in the dictionary once.
func (c *PolicyCatalog) GroupIndex(group partsformat.Group) (uint16, error) {
	d := c.partDict()
	if d == nil {
		return 0, errPackedDict
	}
	i, err := d.InternGroup(string(group))
	if err != nil {
		return 0, err
	}
	return i, c.placeGroup(i, group)
}

// placeGroup installs group at packed index i, as the store dictionary or a
// copied part's donor numbered it; i must not hold another group.
func (c *PolicyCatalog) placeGroup(i uint16, group partsformat.Group) error {
	if i >= maxPackedGroups {
		return errPackedGroups
	}
	p := &c.packed
	if groups := p.groups.Load(); groups == nil || int(i) >= len(*groups) || (*groups)[i] == nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		var groups []*policyGroupPages
		if g := p.groups.Load(); g != nil {
			groups = *g
		}
		if int(i) >= len(groups) || groups[i] == nil {
			next := make([]*policyGroupPages, max(len(groups), int(i)+1))
			copy(next, groups)
			next[i] = &policyGroupPages{group: group}
			p.groups.Store(&next)
			return nil
		}
	}
	if (*p.groups.Load())[i].group != group {
		return errors.New("v2core: packed group index names another group")
	}
	return nil
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
	if gp == nil {
		return nil
	}
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
	if groups == nil || int(g) >= len(*groups) || (*groups)[g] == nil {
		return c.dictGroup(g)
	}
	return (*groups)[g].group, true
}

// packedMatches checks that a packed ID names the identity's own group and
// vshard, so no identity is published under another dictionary range.
func (c *PolicyCatalog) packedMatches(h *PolicyIdentity) bool {
	group, ok := c.packedGroup(h.id)
	g, v, _ := UnpackID(h.id)
	// A group known only to the dictionary gets its pages here.
	return ok && group == h.policy.Group && v == h.vshard && c.placeGroup(g, group) == nil
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

// dictGroup resolves a group index this process has not used yet, such as
// one an L0 file or a part of an earlier process carries.
func (c *PolicyCatalog) dictGroup(g uint16) (partsformat.Group, bool) {
	d := c.partDict()
	if d == nil {
		return "", false
	}
	group, ok := d.Group(g)
	return partsformat.Group(group), ok
}

// dictIdentity registers packed ID id from the store dictionary alone: its
// name and durable policy suffix. L0 replay uses it for names no catalog
// bound in this process.
func (c *PolicyCatalog) dictIdentity(id uint64) (*PolicyIdentity, error) {
	group, ok := c.packedGroup(id)
	if !ok {
		return nil, errors.New("unknown packed group")
	}
	if _, err := c.GroupIndex(group); err != nil { // creates the group's pages
		return nil, err
	}
	_, vshard, dict := UnpackID(id)
	def, ok := DictionaryDefinition(c.partDict(), string(group), vshard, dict)
	if !ok {
		return nil, errors.New("no live dictionary entry")
	}
	h, err := c.intern(id, "", def)
	if err != nil {
		return nil, err
	}
	h.rememberDictID(dict)
	return h, nil
}
