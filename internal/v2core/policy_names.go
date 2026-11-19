package v2core

import (
	"errors"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// PolicyNameSource is one immutable mapped discovery run. NameAt returns owned
// bytes; holding the source pins its mapping through readers and seal output.
// Positions are local to this source, never to the catalog's current run.
type PolicyNameSource interface {
	FindName(string) (uint32, bool)
	NameAt(uint32) string
}

// A page binds positions to their exact mapping owners. The low word is 1+the
// position; the high word is 1+the source index. Unpublished names use nameTail.
// Keeping older sources only for names missing from the new base also preserves
// identities of expired names, which can still be referenced by the journal.
type policyNamePage struct {
	sources []PolicyNameSource
	refs    [1024]uint64
}

func (h *PolicyIdentity) metricName() v2part.ID {
	if h.policy.Name != "" || h.catalog == nil {
		return h.policy.Name
	}
	page, _ := h.catalog.ids.Load(h.id >> 10)
	p := page.(*policyIDPage)
	for {
		if names := p.names.Load(); names != nil {
			if ref := names.refs[h.id&1023]; ref != 0 {
				return v2part.ID(names.sources[(ref>>32)-1].NameAt(uint32(ref) - 1))
			}
		}
		if tail, ok := h.catalog.nameTail.Load(h.id); ok {
			return v2part.ID(tail.(string))
		}
		// A publication may have installed the first mapping and removed the tail
		// after this reader sampled the old page. Retry against its new owner.
	}
}

// BindMappedNames moves matching identities out of the heap tail. The source
// must be immutable and must own its mapping; a catalog-wide mutable resolver
// would silently reinterpret positions after a discovery base replacement.
// Call only on a new base publication, never on per-point or small-tail updates.
func (c *PolicyCatalog) BindMappedNames(source PolicyNameSource) error {
	if source == nil {
		return errors.New("nil mapped policy name source")
	}
	c.namesMu.Lock()
	defer c.namesMu.Unlock()
	c.ids.Range(func(_, value any) bool {
		page := value.(*policyIDPage)
		old := page.names.Load()
		next := &policyNamePage{sources: []PolicyNameSource{source}}
		carried := make(map[uint32]uint32)
		var published []uint64
		for slot := range page.handles {
			h := page.handles[slot].Load()
			if h == nil {
				continue
			}
			if pos, ok := source.FindName(h.Name()); ok {
				next.refs[slot] = uint64(1)<<32 | uint64(pos) + 1
				published = append(published, h.id)
			} else if old != nil && old.refs[slot] != 0 {
				ref := old.refs[slot]
				src := uint32(ref >> 32)
				target, ok := carried[src]
				if !ok {
					next.sources = append(next.sources, old.sources[src-1])
					target = uint32(len(next.sources))
					carried[src] = target
				}
				next.refs[slot] = uint64(target)<<32 | uint64(uint32(ref))
			}
		}
		if len(published) == 0 {
			return true
		}
		page.names.Store(next)
		for _, id := range published {
			c.nameTail.Delete(id)
		}
		return true
	})
	return nil
}
