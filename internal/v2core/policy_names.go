package v2core

import "github.com/go-graphite/go-carbon/internal/v2part"

// PolicyNameSource is one immutable mapped discovery run. NameAt returns owned
// bytes; holding the source pins its mapping through readers and seal output.
// Positions are local to this source, never to the catalog's current run.
type PolicyNameSource interface {
	FindName(string) (uint32, bool)
	NameAt(uint32) string
}

// A page owns positions in exactly one immutable base. Zero uses nameTail.
// Rebinding copies absent names to the tail, so a retired base is never pinned
// by an expired identity for the remainder of the journal epoch.
type policyNamePage struct {
	source PolicyNameSource
	refs   [1024]uint32
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
				return v2part.ID(names.source.NameAt(ref - 1))
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
// A nil source detaches the base, preserving all names in the owned tail.
// Call only on a new base publication, never on per-point or small-tail updates.
func (c *PolicyCatalog) BindMappedNames(source PolicyNameSource) error {
	c.namesMu.Lock()
	defer c.namesMu.Unlock()
	c.ids.Range(func(_, value any) bool {
		page := value.(*policyIDPage)
		old := page.names.Load()
		next := &policyNamePage{source: source}
		var published []uint64
		for slot := range page.handles {
			h := page.handles[slot].Load()
			if h == nil {
				continue
			}
			var pos uint32
			var ok bool
			if source != nil {
				pos, ok = source.FindName(h.Name())
			}
			if ok {
				next.refs[slot] = pos + 1
				published = append(published, h.id)
			} else if old != nil && old.refs[slot] != 0 {
				// Install the owned fallback before publishing a page without this name.
				// Readers holding old keep that source alive through NameAt's owned copy.
				c.nameTail.Store(h.id, old.source.NameAt(old.refs[slot]-1))
			}
		}
		if len(published) == 0 {
			next = nil
		}
		page.names.Store(next)
		for _, id := range published {
			c.nameTail.Delete(id)
		}
		return true
	})
	return nil
}
