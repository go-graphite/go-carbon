package v2core

// ApplicationPolicy is the same immutable identity used by journal, owners and
// readers. Registration adds no separate policy allocation or all-ID cache.
type ApplicationPolicy = PolicyIdentity

// NewApplicationPolicy validates a detached policy for callers without an engine.
func NewApplicationPolicy(name string, encoded []byte) (*ApplicationPolicy, error) {
	return (&PolicyCatalog{}).intern(1, name, encoded)
}

// RollupPolicy returns the canonical policy the catalog validated at intern.
func (h *ApplicationPolicy) RollupPolicy() RollupPolicy { return h.rollupPolicy() }

// RegisterApplicationPolicy borrows the journal's already interned identity.
func (e *coreEngine) RegisterApplicationPolicy(id uint64, name string, encoded []byte) (*ApplicationPolicy, error) {
	return e.identities.intern(id, name, encoded)
}

// MatchesApplicationPolicy checks the engine epoch and authoritative numeric ID.
// Producer-local IDs are mapped by the caller without a per-point lookup.
func (e *coreEngine) MatchesApplicationPolicy(h *ApplicationPolicy, id uint64) bool {
	return h != nil && h.catalog == e.identities && h.id == id
}
