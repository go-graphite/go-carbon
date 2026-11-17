package v2core

// ApplicationPolicy is the same immutable identity used by journal, owners and
// readers. Registration adds no separate policy allocation or all-ID cache.
type ApplicationPolicy = PolicyIdentity

// NewApplicationPolicy validates a detached policy for callers without an engine.
func NewApplicationPolicy(name string, encoded []byte) (*ApplicationPolicy, error) {
	return (&PolicyCatalog{}).intern(1, name, encoded)
}

// RegisterApplicationPolicy borrows the journal's already interned identity.
func (e *coreEngine) RegisterApplicationPolicy(id uint64, name string, encoded []byte) (*ApplicationPolicy, error) {
	return e.identities.intern(id, name, encoded)
}
