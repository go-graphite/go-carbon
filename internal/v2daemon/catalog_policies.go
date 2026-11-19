package v2daemon

import "github.com/go-graphite/go-carbon/internal/v2core"

// catalogPolicies translates journal-local IDs into the engine's existing
// catalog. It owns no per-name state and stays bound to one engine epoch.
type catalogPolicies struct {
	engine              *v2core.Engine
	producer, producers int
}

func (c catalogPolicies) Bind(id uint64, name string, definition []byte) error {
	_, err := c.engine.RegisterApplicationPolicy(NumericID(id, c.producer, c.producers), name, definition)
	return err
}

func (c catalogPolicies) policy(id uint64) *v2core.ApplicationPolicy {
	return c.engine.ApplicationPolicy(NumericID(id, c.producer, c.producers))
}

func (c catalogPolicies) Key(id uint64) string        { return c.policy(id).Name() }
func (c catalogPolicies) Definition(id uint64) []byte { return c.policy(id).Definition() }
func (c catalogPolicies) Matches(id uint64, name string, definition []byte) bool {
	h := c.policy(id)
	return h.Name() == name && h.MatchesDefinition(definition)
}
