package v2daemon

import (
	"errors"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

// catalogPolicies binds journal-local IDs to engine identities. With a store
// dictionary every name is admitted under its packed dictionary ID; the
// local ID only indexes the producer's handle slice and is never persisted
// outside the catalog. Without one the catalog-derived NumericID remains.
type catalogPolicies struct {
	p        *Producer
	recovery *v2core.PolicyRecovery
}

func (c catalogPolicies) Bind(id uint64, name string, definition []byte) error {
	p := c.p
	h, err := p.engine.AdmitIdentity(name, definition)
	if err != nil {
		return err
	}
	if h == nil {
		numeric := NumericID(id, p.index, p.count)
		if c.recovery != nil {
			err = c.recovery.Bind(numeric, name, definition)
			h = p.engine.ApplicationPolicy(numeric)
		} else {
			h, err = p.engine.RegisterApplicationPolicy(numeric, name, definition)
		}
		if err != nil {
			return err
		}
	}
	if h == nil {
		return errors.New("application catalog has no bound policy")
	}
	for uint64(len(p.applications)) <= id {
		p.applications = append(p.applications, nil) // catalog IDs are dense
	}
	p.applications[id] = h
	return nil
}

func (c catalogPolicies) policy(id uint64) *v2core.ApplicationPolicy {
	if id < uint64(len(c.p.applications)) {
		return c.p.applications[id]
	}
	return nil
}

func (c catalogPolicies) Key(id uint64) string        { return c.policy(id).Name() }
func (c catalogPolicies) Definition(id uint64) []byte { return c.policy(id).Definition() }
func (c catalogPolicies) Matches(id uint64, name string, definition []byte) bool {
	h := c.policy(id)
	return h != nil && h.Name() == name && h.MatchesDefinition(definition)
}
