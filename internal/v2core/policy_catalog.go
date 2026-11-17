package v2core

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// PolicyIdentity owns one immutable canonical name/schema/definition. Its bytes
// may be borrowed by catalogs, dispatches and pinned read generations. Callers
// must never modify the returned definition or schema slices.
type PolicyIdentity struct {
	id         uint64
	policy     policyName
	definition []byte
	prepared   *sealrollup.Prepared
}

// PolicyCatalog is scoped to one journal epoch/engine, never process global.
// Numeric IDs are allocated by the journal; this directory adds no name index.
// It replaces the router's separate all-ID policy copies.
type PolicyCatalog struct {
	mu        sync.RWMutex
	ids       map[uint64]*PolicyIdentity
	schemas   map[[32]byte]*PolicyIdentity
	encodings map[string]*PolicyIdentity
}

func (c *PolicyCatalog) intern(id uint64, name string, definition []byte) (*PolicyIdentity, error) {
	c.mu.RLock()
	old := c.ids[id]
	c.mu.RUnlock()
	if old != nil {
		if (name != "" && string(old.policy.Name) != name) || !bytes.Equal(old.definition, definition) {
			return nil, errors.New("stored rollup policy changed")
		}
		return old, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.ids[id]; old != nil {
		if (name != "" && string(old.policy.Name) != name) || !bytes.Equal(old.definition, definition) {
			return nil, errors.New("stored rollup policy changed")
		}
		return old, nil
	}
	// Repeated schemas differ only in their canonical, unescaped metric name.
	// Reuse a previously validated suffix; uncommon/escaped names retain the
	// complete JSON oracle below. This cache has the same fixed bound as schemas.
	nameEnd := canonicalNameEnd(definition)
	if id != 0 && nameEnd > 0 {
		if template := c.encodings[string(definition[nameEnd:])]; template != nil {
			const start = len(`{"Name":"`)
			if name != "" && name != string(definition[start:nameEnd]) {
				return nil, errors.New("noncanonical rollup policy")
			}
			owned := bytes.Clone(definition)
			h := &PolicyIdentity{id: id, policy: policyName{Name: partsformat.ID(unsafe.String(&owned[start], nameEnd-start)), Group: template.policy.Group}, definition: owned[:len(owned):len(owned)], prepared: template.prepared}
			c.ids[id] = h
			return h, nil
		}
	}
	var p RollupPolicy
	if err := json.Unmarshal(definition, &p); err != nil {
		return nil, err
	}
	canonical, err := EncodeRollupPolicy(p)
	if err != nil {
		return nil, err
	}
	if id == 0 || (name != "" && name != string(p.Name)) || !bytes.Equal(canonical, definition) {
		return nil, errors.New("noncanonical rollup policy")
	}
	digest, err := sealrollup.PolicyDigest(p.Config)
	if err != nil {
		return nil, err
	}
	var prepared *sealrollup.Prepared
	if old := c.schemas[digest]; old != nil {
		p.Config = old.schema()
		prepared = old.prepared
	} else {
		prepared, err = sealrollup.Prepare(p.Config)
		if err != nil {
			return nil, err
		}
	}
	// Canonical JSON starts with Name. For an unescaped name, borrow the
	// immutable definition bytes rather than retaining another name allocation.
	const nameStart = len(`{"Name":"`)
	if len(canonical) > nameStart+len(p.Name) && bytes.Equal(canonical[:nameStart], []byte(`{"Name":"`)) && bytes.Equal(canonical[nameStart:nameStart+len(p.Name)], []byte(p.Name)) && canonical[nameStart+len(p.Name)] == '"' {
		p.Name = partsformat.ID(unsafe.String(&canonical[nameStart], len(p.Name)))
	}
	h := &PolicyIdentity{id: id, policy: policyName{Name: p.Name, Group: p.Group}, definition: canonical[:len(canonical):len(canonical)], prepared: prepared}
	if c.ids == nil {
		c.ids = make(map[uint64]*PolicyIdentity)
		c.schemas = make(map[[32]byte]*PolicyIdentity)
		c.encodings = make(map[string]*PolicyIdentity)
	}
	c.ids[id] = h
	if nameEnd > 0 && len(c.encodings) < 64 {
		c.encodings[string(canonical[nameEnd:])] = h
	}
	// Schema reuse is bounded; it does not keep a second all-name directory.
	if len(c.schemas) < 64 {
		c.schemas[digest] = h
	}
	return h, nil
}
func (c *PolicyCatalog) lookup(id uint64) *PolicyIdentity {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ids[id]
}

// BindDefinition lets the journal borrow the engine's canonical identity.
func (e *coreEngine) BindDefinition(id uint64, name string, definition []byte) (string, []byte, error) {
	h, err := e.identities.intern(id, name, definition)
	if err != nil {
		return "", nil, err
	}
	return string(h.policy.Name), h.definition, nil
}

// Only mutable lifecycle/frontier scalars belong in each owner/part directory.
// Identity, raw policy and schema digest are reconstructed from the shared handle.
type metricMetadata struct {
	BaselineOrder uint64
	ResetOrder    uint64
	LastUpdate    int64
	Revision      uint64
	Frontier      struct {
		Order   uint64
		MaxTime int64
	}
	Version uint8
	Expired bool
}

func compactMetadata(m RollupMetadata) metricMetadata {
	var out metricMetadata
	out.BaselineOrder = m.BaselineOrder
	out.ResetOrder = m.ResetOrder
	out.LastUpdate = m.LastUpdate
	out.Revision = m.Revision
	out.Frontier.Order = m.Frontier.Order
	out.Frontier.MaxTime = m.Frontier.MaxTime
	out.Version = m.Version
	out.Expired = m.Expired
	return out
}
func (h *PolicyIdentity) metadata(m metricMetadata) RollupMetadata {
	return RollupMetadata{BaselineOrder: m.BaselineOrder, ResetOrder: m.ResetOrder, Expired: m.Expired, Version: m.Version, ID: h.id, Policy: h.definition, Frontier: sealrollup.Frontier{Order: m.Frontier.Order, MaxTime: m.Frontier.MaxTime, Policy: h.prepared.Digest()}, LastUpdate: m.LastUpdate, Revision: m.Revision}
}
func (m *rollupMetric) metadata() RollupMetadata   { return m.PolicyIdentity.metadata(m.meta) }
func (m *readPartMetric) metadata() RollupMetadata { return m.PolicyIdentity.metadata(m.meta) }

type policyName struct {
	Name  partsformat.ID
	Group partsformat.Group
}

func (h *PolicyIdentity) schema() sealrollup.Config { return h.prepared.Config() }
func (h *PolicyIdentity) rollupPolicy() RollupPolicy {
	return RollupPolicy{Name: h.policy.Name, Group: h.policy.Group, Config: h.schema()}
}

// Only plain ASCII names take the suffix fast path. JSON escaping, UTF-8
// replacement, HTML escaping and unusual input all go through encoding/json.
func canonicalNameEnd(definition []byte) int {
	const prefix = `{"Name":"`
	if !bytes.HasPrefix(definition, []byte(prefix)) {
		return 0
	}
	for i := len(prefix); i < len(definition); i++ {
		c := definition[i]
		if c == '"' {
			if i == len(prefix) || i-len(prefix) > partsformat.MaxNameBytes {
				return 0
			}
			return i
		}
		if c < 0x20 || c >= 0x7f || c == '\\' || c == '<' || c == '>' || c == '&' {
			return 0
		}
	}
	return 0
}
