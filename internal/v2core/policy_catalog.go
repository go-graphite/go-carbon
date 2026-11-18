package v2core

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// PolicyIdentity owns an immutable name and borrows one shared schema/key prefix.
// Journal and part boundaries reconstruct the original canonical definition.
type PolicyIdentity struct {
	catalog *PolicyCatalog
	id      uint64
	policy  policyName
	vshard  uint16
	plain   bool
	// nameHash is NameHash(name), computed once at intern time while the name
	// bytes are hot, so readers index names without touching them.
	nameHash uint64
}

// Definitions and key prefixes belong to the catalog epoch, rather than a name.
// Only the name and numeric identity vary between handles.
type policyDefinition struct {
	suffix    []byte
	prepared  *sealrollup.Prepared
	keyPrefix []byte
	Group     partsformat.Group
}
type policyIDPage [1024]atomic.Pointer[PolicyIdentity]

// PolicyCatalog is scoped to one journal epoch. Published pages and handles are
// immutable; reads take no construction mutex, including during page growth.
type PolicyCatalog struct {
	ids         sync.Map // page number -> *policyIDPage, not one map node per name
	definitions sync.Map // canonical suffix -> *policyDefinition
	shards      [partsformat.VirtualShards]atomic.Pointer[policyCatalogShard]
}
type policyCatalogShard struct {
	mu        sync.Mutex
	schemas   map[[32]byte]*PolicyIdentity
	encodings map[string]*PolicyIdentity
}

// Definition returns a caller-owned copy of the canonical journal definition.
// Registered batches retain the shared identity until a reader needs its bytes.
func (h *PolicyIdentity) Definition() []byte { return h.definitionBytes() }

// NameHash returns the cached process-local 64-bit hash of Name.
func (h *PolicyIdentity) NameHash() uint64 { return h.nameHash }

var nameHashSeed = maphash.MakeSeed()

// NameHash hashes a metric name with the seed PolicyIdentity.NameHash uses.
func NameHash(name string) uint64 { return maphash.String(nameHashSeed, name) }

// Name returns the immutable registered metric name without encoding its policy.
func (h *PolicyIdentity) Name() string { return string(h.policy.Name) }

// definitionBytes reconstructs the unchanged canonical journal/part encoding.
// Ordinary routing and registered application lookup need only the handle.
func (h *PolicyIdentity) definitionBytes() []byte {
	return h.appendDefinition(make([]byte, 0, len(h.policy.Name)+len(h.policy.suffix)+16))
}

// appendDefinition writes the canonical definition directly into an owner arena.
func (h *PolicyIdentity) appendDefinition(out []byte) []byte {
	return appendPolicyDefinition(out, h.policy.Name, h.plain, h.policy.policyDefinition)
}

// appendPolicyDefinition is appendDefinition from copied identity fields.
func appendPolicyDefinition(out []byte, name partsformat.ID, plain bool, def *policyDefinition) []byte {
	out = append(out, `{"Name":"`...)
	if plain {
		out = append(out, name...)
	} else {
		encoded, _ := json.Marshal(string(name))
		out = append(out, encoded[1:len(encoded)-1]...)
	}
	return append(out, def.suffix...)
}
func (h *PolicyIdentity) definitionEqual(raw []byte) bool {
	if !h.plain {
		return bytes.Equal(h.definitionBytes(), raw)
	}
	const prefix = `{"Name":"`
	n := len(prefix) + len(h.policy.Name)
	return len(raw) == n+len(h.policy.suffix) && bytes.HasPrefix(raw, []byte(prefix)) && string(raw[len(prefix):n]) == string(h.policy.Name) && bytes.Equal(raw[n:], h.policy.suffix)
}
func detachedPolicy(id uint64, p RollupPolicy, raw []byte, prepared *sealrollup.Prepared) *PolicyIdentity {
	name, _ := json.Marshal(string(p.Name))
	suffix := bytes.Clone(raw[len(`{"Name":`)+len(name)-1:])
	prefix, _, _ := partsformat.GroupRange(p.Group)
	return &PolicyIdentity{id: id, policy: policyName{Name: p.Name, policyDefinition: &policyDefinition{suffix: suffix, prepared: prepared, keyPrefix: prefix, Group: p.Group}}, plain: canonicalNameEnd(raw) > 0, vshard: partsformat.Route(p.Name), nameHash: NameHash(string(p.Name))}
}

func policyIdentityMatches(old *PolicyIdentity, name string, definition []byte) (*PolicyIdentity, error) {
	if (name != "" && string(old.policy.Name) != name) || !old.definitionEqual(definition) {
		return nil, errors.New("stored rollup policy changed")
	}
	return old, nil
}

func (c *PolicyCatalog) publish(h *PolicyIdentity) (*PolicyIdentity, error) {
	pageID := h.id >> 10
	page, _ := c.ids.Load(pageID)
	if page == nil {
		page, _ = c.ids.LoadOrStore(pageID, &policyIDPage{})
	}
	slot := &page.(*policyIDPage)[h.id&1023]
	if slot.CompareAndSwap(nil, h) {
		return h, nil
	}
	return policyIdentityMatches(slot.Load(), string(h.policy.Name), h.definitionBytes())
}

func (c *PolicyCatalog) intern(id uint64, name string, definition []byte) (*PolicyIdentity, error) {
	if old := c.lookup(id); old != nil {
		return policyIdentityMatches(old, name, definition)
	}
	// Routing is only a construction-lock choice, never an identity. Invalid or
	// escaped encodings still go through the complete canonical JSON oracle.
	nameEnd := canonicalNameEnd(definition)
	routeName := name
	if routeName == "" {
		if nameEnd > 0 {
			routeName = string(definition[len(`{"Name":"`):nameEnd])
		} else {
			var p RollupPolicy
			if err := json.Unmarshal(definition, &p); err != nil {
				return nil, err
			}
			routeName = string(p.Name)
		}
	}
	slot := &c.shards[partsformat.Route(partsformat.ID(routeName))]
	shard := slot.Load()
	if shard == nil {
		fresh := &policyCatalogShard{schemas: make(map[[32]byte]*PolicyIdentity), encodings: make(map[string]*PolicyIdentity)}
		if slot.CompareAndSwap(nil, fresh) {
			shard = fresh
		} else {
			shard = slot.Load()
		}
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if old := c.lookup(id); old != nil {
		return policyIdentityMatches(old, name, definition)
	}
	// Repeated schemas differ only in their canonical, unescaped metric name.
	// Reuse a previously validated suffix; uncommon/escaped names retain the
	// complete JSON oracle below. This cache has the same fixed bound as schemas.
	if id != 0 && nameEnd > 0 {
		if template := shard.encodings[string(definition[nameEnd:])]; template != nil {
			const start = len(`{"Name":"`)
			if name != "" && name != string(definition[start:nameEnd]) {
				return nil, errors.New("noncanonical rollup policy")
			}
			ownedName := string(definition[start:nameEnd])
			h := &PolicyIdentity{catalog: c, vshard: partsformat.Route(partsformat.ID(ownedName)), id: id, policy: policyName{Name: partsformat.ID(ownedName), policyDefinition: template.policy.policyDefinition}, plain: true, nameHash: NameHash(ownedName)}
			return c.publish(h)
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
	if old := shard.schemas[digest]; old != nil {
		p.Config = old.schema()
		prepared = old.policy.prepared
	} else {
		prepared, err = sealrollup.Prepare(p.Config)
		if err != nil {
			return nil, err
		}
	}
	h := detachedPolicy(id, p, canonical, prepared)
	key := string(h.policy.suffix)
	shared, _ := c.definitions.LoadOrStore(key, h.policy.policyDefinition)
	h.policy.policyDefinition = shared.(*policyDefinition)
	h.catalog = c

	if nameEnd > 0 && len(shard.encodings) < 64 {
		shard.encodings[string(canonical[nameEnd:])] = h
	}
	// Schema reuse is bounded; it does not keep a second all-name directory.
	if len(shard.schemas) < 64 {
		shard.schemas[digest] = h
	}
	return c.publish(h)
}
func (c *PolicyCatalog) lookup(id uint64) *PolicyIdentity {
	page, _ := c.ids.Load(id >> 10)
	if page == nil {
		return nil
	}
	return page.(*policyIDPage)[id&1023].Load()
}

// BindDefinition shares the canonical name and validates the journal definition.
// The journal stores its own shared suffix, rather than a full encoding per name.
func (e *coreEngine) BindDefinition(id uint64, name string, definition []byte) (string, []byte, error) {
	h, err := e.identities.intern(id, name, definition)
	if err != nil {
		return "", nil, err
	}
	return string(h.policy.Name), h.definitionBytes(), nil
}

// Only mutable lifecycle/frontier scalars belong in each owner/part directory.
// Identity, raw policy and schema digest are reconstructed from the shared handle.
type metricMetadata struct {
	ClosedThrough int64
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
	out.ClosedThrough = m.ClosedThrough
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
	out := h.metadataScalars(m)
	out.Policy = h.definitionBytes()
	return out
}

// Planning carries immutable identity separately until the part is encoded.
func (h *PolicyIdentity) metadataScalars(m metricMetadata) RollupMetadata {
	return RollupMetadata{ClosedThrough: m.ClosedThrough, BaselineOrder: m.BaselineOrder, ResetOrder: m.ResetOrder, Expired: m.Expired, Version: m.Version, ID: h.id, Frontier: sealrollup.Frontier{Order: m.Frontier.Order, MaxTime: m.Frontier.MaxTime, Policy: h.policy.prepared.Digest()}, LastUpdate: m.LastUpdate, Revision: m.Revision}
}
func (m *rollupMetric) metadata() RollupMetadata    { return m.PolicyIdentity.metadata(m.meta) }
func (m *readMetricState) metadata() RollupMetadata { return m.PolicyIdentity.metadata(m.meta) }

type policyName struct {
	Name partsformat.ID
	*policyDefinition
}

func (h *PolicyIdentity) schema() sealrollup.Config { return h.policy.prepared.Config() }
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

// Idle closure changes no input activity revision or source order. Its committed
// watermark breaks otherwise equal metadata ties, including after merge/reopen.
func (m metricMetadata) after(other metricMetadata) bool {
	return m.Frontier.Order > other.Frontier.Order || m.Frontier.Order == other.Frontier.Order && (m.Revision > other.Revision || m.Revision == other.Revision && m.ClosedThrough > other.ClosedThrough)
}
