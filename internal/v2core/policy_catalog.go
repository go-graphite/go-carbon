package v2core

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// PolicyIdentity resolves its immutable name through its catalog epoch and
// borrows one shared schema/key prefix. Detached handles retain a local name.
// Journal and part boundaries reconstruct the original canonical definition.
type PolicyIdentity struct {
	catalog  *PolicyCatalog
	id       uint64
	policy   policyName
	heapName atomic.Pointer[string]
	vshard   uint16
	plain    bool
	// ownerSlot is a seal planner's hint: 1 + the owner-local slot install
	// gave this name, 0 when none. It sits in padding (0 B per name). Planners
	// sharing the identity (adoption) overwrite it, so it is only a hint:
	// readers confirm ids[slot] == id and fall back to the metrics map.
	ownerSlot atomic.Uint32
	// route is namefp.Route(name)>>32, the second fingerprint half. It takes
	// the struct from 56 B to 64 B, and both round up to Go's 64 B size class,
	// so it costs 0 B of heap per name.
	route uint32
	// nameHash is namefp.Hash(name), computed once at intern time while the name
	// bytes are hot, so readers index names without touching them.
	nameHash uint64
}

// Definitions belong to the catalog epoch, rather than a name.
// Only the name and numeric identity vary between handles.
type policyDefinition struct {
	suffix   []byte
	prepared *sealrollup.Prepared
	Group    partsformat.Group
}
type policyIDPage struct {
	handles [1024]atomic.Pointer[PolicyIdentity]
	names   atomic.Pointer[policyNamePage]
}

// PolicyCatalog is scoped to one journal epoch. Handles and name bindings are
// immutable; page pointers publish new bindings atomically. Reads take no
// construction mutex, including during page growth.
type PolicyCatalog struct {
	namesMu     sync.Mutex // serializes base rebinding
	ids         sync.Map   // page number -> *policyIDPage, not one map node per name
	definitions sync.Map   // canonical suffix -> *policyDefinition
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

// fingerprint identifies the name without reading its bytes.
func (h *PolicyIdentity) fingerprint() namefp.Fingerprint {
	return namefp.Fingerprint{Hash: h.nameHash, Route: h.route}
}

// Name returns the immutable registered metric name without encoding its policy.
func (h *PolicyIdentity) Name() string { return string(h.metricName()) }

// definitionBytes reconstructs the unchanged canonical journal/part encoding.
// Ordinary routing and registered application lookup need only the handle.
func (h *PolicyIdentity) definitionBytes() []byte {
	return h.appendDefinition(make([]byte, 0, len(h.metricName())+len(h.policy.suffix)+16))
}

// appendDefinition writes the canonical definition directly into an owner arena.
func (h *PolicyIdentity) appendDefinition(out []byte) []byte {
	nameBytes := h.metricName()
	out = append(out, `{"Name":"`...)
	if h.plain {
		out = append(out, nameBytes...)
	} else {
		name, _ := json.Marshal(string(nameBytes))
		out = append(out, name[1:len(name)-1]...)
	}
	return append(out, h.policy.suffix...)
}
func (h *PolicyIdentity) definitionEqual(raw []byte) bool {
	if !h.plain {
		return bytes.Equal(h.definitionBytes(), raw)
	}
	const prefix = `{"Name":"`
	name := h.metricName()
	n := len(prefix) + len(name)
	return len(raw) == n+len(h.policy.suffix) && bytes.HasPrefix(raw, []byte(prefix)) && string(raw[len(prefix):n]) == string(name) && bytes.Equal(raw[n:], h.policy.suffix)
}
func detachedPolicy(id uint64, p RollupPolicy, raw []byte, prepared *sealrollup.Prepared) *PolicyIdentity {
	name, _ := json.Marshal(string(p.Name))
	suffix := bytes.Clone(raw[len(`{"Name":`)+len(name)-1:])
	h := &PolicyIdentity{id: id, policy: policyName{policyDefinition: &policyDefinition{suffix: suffix, prepared: prepared, Group: p.Group}}, plain: canonicalNameEnd(raw) > 0, vshard: partsformat.Route(p.Name), nameHash: namefp.Hash(string(p.Name)), route: uint32(namefp.Route(p.Name) >> 32)}
	ownedName := string(p.Name)
	h.heapName.Store(&ownedName)
	return h
}

func policyIdentityMatches(old *PolicyIdentity, name string, definition []byte) (*PolicyIdentity, error) {
	if (name != "" && string(old.metricName()) != name) || !old.definitionEqual(definition) {
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
	slot := &page.(*policyIDPage).handles[h.id&1023]
	name := h.Name()
	if slot.CompareAndSwap(nil, h) {
		return h, nil
	}
	return policyIdentityMatches(slot.Load(), name, h.definitionBytes())
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
			h := &PolicyIdentity{catalog: c, vshard: partsformat.Route(partsformat.ID(ownedName)), id: id, policy: policyName{policyDefinition: template.policy.policyDefinition}, plain: true, nameHash: namefp.Hash(ownedName), route: uint32(namefp.Route(ownedName) >> 32)}
			h.heapName.Store(&ownedName)
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
	return page.(*policyIDPage).handles[id&1023].Load()
}

// ApplicationPolicy resolves an ID in this engine's immutable catalog epoch.
// The returned identity keeps the catalog alive for its consumers.
func (e *coreEngine) ApplicationPolicy(id uint64) *ApplicationPolicy {
	return e.identities.lookup(id)
}

// MatchesDefinition compares the canonical journal bytes without rebuilding
// ordinary unescaped definitions.
func (h *PolicyIdentity) MatchesDefinition(definition []byte) bool {
	return h.definitionEqual(definition)
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
func (m *readMetricState) metadata() RollupMetadata { return m.PolicyIdentity.metadata(m.meta) }

// metadataOf returns the committed metadata of m, installed under id.
func (r *coreRollups) metadataOf(id uint64, m *rollupMetric) RollupMetadata {
	return m.metadata(r.meta[r.slotOf(id, m)])
}

type policyName struct {
	*policyDefinition
}

func (h *PolicyIdentity) schema() sealrollup.Config { return h.policy.prepared.Config() }
func (h *PolicyIdentity) rollupPolicy() RollupPolicy {
	return RollupPolicy{Name: h.metricName(), Group: h.policy.Group, Config: h.schema()}
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
