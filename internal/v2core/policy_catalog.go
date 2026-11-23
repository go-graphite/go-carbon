package v2core

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/namefp"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// PolicyIdentity references a dictionary name and one shared schema/key prefix.
// Before its first dictionary append, it owns the immutable admission name.
// Journal and part boundaries reconstruct the original canonical definition.
type PolicyIdentity struct {
	catalog *PolicyCatalog
	id      uint64
	policy  policyName
	name    atomic.Pointer[byte]
	nameLen uint32
	vshard  uint16
	plain   bool
	// ownerSlot is a seal planner's hint: 1 + the owner-local slot install
	// gave this name, 0 when none. It sits in padding (0 B per name). Planners
	// sharing the identity (adoption) overwrite it, so it is only a hint:
	// readers confirm ids[slot] == id and fall back to the metrics map.
	ownerSlot atomic.Uint32
	// route is namefp.Route(name)>>32, the second fingerprint half. It takes
	// the struct from 56 B to 64 B, and both round up to Go's 64 B size class,
	// so it costs 0 B of heap per name.
	route uint32
	// dictID is 1 + the name's ID in the store dictionary (ID-keyed parts)
	// once a seal or a read resolved it, 0 before. It fills the padding after
	// route (0 B per name) and outlives the planner's per-owner state, so a
	// name is appended to the dictionary once per process; cold reads
	// resolve its bytes there. Merges treat it as the name's live ID,
	// so a dictionary Delete of the name must clear it.
	dictID atomic.Uint32
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

type policyIDPage [1024]atomic.Pointer[PolicyIdentity]

// The normal numeric-ID space uses a small, geometrically grown directory.
// Only creation takes the mutex; published directory entries are atomic and
// never removed. Overflow IDs keep the sparse map rather than reserving holes.
type policyIDPages struct {
	pages []atomic.Pointer[policyIDPage]
}

const denseIDPages = 1 << 20 // one billion IDs; at most 8 MiB of directory

// PolicyCatalog is scoped to one journal epoch. Published pages and handles are
// immutable; reads take no construction mutex, including during page growth.
type PolicyCatalog struct {
	pages       atomic.Pointer[policyIDPages]
	pagesMu     sync.Mutex
	ids         sync.Map // sparse overflow pages for IDs at or above 2^30
	definitions sync.Map // canonical suffix -> *policyDefinition
	shards      [partsformat.VirtualShards]atomic.Pointer[policyCatalogShard]
	// dict is the store name dictionary that ID-keyed parts resolve through
	// (id_keys.go); nil while the store writes name-keyed parts only.
	dict atomic.Pointer[v2dict.Dict]
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
func (h *PolicyIdentity) Name() string {
	if p := h.name.Load(); p != nil {
		return unsafe.String(p, int(h.nameLen))
	}
	var buf [256]byte
	name, _, ok := h.catalog.partDict().LookupID(string(h.policy.Group), h.vshard, h.dictID.Load()-1, buf[:0])
	if !ok {
		panic("v2core: identity lost its dictionary name")
	}
	return string(name)
}

func (h *PolicyIdentity) metricName() partsformat.ID { return partsformat.ID(h.Name()) }

// withName retains immutable, owned bytes only until the dictionary owns them.
func (h *PolicyIdentity) withName(name string) *PolicyIdentity {
	h.nameLen = uint32(len(name))
	h.name.Store(unsafe.StringData(name))
	return h
}

// rememberDictID publishes the reference before releasing the admission bytes.
// A reader that loaded the old pointer keeps those bytes alive on its stack.
func (h *PolicyIdentity) rememberDictID(id uint32) {
	h.dictID.Store(id + 1)
	if h.catalog.partDict() != nil {
		h.name.Store(nil)
	}
}

// definitionBytes reconstructs the unchanged canonical journal/part encoding.
// Ordinary routing and registered application lookup need only the handle.
func (h *PolicyIdentity) definitionBytes() []byte {
	return h.appendDefinition(make([]byte, 0, int(h.nameLen)+len(h.policy.suffix)+16))
}

// appendDefinition writes the canonical definition directly into an owner arena.
func (h *PolicyIdentity) appendDefinition(out []byte) []byte {
	if p := h.name.Load(); p != nil {
		return h.appendDefinitionName(out, unsafe.String(p, int(h.nameLen)))
	}
	if !h.plain {
		return h.appendDefinitionName(out, h.Name())
	}
	// Decode into the caller's arena rather than allocating a temporary name.
	out = append(out, `{"Name":"`...)
	out, _, ok := h.catalog.partDict().LookupID(string(h.policy.Group), h.vshard, h.dictID.Load()-1, out)
	if !ok {
		panic("v2core: identity lost its dictionary name")
	}
	return append(out, h.policy.suffix...)
}

// appendDefinitionName reuses a name already validated by a directory or scan.
func (h *PolicyIdentity) appendDefinitionName(out []byte, name string) []byte {
	out = append(out, `{"Name":"`...)
	if h.plain {
		out = append(out, name...)
	} else {
		encoded, _ := json.Marshal(name)
		out = append(out, encoded[1:len(encoded)-1]...)
	}
	return append(out, h.policy.suffix...)
}
func (h *PolicyIdentity) definitionEqual(raw []byte) bool {
	if !h.plain {
		return bytes.Equal(h.definitionBytes(), raw)
	}
	const prefix = `{"Name":"`
	n := len(prefix) + int(h.nameLen)
	return len(raw) == n+len(h.policy.suffix) && bytes.HasPrefix(raw, []byte(prefix)) && h.matchesNameBytes(raw[len(prefix):n]) && bytes.Equal(raw[n:], h.policy.suffix)
}

// matchesName checks exact dictionary bytes through its forward index. Reverse
// lookup would decompress and allocate the same name again during recovery.
func (h *PolicyIdentity) matchesName(name string) bool {
	return h.matchesNameBytes([]byte(name))
}

func (h *PolicyIdentity) matchesNameBytes(name []byte) bool {
	if len(name) != int(h.nameLen) {
		return false
	}
	if p := h.name.Load(); p != nil {
		return bytes.Equal(name, unsafe.Slice(p, int(h.nameLen)))
	}
	e, ok := h.catalog.partDict().Lookup(string(h.policy.Group), h.vshard, name)
	return ok && !e.Deleted() && e.ID+1 == h.dictID.Load()
}

func detachedPolicy(id uint64, p RollupPolicy, raw []byte, prepared *sealrollup.Prepared) *PolicyIdentity {
	name, _ := json.Marshal(string(p.Name))
	suffix := bytes.Clone(raw[len(`{"Name":`)+len(name)-1:])
	return (&PolicyIdentity{id: id, policy: policyName{policyDefinition: &policyDefinition{suffix: suffix, prepared: prepared, Group: p.Group}}, plain: canonicalNameEnd(raw) > 0, vshard: partsformat.Route(p.Name), nameHash: namefp.Hash(string(p.Name)), route: uint32(namefp.Route(p.Name) >> 32)}).withName(string(p.Name))
}

func policyIdentityMatches(old *PolicyIdentity, name string, definition []byte) (*PolicyIdentity, error) {
	if !old.definitionEqual(definition) {
		return nil, errors.New("stored rollup policy changed")
	}
	if name != "" {
		// The canonical definition already checked these bytes against the
		// dictionary. Compare the supplied name to them without a second lookup.
		matches := false
		if old.plain {
			matches = bytes.Equal([]byte(name), definition[len(`{"Name":"`):len(`{"Name":"`)+int(old.nameLen)])
		} else {
			matches = old.matchesName(name)
		}
		if !matches {
			return nil, errors.New("stored rollup policy changed")
		}
	}
	return old, nil
}

func (c *PolicyCatalog) publish(h *PolicyIdentity) (*PolicyIdentity, error) {
	p := c.idPage(h.id>>10, true)
	slot := &p[h.id&1023]
	if slot.CompareAndSwap(nil, h) {
		return h, nil
	}
	return policyIdentityMatches(slot.Load(), string(h.metricName()), h.definitionBytes())
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
			h := (&PolicyIdentity{catalog: c, vshard: partsformat.Route(partsformat.ID(ownedName)), id: id, policy: policyName{policyDefinition: template.policy.policyDefinition}, plain: true, nameHash: namefp.Hash(ownedName), route: uint32(namefp.Route(ownedName) >> 32)}).withName(ownedName)
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

// idPage returns an epoch-owned page. Directory growth copies pointers only;
// holding pagesMu prevents a concurrent creator from publishing into an old copy.
func (c *PolicyCatalog) idPage(id uint64, create bool) *policyIDPage {
	if id >= denseIDPages {
		p, _ := c.ids.Load(id)
		if p == nil && create {
			p, _ = c.ids.LoadOrStore(id, &policyIDPage{})
		}
		if p != nil {
			return p.(*policyIDPage)
		}
		return nil
	}
	d := c.pages.Load()
	if d != nil && id < uint64(len(d.pages)) {
		if p := d.pages[id].Load(); p != nil {
			return p
		}
	}
	if !create {
		return nil
	}
	c.pagesMu.Lock()
	defer c.pagesMu.Unlock()
	d = c.pages.Load()
	if d == nil || id >= uint64(len(d.pages)) {
		n := 1
		for uint64(n) <= id {
			n *= 2
		}
		next := &policyIDPages{pages: make([]atomic.Pointer[policyIDPage], n)}
		if d != nil {
			for i := range d.pages {
				next.pages[i].Store(d.pages[i].Load())
			}
		}
		c.pages.Store(next)
		d = next
	}
	p := d.pages[id].Load()
	if p == nil {
		p = &policyIDPage{}
		d.pages[id].Store(p)
	}
	return p
}

func (c *PolicyCatalog) lookup(id uint64) *PolicyIdentity {
	if p := c.idPage(id>>10, false); p != nil {
		return p[id&1023].Load()
	}
	return nil
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
	out := h.metadataScalars(m)
	out.Policy = h.definitionBytes()
	return out
}

// Planning carries immutable identity separately until the part is encoded.
func (h *PolicyIdentity) metadataScalars(m metricMetadata) RollupMetadata {
	return RollupMetadata{BaselineOrder: m.BaselineOrder, ResetOrder: m.ResetOrder, Expired: m.Expired, Version: m.Version, ID: h.id, Frontier: sealrollup.Frontier{Order: m.Frontier.Order, MaxTime: m.Frontier.MaxTime, Policy: h.policy.prepared.Digest()}, LastUpdate: m.LastUpdate, Revision: m.Revision}
}
func (m *readMetricState) metadata() RollupMetadata { return m.PolicyIdentity.metadata(m.meta) }

// Directory keys were validated when their immutable identity was installed.
func (m *readMetricState) metadataForName(name string) (RollupPolicy, RollupMetadata) {
	h := m.PolicyIdentity
	out := h.metadataScalars(m.meta)
	out.Policy = h.appendDefinitionName(make([]byte, 0, len(name)+len(h.policy.suffix)+16), name)
	return RollupPolicy{Name: partsformat.ID(name), Group: h.policy.Group, Config: h.schema()}, out
}

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
	return m.Frontier.Order > other.Frontier.Order || m.Frontier.Order == other.Frontier.Order && (m.Revision > other.Revision)
}
