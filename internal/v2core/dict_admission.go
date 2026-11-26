package v2core

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"slices"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/v2dict"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

// AdmitNames gives every identity in hs that has no dictionary ID yet its
// ID now, with its durable policy suffix, then syncs the dictionary once:
// the producer's drain calls it where it syncs its catalog, so a name is
// durable in the dictionary before any L0 run or seal carries its points,
// and a seal finds the ID already present. Identities may repeat. It is a
// no-op without a store dictionary (name-keyed parts). hs is reordered.
func (e *coreEngine) AdmitNames(hs []*ApplicationPolicy) error {
	d := e.identities.partDict()
	if d == nil || len(hs) == 0 {
		return nil
	}
	// AppendBatch takes one shard lock per run of one shard; a sort by
	// vshard makes each shard's new names one run, and by identity puts a
	// name's repeats next to each other, where dictAppend skips them.
	slices.SortFunc(hs, func(a, b *ApplicationPolicy) int {
		if a.vshard != b.vshard {
			return int(a.vshard) - int(b.vshard)
		}
		return cmp.Compare(uintptr(unsafe.Pointer(a)), uintptr(unsafe.Pointer(b)))
	})
	if err := e.identities.dictAppend(d, hs); err != nil {
		return err
	}
	return d.Sync() // no fsync when an earlier Sync covers every append
}

// dictAppend appends the names of ms that have no dictionary ID yet, each
// with its policy suffix index, and remembers their IDs. The caller syncs.
func (c *PolicyCatalog) dictAppend(d *v2dict.Dict, ms []*PolicyIdentity) error {
	var scratch [dictChunk]v2dict.NewName
	var metrics [dictChunk]*PolicyIdentity
	n := 0
	appendNames := func() error {
		err := d.AppendBatch(scratch[:n])
		if err == nil {
			for i := range n {
				metrics[i].rememberDictID(scratch[i].Entry.ID)
			}
		}
		n = 0
		return err
	}
	for _, m := range ms {
		if m.dictID.Load() != 0 || (n > 0 && metrics[n-1] == m) {
			continue
		}
		policy, err := m.dictPolicy(d)
		if err != nil {
			return err
		}
		scratch[n], metrics[n] = v2dict.NewName{Group: string(m.policy.Group), VShard: m.vshard, Name: string(m.metricName()), Policy: policy}, m
		if n++; n == dictChunk {
			if err := appendNames(); err != nil {
				return err
			}
		}
	}
	if n > 0 {
		return appendNames()
	}
	return nil
}

// dictPolicy returns the dictionary's index of h's policy suffix, interning
// it once per shared definition.
func (h *PolicyIdentity) dictPolicy(d *v2dict.Dict) (uint16, error) {
	def := h.policy.policyDefinition
	if i := def.dictPolicy.Load(); i != 0 {
		return uint16(i), nil
	}
	i, err := d.InternPolicy(def.suffix)
	if err != nil {
		return 0, err
	}
	def.dictPolicy.Store(uint32(i))
	return i, nil
}

// DictID returns the name's store dictionary ID once admission, a seal or a
// read resolved it.
func (h *PolicyIdentity) DictID() (uint32, bool) {
	id := h.dictID.Load()
	return id - 1, id != 0
}

// NeedsDictID reports a name the next AdmitNames must resolve: the store
// writes ID-keyed parts and the name has no dictionary ID in this process.
func (h *PolicyIdentity) NeedsDictID() bool {
	return h.dictID.Load() == 0 && h.catalog != nil && h.catalog.partDict() != nil
}

// DictionaryDefinition rebuilds the name's canonical definition from the
// store dictionary alone: its name bytes and its policy suffix. It reports
// false before the name has a dictionary ID and for an entry recorded with
// policy 0 (before the policy table existed; the catalog holds it).
func (h *PolicyIdentity) DictionaryDefinition() ([]byte, bool) {
	id, ok := h.DictID()
	if !ok || h.catalog == nil {
		return nil, false
	}
	return DictionaryDefinition(h.catalog.partDict(), string(h.policy.Group), h.vshard, id)
}

// DictionaryDefinition is the canonical definition of (group, vshard, id)
// from dictionary d alone, encoded exactly as PolicyIdentity.Definition.
func DictionaryDefinition(d *v2dict.Dict, group string, vshard uint16, id uint32) ([]byte, bool) {
	if d == nil {
		return nil, false
	}
	out := append(make([]byte, 0, 256), `{"Name":"`...)
	start := len(out)
	out, e, ok := d.LookupID(group, vshard, id, out)
	if !ok || e.Deleted() {
		return nil, false
	}
	suffix, ok := d.Policy(e.Policy)
	if !ok {
		return nil, false
	}
	if canonicalNameEnd(append(out, '"')) != len(out) {
		// Not a plain name: the identity encodes it through encoding/json.
		encoded, _ := json.Marshal(string(out[start:]))
		out = append(out[:start], encoded[1:len(encoded)-1]...)
	}
	return append(out, suffix...), true
}

// Dictionary returns the store name dictionary, nil while the store writes
// name-keyed parts only.
func (e *coreEngine) Dictionary() *v2dict.Dict { return e.identities.partDict() }

// AdmitIdentity registers name under its packed dictionary ID: the group
// comes from definition, the vshard from the name, and the dictionary entry
// is looked up or appended (no fsync; the producer's drain syncs before core
// sees the points). It returns nil without a store dictionary; the caller
// keeps its catalog-derived ID. A deleted name revives under a new ID.
func (e *coreEngine) AdmitIdentity(name string, definition []byte) (*ApplicationPolicy, error) {
	return e.identities.admit(name, definition)
}

func (c *PolicyCatalog) admit(name string, definition []byte) (*PolicyIdentity, error) {
	d := c.partDict()
	if d == nil {
		return nil, nil
	}
	group, suffix, err := c.admissionGroup(definition)
	if err != nil {
		return nil, err
	}
	vshard := partsformat.Route(partsformat.ID(name))
	gi, err := c.GroupIndex(group)
	if err != nil {
		return nil, err
	}
	ent, ok := d.Lookup(string(group), vshard, unsafe.Slice(unsafe.StringData(name), len(name)))
	if !ok || ent.Deleted() {
		policy, err := d.InternPolicy(suffix)
		if err != nil {
			return nil, err
		}
		if ent, _, err = d.Append(string(group), vshard, unsafe.Slice(unsafe.StringData(name), len(name)), policy); err != nil {
			return nil, err
		}
	}
	id := PackID(gi, vshard, ent.ID)
	if h := c.lookup(id); h != nil && h.name.Load() == nil {
		// A dictionary-only handle (an ID-keyed part's): the lookup above
		// proved the name, so only the policy bytes are left to compare.
		if uint32(len(name)) != h.nameLen || !bytes.Equal(h.appendDefinitionName(nil, name), definition) {
			return nil, errors.New("stored rollup policy changed")
		}
		return h, nil
	}
	h, err := c.intern(id, name, definition)
	if err != nil {
		return nil, err
	}
	h.rememberDictID(ent.ID)
	return h, nil
}

// admissionGroup returns definition's group and policy suffix, parsing the
// JSON only for the first name of a schema or an escaped name.
func (c *PolicyCatalog) admissionGroup(definition []byte) (partsformat.Group, []byte, error) {
	if n := canonicalNameEnd(definition); n > 0 {
		if v, ok := c.definitions.Load(string(definition[n:])); ok {
			return v.(*policyDefinition).Group, definition[n:], nil
		}
	}
	var p RollupPolicy
	if err := json.Unmarshal(definition, &p); err != nil {
		return "", nil, err
	}
	enc, err := json.Marshal(string(p.Name))
	if start := len(`{"Name":`) + len(enc) - 1; err != nil || len(definition) <= start {
		return "", nil, errors.New("noncanonical rollup policy")
	} else {
		return p.Group, definition[start:], nil
	}
}

// ID returns the engine ID the handle is registered under.
func (h *PolicyIdentity) ID() uint64 { return h.id }
