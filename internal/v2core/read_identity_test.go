package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestReadValidationCost(t *testing.T) {
	d, err := v2dict.Open(t.TempDir(), v2dict.Options{BufferBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := &PolicyCatalog{}
	c.dict.Store(d)
	name := "read.validation." + strings.Repeat("long.metric.", 30)
	policy, raw := rollupTestPolicy(t, name, "1s:1h,1m:1d", sealrollup.Last, .5)
	config, _ := json.Marshal(policy.Config)
	v2part.RegisterPolicies([]v2part.ArchivePolicy{{Digest: sha256.Sum256(config), Definition: config}})
	h, err := c.admit(name, raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 512; i++ {
		_, _, err = d.Append(string(h.policy.Group), h.vshard, []byte(fmt.Sprintf("prefix.%04d", i)), 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	e, _, err := d.Append(string(h.policy.Group), h.vshard, []byte(name), 0)
	if err != nil {
		t.Fatal(err)
	}
	h.rememberDictID(e.ID)
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	key := v2part.Key{Group: h.policy.Group, VShard: h.vshard, Local: e.ID, Metric: v2part.ID(name), ByID: true}
	meta := h.metadataScalars(metricMetadata{Version: 3})
	meta.Frontier.Order = 1
	encoded := appendRollupMetadataSlim(nil, meta)
	var definition []byte
	start := time.Now()
	allocs := testing.AllocsPerRun(1000, func() {
		got, slim, err := decodeRollupMetadataHeader(encoded)
		if err != nil {
			panic(err)
		}
		_, err = c.readIdentity(got, slim, key, &definition)
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("known slim identity allocated %.0f times", allocs)
	}
	t.Logf("READ_VALIDATION ns/row=%d allocs/row=%.0f", time.Since(start).Nanoseconds()/1001, allocs)
}

// Repeated slim rows carry numeric identity plus a digest. They must not read
// canonical name bytes again; full rows and new identities still validate them.
func TestReadIdentityDictionaryValidation(t *testing.T) {
	for _, name := range []string{"identity.metric", `identity.\"escaped`, "identity.<html>", "identity.é"} {
		t.Run(name, func(t *testing.T) {
			d, err := v2dict.Open(t.TempDir(), v2dict.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			c := &PolicyCatalog{}
			c.dict.Store(d)
			policy, raw := rollupTestPolicy(t, name, "1s:1h,1m:1d", sealrollup.Last, .5)
			config, _ := json.Marshal(policy.Config)
			v2part.RegisterPolicies([]v2part.ArchivePolicy{{Digest: sha256.Sum256(config), Definition: config}})
			h, e := unboundPacked(t, c, d, name, raw)
			if err = d.Flush(); err != nil {
				t.Fatal(err)
			}
			key := v2part.Key{Group: h.policy.Group, VShard: h.vshard, ByID: true, Local: e.ID, Metric: v2part.ID(name)}
			meta := h.metadataScalars(metricMetadata{Version: 3})
			meta.Frontier.Order = 5
			var definition []byte
			// First encounter must validate the canonical definition and bind the ID.
			got, err := c.readIdentity(meta, true, key, &definition)
			if err != nil || got != h || !bytes.Equal(definition, raw) || h.dictID.Load() != e.ID+1 {
				t.Fatalf("first encounter: %v", err)
			}
			// Poison only the handle's byte resolver. The catalog still owns the dictionary.
			h.catalog = &PolicyCatalog{}
			got, err = c.readIdentity(meta, true, key, &definition)
			if err != nil || got != h {
				t.Fatalf("known identity: %v", err)
			}
			state := readMetricState{PolicyIdentity: h, meta: compactMetadata(meta)}
			snapshot := &RollupSnapshot{metrics: map[string]*snapshotMetric{name: {latest: state}}}
			if err = snapshot.MetadataContext(context.Background(), func(n string, p RollupPolicy, m RollupMetadata) {
				if n != name || p.Name != v2part.ID(name) || !bytes.Equal(m.Policy, raw) {
					t.Fatal("directory reconstruction changed bytes")
				}
			}); err != nil {
				t.Fatal(err)
			}
			h.catalog = c
			// A legacy ID-keyed part carries its old NumericID; the key names
			// the identity.
			legacy := meta
			legacy.ID = 7
			if got, err = c.readIdentity(legacy, true, key, &definition); err != nil || got != h {
				t.Fatalf("legacy NumericID meta.ID: %v", err)
			}
			_, otherRaw := rollupTestPolicy(t, name+".other", "1s:1h,1m:1d", sealrollup.Last, .5)
			other, entry := unboundPacked(t, c, d, name+".other", otherRaw)
			other.rememberDictID(entry.ID)
			for _, change := range []struct {
				name   string
				mutate func(*RollupMetadata, *v2part.Key)
			}{
				{"global ID", func(m *RollupMetadata, _ *v2part.Key) { m.ID = other.id }},
				{"dictionary ID", func(_ *RollupMetadata, k *v2part.Key) { k.Local++ }},
				{"group", func(_ *RollupMetadata, k *v2part.Key) { k.Group = "changed" }},
				{"shard", func(_ *RollupMetadata, k *v2part.Key) { k.VShard = (k.VShard + 1) % v2part.VirtualShards }},
				{"digest", func(m *RollupMetadata, _ *v2part.Key) { m.Frontier.Policy[0] ^= 1 }},
				{"reset", func(m *RollupMetadata, _ *v2part.Key) { m.ResetOrder = 6 }},
				{"expired", func(m *RollupMetadata, _ *v2part.Key) { m.Expired = true }},
				{"frontier", func(m *RollupMetadata, _ *v2part.Key) { m.Frontier.Order = 0 }},
				{"version", func(m *RollupMetadata, _ *v2part.Key) { m.Version = 0 }},
			} {
				t.Run(change.name, func(t *testing.T) {
					m, k := meta, key
					change.mutate(&m, &k)
					if _, err := c.readIdentity(m, true, k, &definition); err == nil {
						t.Fatal("accepted corruption")
					}
				})
			}
			full := meta
			full.Policy = raw
			if _, err = c.readIdentity(full, false, key, &definition); err != nil {
				t.Fatal(err)
			}
			_, changed := rollupTestPolicy(t, name+"changed", "1s:1h,1m:1d", sealrollup.Last, .5)
			full.Policy = append([]byte(" "), raw...)
			if _, err = c.readIdentity(full, false, key, &definition); err == nil {
				t.Fatal("accepted noncanonical full definition")
			}
			full.Policy = changed
			if _, err = c.readIdentity(full, false, key, &definition); err == nil {
				t.Fatal("accepted changed full definition")
			}
			// First encounters cannot use a conflicting numeric identity or canonical name.
			h.dictID.Store(0)
			h.withName(name)
			bad := key
			bad.Metric = v2part.ID(name + "changed")
			if _, err = c.readIdentity(meta, true, bad, &definition); err == nil {
				t.Fatal("accepted changed first name")
			}
			h.rememberDictID(e.ID)
			// Real call sites resolve dictionary keys before the slim shortcut: old IDs
			// remain invalid after deletion and revival, even if a stale handle exists.
			if _, err = d.Delete(string(key.Group), key.VShard, []byte(name)); err != nil {
				t.Fatal(err)
			}
			if h.matchesName(name) {
				t.Fatal("accepted deleted name")
			}
			if err := (&keyNames{dict: d}).resolve(&key); err == nil {
				t.Fatal("resolved deleted ID")
			}
			next, _, err := d.Append(string(key.Group), key.VShard, []byte(name), 0)
			if err != nil {
				t.Fatal(err)
			}
			if next.ID == e.ID || h.matchesName(name) {
				t.Fatal("revived name reused stale ID")
			}
			if err := (&keyNames{dict: d}).resolve(&key); err == nil {
				t.Fatal("resolved old revived ID")
			}
		})
	}
}

// unboundPacked appends name to d and interns its packed ID without binding
// the dictionary ID, as a part's first read finds it.
func unboundPacked(t *testing.T, c *PolicyCatalog, d *v2dict.Dict, name string, raw []byte) (*PolicyIdentity, v2dict.Entry) {
	t.Helper()
	group, _, err := c.admissionGroup(raw)
	if err != nil {
		t.Fatal(err)
	}
	vshard := partsformat.Route(partsformat.ID(name))
	e, _, err := d.Append(string(group), vshard, []byte(name), 0)
	if err != nil {
		t.Fatal(err)
	}
	gi, err := c.GroupIndex(group)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.intern(PackID(gi, vshard, e.ID), name, raw)
	if err != nil {
		t.Fatal(err)
	}
	return h, e
}
