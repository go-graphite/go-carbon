package v2core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Advancing a recovery iterator may overwrite its key/value and the shared
// definition workspace. Every published identity must still encode its own name.
func TestRecoveryMetadataOwnsWorkspace(t *testing.T) {
	for _, catalog := range []*PolicyCatalog{nil, {}} {
		workspace := make([]byte, 0, 4096)
		var entries []*rollupScanEntry
		var definitions [][]byte
		names := []string{"seed.metric", "first.metric", "second.metric.longer", `escaped."metric`, "unicode.é.metric"}
		for i, name := range names {
			policy, raw := rollupTestPolicy(t, name, "1s:1h,1m:1d", sealrollup.Average, .5)
			// Recovery commonly finds a schema already interned in this shard.
			if catalog != nil {
				if _, err := catalog.intern(uint64(i+1000), name, raw); err != nil {
					t.Fatal(err)
				}
			}
			config, err := json.Marshal(policy.Config)
			if err != nil {
				t.Fatal(err)
			}
			v2part.RegisterPolicies([]v2part.ArchivePolicy{{Digest: sha256.Sum256(config), Definition: config}})
			digest, err := sealrollup.PolicyDigest(policy.Config)
			if err != nil {
				t.Fatal(err)
			}
			meta := RollupMetadata{ID: uint64(i + 1), Version: 3, Policy: raw, Revision: uint64(i + 1), Frontier: sealrollup.Frontier{Order: uint64(i + 1), Policy: digest}}
			encoded := appendRollupMetadataSlim(nil, meta)
			if i%2 == 1 {
				encoded = AppendRollupMetadata(nil, meta) // borrowed legacy/full envelope too
			}
			entry := &rollupScanEntry{group: policy.Group}
			key := v2part.Key{Metric: policy.Name, Group: policy.Group, Archive: rollupMetadataArchive}
			if err := entry.addMetadata(catalog, key, encoded, &workspace); err != nil {
				t.Fatal(err)
			}
			clear(encoded)
			entries = append(entries, entry)
			definitions = append(definitions, bytes.Clone(raw))
		}
		clear(workspace[:cap(workspace)])
		for i, entry := range entries {
			if got := entry.identity.Definition(); !bytes.Equal(got, definitions[i]) || entry.identity.Name() != names[i] {
				t.Fatalf("identity %d retained borrowed recovery bytes: name=%q definition=%s", i, entry.identity.Name(), got)
			}
		}
	}
}
