package v2part

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"sync"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// SealedPolicy is a sealed-archive policy recovered from a part policy table.
// It is shared and immutable. JSON is the exact ArchivePolicy.Definition.
type SealedPolicy struct {
	Config sealrollup.Config
	JSON   []byte
	Digest Digest // sealrollup.PolicyDigest(Config)
}

// sealedPolicies indexes every part policy table this process has written,
// parsed or validated. Keys are content hashes, so entries never conflict,
// and the table never consults a producer catalogue: a part copied to another
// host resolves from its own descriptor.
var sealedPolicies struct {
	definitions sync.Map // sha256(Definition) -> *SealedPolicy, nil when not a sealed config
	policies    sync.Map // sealrollup.PolicyDigest -> *SealedPolicy
}

// RegisterPolicies records the sealed-archive policies of a part table.
// Definitions that are not canonical sealrollup.Config JSON are ignored.
func RegisterPolicies(policies []ArchivePolicy) {
	for i := range policies {
		p := &policies[i]
		if _, ok := sealedPolicies.definitions.Load(p.Digest); ok {
			continue
		}
		if sha256.Sum256(p.Definition) != p.Digest {
			continue
		}
		var resolved *SealedPolicy
		var config sealrollup.Config
		if json.Unmarshal(p.Definition, &config) == nil {
			if canonical, err := json.Marshal(config); err == nil && bytes.Equal(canonical, p.Definition) {
				if digest, err := sealrollup.PolicyDigest(config); err == nil {
					v, _ := sealedPolicies.policies.LoadOrStore(digest, &SealedPolicy{Config: config, JSON: canonical, Digest: digest})
					resolved = v.(*SealedPolicy)
				}
			}
		}
		sealedPolicies.definitions.Store(p.Digest, resolved)
	}
}

// ResolvePolicy returns the policy whose sealrollup.PolicyDigest is digest,
// from the part policy tables registered so far.
func ResolvePolicy(digest Digest) (*SealedPolicy, bool) {
	v, ok := sealedPolicies.policies.Load(digest)
	if !ok {
		return nil, false
	}
	return v.(*SealedPolicy), true
}

// TablePolicies returns the sealrollup.PolicyDigest of every sealed-archive
// policy that the part table policies itself carries.
func TablePolicies(policies []ArchivePolicy) map[Digest]bool {
	RegisterPolicies(policies)
	out := map[Digest]bool{}
	for i := range policies {
		if v, ok := sealedPolicies.definitions.Load(policies[i].Digest); ok && v.(*SealedPolicy) != nil {
			out[v.(*SealedPolicy).Digest] = true
		}
	}
	return out
}
