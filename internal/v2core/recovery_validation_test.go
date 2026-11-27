package v2core

import (
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Recovery needs the identity and scalar frontier, not another encoded policy
// per envelope. Keep that boundary allocation-free without relaxing validation.
func TestRecoveryEnvelopeValidation(t *testing.T) {
	policy, raw := rollupTestPolicy(t, "recovery.validation", "1s:1h,1m:1d", sealrollup.Average, .5)
	catalog := &PolicyCatalog{}
	identity, err := catalog.intern(7, string(policy.Name), raw)
	if err != nil {
		t.Fatal(err)
	}
	valid := rollupScanEntry{group: policy.Group, identity: identity}
	valid.meta.Version = 3
	valid.meta.Frontier.Order = 9
	valid.meta.ResetOrder = 2
	valid.meta.BaselineOrder = 1
	for _, tc := range []struct {
		name string
		edit func(*rollupScanEntry)
	}{
		{"identity", func(e *rollupScanEntry) { e.identity = nil }},
		{"group", func(e *rollupScanEntry) { e.group += ".other" }},
		{"version-zero", func(e *rollupScanEntry) { e.meta.Version = 0 }},
		{"version-future", func(e *rollupScanEntry) { e.meta.Version = 5 }},
		{"frontier", func(e *rollupScanEntry) { e.meta.Frontier.Order = 0 }},
		{"baseline", func(e *rollupScanEntry) { e.meta.BaselineOrder = 10 }},
		{"reset", func(e *rollupScanEntry) { e.meta.ResetOrder = 10 }},
		{"v1-reset", func(e *rollupScanEntry) { e.meta.Version = 1 }},
		{"v1-expired", func(e *rollupScanEntry) {
			e.meta.Version, e.meta.ResetOrder, e.meta.Expired = 1, 0, true
		}},
		{"expired-frontier", func(e *rollupScanEntry) { e.meta.Expired = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := valid
			tc.edit(&entry)
			if _, err := entry.finish(policy.Name); err == nil {
				t.Fatal("invalid recovery envelope accepted")
			}
		})
	}
	t.Run("zero-id", func(t *testing.T) {
		entry := valid
		entry.identity = detachedPolicy(0, policy, raw, identity.policy.prepared)
		if _, err := entry.finish(policy.Name); err == nil {
			t.Fatal("zero numeric identity accepted")
		}
	})
	if _, err := valid.finish(v2part.ID("recovery.other")); err == nil {
		t.Fatal("wrong metric name accepted")
	}
	for version := uint8(1); version <= 4; version++ {
		entry := valid
		entry.meta.Version, entry.meta.ResetOrder = version, 0
		if version == 4 {
			// Version 4 reset and baseline fields are part sequences, which
			// may exceed the producer order frontier.
			entry.meta.BaselineOrder, entry.meta.ResetOrder, entry.meta.Expired = 20, 19, true
		}
		allocs := testing.AllocsPerRun(100, func() {
			meta, err := entry.finish(policy.Name)
			if err != nil || meta.ID != 7 || compactMetadata(meta) != entry.meta {
				t.Fatalf("recovery scalar validation: %+v, %v", meta, err)
			}
		})
		if allocs != 0 {
			t.Fatalf("version %d: recovery validation allocated %g objects per envelope", version, allocs)
		}
	}
}
