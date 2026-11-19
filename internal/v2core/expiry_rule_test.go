package v2core

import (
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// TestExpiryRuleResolvedOncePerName: idle scans call ExpiryRule once per name
// and hand its result to every later Expire call for that name.
func TestExpiryRuleResolvedOncePerName(t *testing.T) {
	if s := unsafe.Sizeof(rollupMetric{}); s != 24 {
		t.Fatalf("rollupMetric is %d B; the expiry rule must fit its padding", s)
	}
	const names = 64
	r := newCoreRollups()
	for i := 0; i < names; i++ {
		p, err := EncodeRollupPolicy(RollupPolicy{Name: partsformat.ID(fmt.Sprintf("n.%d", i)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 60}}}})
		if err != nil {
			t.Fatal(err)
		}
		m, err := r.metric(uint64(i+1), p)
		if err != nil {
			t.Fatal(err)
		}
		r.install(uint64(i+1), m, freshMetadata)
	}
	resolved := map[string]int{}
	r.expiryRule = func(p RollupPolicy) uint16 {
		resolved[string(p.Name)]++
		var i int
		fmt.Sscanf(string(p.Name), "n.%d", &i)
		return uint16(i % 7)
	}
	got := map[string]uint16{}
	r.expire = func(rule uint16, p RollupPolicy, _ RollupMetadata, _ time.Time) bool {
		got[string(p.Name)] = rule
		return false
	}
	for scan := 0; scan < 3; scan++ {
		for id, m := range r.idleCandidates() {
			r.expires(m, m.metadata(r.metaOf(id)), time.Unix(1, 0))
		}
	}
	for i := 0; i < names; i++ {
		name := fmt.Sprintf("n.%d", i)
		if resolved[name] != 1 || got[name] != uint16(i%7) {
			t.Fatalf("%s: resolved %d times, Expire saw rule %d, want 1 and %d", name, resolved[name], got[name], i%7)
		}
	}
	// Without ExpiryRule, Expire sees rule 0.
	r.expiryRule = nil
	for _, m := range r.metrics {
		m.expiry = 0
		r.expires(m, RollupMetadata{}, time.Unix(1, 0))
		if got[string(m.policy.Name)] != 0 {
			t.Fatalf("%s: rule %d without ExpiryRule", m.policy.Name, got[string(m.policy.Name)])
		}
	}
}
