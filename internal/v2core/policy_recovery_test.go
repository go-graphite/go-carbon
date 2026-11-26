package v2core

import (
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"testing"
)

func TestPolicyRecoveryExact(t *testing.T) {
	d, err := v2dict.Open(t.TempDir(), v2dict.Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := &PolicyCatalog{}
	c.dict.Store(d)
	engine := &coreEngine{identities: c}
	for i, name := range []string{"plain.metric", `escaped.\"metric`, "unicode.é.metric"} {
		_, raw := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
		h, err := c.intern(uint64(i+1), name, raw)
		if err != nil {
			t.Fatal(err)
		}
		e, _, err := d.Append(string(h.policy.Group), h.vshard, []byte(name), 0)
		if err != nil {
			t.Fatal(err)
		}
		h.rememberDictID(e.ID)
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := d.Append("codec-fixture", 0, []byte("merge"), 0); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := d.MergeAll(); err != nil {
			t.Fatal(err)
		}
		r := engine.NewPolicyRecovery()
		if err := r.Bind(h.id, name, raw); err != nil {
			t.Fatal(err)
		}
		if err := r.Finish(); err != nil {
			t.Fatal(err)
		}
		// A same-length canonical name change must be caught by exact batch equality,
		// even when the suffix and numeric ID still match.
		other := "X" + name[1:]
		bad := h.appendDefinitionName(nil, other)
		r = engine.NewPolicyRecovery()
		if err := r.Bind(h.id, other, bad); err != nil {
			continue
		}
		if err := r.Finish(); err == nil {
			t.Fatal("accepted changed name")
		}
		_, bad = rollupTestPolicy(t, name, "1s:1d", sealrollup.Sum, .5)
		if err := engine.NewPolicyRecovery().Bind(h.id, name, bad); err == nil {
			t.Fatal("accepted changed policy")
		}
	}
}
