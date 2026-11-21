package v2core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

func TestPolicyCatalogEncodingOracle(t *testing.T) {
	c := &PolicyCatalog{}
	_, seed := rollupTestPolicy(t, "seed.metric", "1s:1d", sealrollup.Last, .5)
	if _, err := c.intern(1, "", seed); err != nil {
		t.Fatal(err)
	}
	var cases [][]byte
	for _, name := range []string{"plain.metric", `slash.\\metric`, `quote.\"metric`, "unicode.é.metric", "html.<>&", "line.\nmetric", "", "separator.\u2028"} {
		var p RollupPolicy
		if err := json.Unmarshal(seed, &p); err != nil {
			t.Fatal(err)
		}
		p.Name = partsformat.ID(name)
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, raw)
	}
	for i := range seed {
		for _, v := range []byte{0, '"', '\\', 0xff, ' '} {
			raw := bytes.Clone(seed)
			raw[i] = v
			cases = append(cases, raw)
		}
	}
	cases = append(cases, append(bytes.Clone(seed), ' '), seed[:len(seed)-1])
	for i, raw := range cases {
		var policy RollupPolicy
		decodeErr := json.Unmarshal(raw, &policy)
		canonical, encodeErr := EncodeRollupPolicy(policy)
		want := decodeErr == nil && encodeErr == nil && bytes.Equal(raw, canonical)
		h, err := c.intern(uint64(i+2), "", raw)
		if (err == nil) != want {
			t.Fatalf("case %d accepted=%v want=%v raw=%q err=%v", i, err == nil, want, raw, err)
		}
		if want && (h.rollupPolicy().Name != policy.Name || !bytes.Equal(h.definitionBytes(), raw)) {
			t.Fatalf("case %d differs from JSON oracle", i)
		}
	}
	for i := range c.shards {
		if shard := c.shards[i].Load(); shard != nil && (len(shard.encodings) > 64 || len(shard.schemas) > 64) {
			t.Fatal("unbounded schema cache")
		}
	}
}

func BenchmarkPolicyCatalogIntern(b *testing.B) {
	var raws [][]byte
	for i := 0; i < 1024; i++ {
		p := RollupPolicy{Name: partsformat.ID(fmt.Sprintf("app.region.component.host%08d.requests", i)), Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}}}}
		raw, err := EncodeRollupPolicy(p)
		if err != nil {
			b.Fatal(err)
		}
		raws = append(raws, raw)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := &PolicyCatalog{}
		for j, raw := range raws {
			if _, err := c.intern(uint64(j+1), "", raw); err != nil {
				b.Fatal(err)
			}
		}
	}
}
