package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

func TestClassicCreationPolicies(t *testing.T) {
	dir := t.TempDir()
	schemas := filepath.Join(dir, "schemas")
	agg := filepath.Join(dir, "aggregation")
	if err := os.WriteFile(schemas, []byte("[fallback]\npattern = .*\nretentions = 60s:1d\n[special]\npattern = ^special\\.\npriority = 10\nretentions = 1s:1h,60s:1d\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(agg, []byte("[counts]\npattern = \\.count$\naggregationMethod = sum\nxFilesFactor = 0.25\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := readClassicPolicyFiles(schemas, agg)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		step   int64
		method sealrollup.Method
		xff    float32
	}{{"special.count", 1, sealrollup.Sum, .25}, {"other", 60, sealrollup.Average, .5}} {
		encoded, err := source.policy(tc.name)
		if err != nil {
			t.Fatal(err)
		}
		var p v2core.RollupPolicy
		if err = json.Unmarshal(encoded, &p); err != nil {
			t.Fatal(err)
		}
		if string(p.Name) != tc.name || p.Config.Archives[0].Step != tc.step || p.Config.Method != tc.method || p.Config.XFilesFactor != tc.xff {
			t.Fatalf("%s: %+v", tc.name, p)
		}
	}
	if _, err = readClassicPolicyFiles(schemas, ""); err == nil {
		t.Fatal("accepted unpaired files")
	}
	source.schemas = nil
	if _, err = source.policy("missing"); err == nil {
		t.Fatal("accepted name without schema")
	}
}
