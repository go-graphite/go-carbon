package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func TestMoveStageRealPartRetryAndDirectoryIsolation(t *testing.T) {
	source := t.TempDir()
	path := filepath.Join(source, "source.sst")
	policy := v2core.RollupPolicy{Name: "move.metric", Group: "test", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}}
	encoded, e := v2core.EncodeRollupPolicy(policy)
	if e != nil {
		t.Fatal(e)
	}
	digest, e := sealrollup.PolicyDigest(policy.Config)
	if e != nil {
		t.Fatal(e)
	}
	definition, _ := json.Marshal(policy.Config)
	spec := v2part.Spec{Scope: v2part.Scope{Group: "test", VShard: v2part.GroupVShard, Part: v2part.PartID{1}}, Source: v2part.Provenance{Incarnation: v2part.PartID{2}, Stream: "source-0", MinOrder: 1, MaxOrder: 1}, MinTime: 1000, MaxTime: 1000, Policies: []v2part.ArchivePolicy{{Archive: 0, Definition: definition, Digest: sha256.Sum256(definition)}}}
	w, e := v2part.NewDenseWriter(path, spec)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	key := v2part.Key{Group: "test", VShard: v2part.Route("move.metric"), Part: spec.Scope.Part, Metric: "move.metric"}
	raw := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 0, Order: 1, Sample: sealrollup.Sample{Time: 1000, Bits: math.Float64bits(2)}}}
	k, _ := v2part.Encode(key)
	if e = w.SetEncoded(k, sealrollup.EncodeRecord(raw)); e != nil {
		t.Fatal(e)
	}
	key.Archive = v2part.MetadataArchive
	k, _ = v2part.Encode(key)
	meta, _ := json.Marshal(v2core.RollupMetadata{Version: 1, ID: 9, Policy: encoded, Frontier: sealrollup.Frontier{Order: 1, MaxTime: 1000, Policy: digest}})
	if e = w.SetEncoded(k, meta); e != nil {
		t.Fatal(e)
	}
	d, e := w.Close()
	if e != nil {
		t.Fatal(e)
	}
	body, e := d.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	descriptor := path + ".descriptor"
	if e = os.WriteFile(descriptor, body, 0600); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(t.TempDir(), "read-catalog")
	args := []string{"--catalog-dir", target, "--part", path, "--descriptor", descriptor}
	for _, wantInstalled := range []bool{true, false} {
		var out bytes.Buffer
		if e = moveStage(args, &out); e != nil {
			t.Fatal(e)
		}
		var receipt struct {
			Installed bool
			Class     uint8
			Ready     bool
		}
		if e = json.Unmarshal(out.Bytes(), &receipt); e != nil {
			t.Fatal(e)
		}
		if receipt.Installed != wantInstalled || receipt.Class != 0 || receipt.Ready {
			t.Fatalf("bad receipt %+v", receipt)
		}
	}
	// The command must not adopt an arbitrary existing daemon/store directory.
	other := t.TempDir()
	if e = os.WriteFile(filepath.Join(other, "CURRENT"), []byte("unrelated store"), 0600); e != nil {
		t.Fatal(e)
	}
	args[1] = other
	if e = moveStage(args, &bytes.Buffer{}); e == nil {
		t.Fatal("accepted unrelated nonempty directory")
	}
	b, e := os.ReadFile(filepath.Join(other, "CURRENT"))
	if e != nil || string(b) != "unrelated store" {
		t.Fatal("unrelated file changed")
	}
}
