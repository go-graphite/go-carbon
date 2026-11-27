package v2core

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Sealed envelopes are the only source of committed owner scalars. An obsolete
// checkpoint must neither affect recovery nor be rewritten by a later seal.
func TestPartsOnlyRecoveryScalars(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg, policies := recoveryTestSetup(t)
	root := t.TempDir()
	hooks := Hooks{Now: func() time.Time { return time.Unix(1794701400, 0) }}
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	recoveryFixture(t, e, policies, 0, 32)
	want := ownerStateDigest(t, e)
	renders := rollupScanDigest(t, e)
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	obsolete := filepath.Join(root, "owner-000", "rollup-state.ckpt")
	if _, err = os.Stat(obsolete); !os.IsNotExist(err) {
		t.Fatalf("checkpoint created: %v", err)
	}
	poison := []byte("obsolete checkpoint must be ignored")
	if err = os.WriteFile(obsolete, poison, 0600); err != nil {
		t.Fatal(err)
	}
	e, err = Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if got := ownerStateDigest(t, e); got != want {
		t.Fatalf("recovery scalars differ:\n%s\nwant:\n%s", got, want)
	}
	if got := rollupScanDigest(t, e); got != renders {
		t.Fatal("recovery changed renders")
	}
	recoveryFixture(t, e, policies, 32, 7)
	want = ownerStateDigest(t, e)
	mergeDrain(t, e.owners[0])
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := ownerStateDigest(t, e); got != want {
		t.Fatalf("merged recovery scalars differ:\n%s\nwant:\n%s", got, want)
	}
	if got, err := os.ReadFile(obsolete); err != nil || string(got) != string(poison) {
		t.Fatalf("obsolete checkpoint rewritten: %q %v", got, err)
	}
}

// Bootstrap also validates sample bounds, independently of envelope-only open.
func TestBootstrapValidatesSamples(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	policy, raw := rollupTestPolicy(t, "baseline.validation", "1s:120,10s:1h", sealrollup.Last, .5)
	digest, err := sealrollup.PolicyDigest(policy.Config)
	if err != nil {
		t.Fatal(err)
	}
	key := v2part.Key{Group: policy.Group, VShard: v2part.Route(policy.Name), Part: v2part.PartID{1}, Metric: policy.Name, BlockStart: 40}
	encoded, err := v2part.Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	record := sealrollup.Record{Delta: sealrollup.Delta{Archive: 0, Slot: 40, Order: 2, Sample: sealrollup.Sample{Time: 1000, Bits: 1, Sequence: 2}}}
	key.Archive, key.BlockStart = v2part.MetadataArchive, 0
	envelope, err := v2part.Encode(key)
	if err != nil {
		t.Fatal(err)
	}
	meta := RollupMetadata{Version: 3, ID: 7, Policy: raw, Frontier: sealrollup.Frontier{Order: 1, MaxTime: 1000, Policy: digest}}
	rows := []KV{{Key: encoded, Value: sealrollup.EncodeRecord(record)}, {Key: envelope, Value: AppendRollupMetadata(nil, meta)}}
	err = BootstrapRollups(filepath.Join(t.TempDir(), "bootstrap"), cfg, rows, v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "baseline"}, nil)
	if err == nil || !strings.Contains(err.Error(), "rollup order exceeds metadata frontier") {
		t.Fatalf("sample beyond envelope frontier accepted: %v", err)
	}
}
