package v2core

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestPolicyCatalogImmutableOwnership(t *testing.T) {
	for _, name := range []string{"shared.metric", `escaped.\"metric`, "unicode.é.metric", "escaped.<metric>"} {
		t.Run(name, func(t *testing.T) {
			_, raw := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
			original := slices.Clone(raw)
			c := &PolicyCatalog{}
			h, err := c.intern(1, name, raw)
			if err != nil {
				t.Fatal(err)
			}
			clear(raw)
			if h.catalog != c || string(h.policy.Name) != name || !bytes.Equal(h.definition, original) {
				t.Fatal("borrowed caller memory")
			}
			_ = append(h.definition, 255)
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					for range 20 {
						got, err := c.intern(1, name, original)
						if err != nil || got != h {
							t.Error("identity not shared", err)
						}
					}
				})
			}
			wg.Wait()
			if _, err := c.intern(1, "changed", original); err == nil {
				t.Fatal("accepted changed identity")
			}
			_, changed := rollupTestPolicy(t, name, "1s:1d", sealrollup.Sum, .5)
			if _, err := c.intern(1, name, changed); err == nil {
				t.Fatal("accepted changed schema")
			}
		})
	}
}

func TestPolicyCatalogSharedWithPinnedReadDirectory(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	c := &PolicyCatalog{}
	e, err := openCoreWithHooks(t.TempDir(), cfg, Hooks{Identities: c})
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	_, raw := rollupTestPolicy(t, "shared.metric", "1s:1d", sealrollup.Last, .5)
	if err := e.submitBatches(0, []Batch{{ID: 1, Order: 1, Clock: 1700000002, Policy: raw, Points: []Row{{ID: 1, Time: 1700000000, Bits: 1, Seq: 1}}}}, 1, true); err != nil {
		t.Fatal(err)
	}
	snap := e.owners[0].db.NewSnapshot()
	defer snap.Close()
	index, err := NewRollupSnapshotWithCatalog(context.Background(), snap, nil, c)
	if err != nil {
		t.Fatal(err)
	}
	h := c.lookup(1)
	if index.metrics["shared.metric"].latest.PolicyIdentity != h {
		t.Fatal("reader copied immutable identity")
	}
	name, definition, err := e.BindDefinition(1, "shared.metric", raw)
	if err != nil {
		t.Fatal(err)
	}
	if name != "shared.metric" || &definition[0] != &h.definition[0] {
		t.Fatal("journal binding copied policy")
	}
	if err := ScanRollups(snap, func(p RollupPolicy, m RollupMetadata, want []sealrollup.Record) error {
		got, err := index.Records(context.Background(), string(p.Name))
		if err != nil {
			return err
		}
		if !slices.Equal(got, want) {
			t.Fatal("oracle mismatch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyCatalogMetadataRoundTrip(t *testing.T) {
	_, raw := rollupTestPolicy(t, "shared.metric", "1s:1d", sealrollup.Last, .5)
	h, err := (&PolicyCatalog{}).intern(7, "shared.metric", raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, baseline := range []uint64{0, 3} {
		want := RollupMetadata{Version: 2, ID: 7, Policy: h.definition, BaselineOrder: baseline, ResetOrder: 4, LastUpdate: 100, Revision: 8, Frontier: sealrollup.Frontier{Order: 9, MaxTime: 99, Policy: h.prepared.Digest()}}
		if got := h.metadata(compactMetadata(want)); !reflect.DeepEqual(got, want) {
			t.Fatalf("compact metadata lost state: got %+v want %+v", got, want)
		}
	}
}
