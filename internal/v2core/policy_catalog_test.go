package v2core

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
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
			if h.catalog != c || string(h.metricName()) != name || !bytes.Equal(h.definitionBytes(), original) {
				t.Fatal("borrowed caller memory")
			}
			_ = append(h.definitionBytes(), 255)
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
	index, err := NewRollupSnapshot(context.Background(), snap, nil, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := c.lookup(1)
	if index.metrics["shared.metric"].latest.PolicyIdentity != h {
		t.Fatal("reader copied immutable identity")
	}
	bound, err := e.RegisterApplicationPolicy(1, "shared.metric", raw)
	if err != nil {
		t.Fatal(err)
	}
	if bound != h || e.ApplicationPolicy(1) != h || !h.MatchesDefinition(raw) {
		t.Fatal("journal binding changed policy")
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
		want := RollupMetadata{Version: 2, ID: 7, Policy: h.definitionBytes(), BaselineOrder: baseline, ResetOrder: 4, LastUpdate: 100, Revision: 8, Frontier: sealrollup.Frontier{Order: 9, MaxTime: 99, Policy: h.policy.prepared.Digest()}}
		if got := h.metadata(compactMetadata(want)); !reflect.DeepEqual(got, want) {
			t.Fatalf("compact metadata lost state: got %+v want %+v", got, want)
		}
	}
}

func TestPolicyCatalogSharedSchemasAndPagedIDs(t *testing.T) {
	c := &PolicyCatalog{}
	var shared *policyDefinition
	for i := uint64(1); i <= 128; i++ {
		name := fmt.Sprintf("shared.catalog.metric.%d", i)
		_, raw := rollupTestPolicy(t, name, "1s:1d,1m:30d", sealrollup.Last, .5)
		h, err := c.intern(i, name, raw)
		if err != nil {
			t.Fatal(err)
		}
		if shared == nil {
			shared = h.policy.policyDefinition
		}
		if h.policy.policyDefinition != shared {
			t.Fatal("schema/key prefix was copied across virtual shards")
		}
		if got := h.definitionBytes(); !bytes.Equal(got, raw) {
			t.Fatal("canonical bytes changed")
		}
		if c.lookup(i) != h {
			t.Fatal("published handle changed")
		}
	}
	for _, id := range []uint64{1023, 1024, 1025, 1<<62 + 5} { // 2^63 and up is the packed space
		name := fmt.Sprintf("page.metric.%d", id)
		_, raw := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
		h, err := c.intern(id, name, raw)
		if err != nil {
			t.Fatal(err)
		}
		if c.lookup(id) != h || c.lookup(id-1) == h {
			t.Fatal("atomic page boundary aliased identity")
		}
	}
}

func TestPolicyCatalogConcurrentPageGrowth(t *testing.T) {
	var c PolicyCatalog
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range 4 {
		wg.Go(func() {
			<-start
			for i := range 256 {
				id := uint64((i*4+g)<<10) + uint64(g+1)
				h := &PolicyIdentity{id: id, nameHash: id * 0x9e3779b97f4a7c15}
				if got, err := c.publish(h); err != nil || got != h {
					t.Errorf("publish id %d: %v", id, err)
					return
				}
				if c.lookup(id) != h {
					t.Errorf("lost id %d during growth", id)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	for g := range 4 {
		for i := range 256 {
			id := uint64((i*4+g)<<10) + uint64(g+1)
			if c.lookup(id) == nil || c.lookup(id).NameHash() != id*0x9e3779b97f4a7c15 {
				t.Fatalf("lost id %d after growth", id)
			}
		}
	}
}

// Canonical identity validation uses the existing dictionary without temporary names.
func TestIdentityValidationCost(t *testing.T) {
	d, err := v2dict.Open(t.TempDir(), v2dict.Options{BufferBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := &PolicyCatalog{}
	c.dict.Store(d)
	for _, name := range []string{"identity." + strings.Repeat("metric.", 45), `identity.\"escaped`, "identity.<html>", "identity.é"} {
		_, raw := rollupTestPolicy(t, name, "1s:1h,1m:1d", sealrollup.Last, .5)
		// A dictionary store admits names under their packed ID; the
		// neighbours share the name's dictionary block.
		h, err := c.admit(name, raw)
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 512; j++ {
			if _, _, err = d.Append(string(h.policy.Group), h.vshard, []byte(fmt.Sprintf("prefix.%04d", j)), 0); err != nil {
				t.Fatal(err)
			}
		}
		if err = d.Flush(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		allocs := testing.AllocsPerRun(2000, func() {
			got, err := c.intern(h.id, name, raw)
			if err != nil || got != h {
				panic("identity validation changed")
			}
		})
		if h.plain && allocs != 0 {
			t.Fatalf("plain identity validation allocated %.0f times", allocs)
		}
		if _, err := c.intern(h.id, name+"changed", raw); err == nil {
			t.Fatal("accepted changed supplied name")
		}
		t.Logf("IDENTITY_VALIDATION plain=%v ns/row=%d allocs/row=%.0f", h.plain, time.Since(start).Nanoseconds()/2001, allocs)

	}
}
