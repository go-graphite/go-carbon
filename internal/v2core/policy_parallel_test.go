package v2core

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Keep 96 concurrent admission owners even on the bounded laptop CPU budget.
func BenchmarkPolicyCatalog96Owners(b *testing.B) {
	const owners, perOwner = 96, 512
	raws := make([][][]byte, owners)
	for n, left := 0, owners*perOwner; left > 0; n++ {
		name := v2part.ID(fmt.Sprintf("catalog.owner.new.metric.%d", n))
		owner := int(v2part.Route(name)) % owners
		if len(raws[owner]) == perOwner {
			continue
		}
		raw, err := EncodeRollupPolicy(RollupPolicy{Name: name, Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 10, Count: 8640}}}})
		if err != nil {
			b.Fatal(err)
		}
		raws[owner] = append(raws[owner], raw)
		left--
	}
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for range b.N {
		c := &PolicyCatalog{}
		var wg sync.WaitGroup
		for owner := range owners {
			wg.Go(func() {
				for i, raw := range raws[owner] {
					if _, err := c.intern(uint64(i*owners+owner+1), "", raw); err != nil {
						b.Error(err)
						return
					}
				}
			})
		}
		wg.Wait()
	}
	b.ReportMetric(float64(time.Since(start).Nanoseconds())/float64(b.N*owners*perOwner), "ns/name")
}

func TestPolicyCatalogShardIsolation(t *testing.T) {
	c := &PolicyCatalog{}
	p := RollupPolicy{Name: "catalog.isolated.one", Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 100}}}}
	raw, err := EncodeRollupPolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.intern(1, string(p.Name), raw)
	if err != nil {
		t.Fatal(err)
	}
	shard := c.shards[h.vshard].Load()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	// Existing lookups and other virtual shards must not wait on this writer.
	for v2part.Route(p.Name) == h.vshard {
		p.Name += "x"
	}
	raw2, err := EncodeRollupPolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		if c.lookup(1) != h {
			done <- fmt.Errorf("lost published identity")
			return
		}
		_, err := c.intern(2, string(p.Name), raw2)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("unrelated virtual shard blocked")
	}
}

func TestPolicyCatalogConcurrentConflictingID(t *testing.T) {
	c := &PolicyCatalog{}
	_, a := rollupTestPolicy(t, "conflict.first", "1s:1h", sealrollup.Last, .5)
	_, z := rollupTestPolicy(t, "conflict.second", "1s:1h", sealrollup.Last, .5)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, raw := range [][]byte{a, z} {
		go func() { <-start; _, err := c.intern(7, "", raw); results <- err }()
	}
	close(start)
	first, second := <-results, <-results
	if (first == nil) == (second == nil) {
		t.Fatal("conflicting ID must have exactly one winner", first, second)
	}
	h := c.lookup(7)
	if h == nil {
		t.Fatal("missing winner")
	}
	got, err := c.intern(7, string(h.metricName()), h.definitionBytes())
	if err != nil || got != h {
		t.Fatal("published identity changed", err)
	}
}
