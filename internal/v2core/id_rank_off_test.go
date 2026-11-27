package v2core

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Split first-wave seals leave installed names alongside fresh ones. Once a
// name is published, poison its resolver: any rank comparison or reconstruction
// during the next ID-keyed plan panics. Real dictionary IDs still key the parts.
func TestIDKeyedSealNoNameRanks(t *testing.T) {
	r := newCoreRollups()
	r.identities.dict.Store(openTestDict(t))
	const wave = 600
	handles := make([]*PolicyIdentity, 2*wave)
	clock := int64(1801958400)
	r.now = func() time.Time { return time.Unix(clock, 0) }
	defer func() {
		for _, h := range handles {
			if h != nil {
				h.catalog = r.identities
			}
		}
	}()
	order := uint64(0)
	for seal := 0; seal < 4; seal++ {
		clock += 10
		count := wave
		if seal > 0 {
			count *= 2
		}
		var batches []coreBatch
		for i := 0; i < count; i++ {
			if handles[i] == nil {
				name := fmt.Sprintf("numeric.seal.%06d", len(handles)-i)
				_, raw := rollupTestPolicy(t, name, "10s:3d,1m:14d", sealrollup.Last, .1)
				h, err := r.identities.admit(name, raw)
				if err != nil {
					t.Fatal(err)
				}
				handles[i] = h
			}
			h := handles[i]
			// Clear shared hints and overwrite even the metric-local hint through an
			// adopter below. Neither fallback may consult a name.
			h.ownerSlot.Store(0)
			order++
			batches = append(batches, coreBatch{ID: h.id, Registered: h, Order: order, Clock: clock, Points: []row{{ID: h.id, Time: clock - 5, Bits: uint64(i), Seq: order}}})
		}
		plan, err := r.planParts(coreSealInput{Batches: batches})
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Parts) == 0 {
			t.Fatal("empty plan")
		}
		plan.Commit()
		if len(r.dense) != 0 || len(r.ids) != count {
			t.Fatalf("ID-keyed seal rebuilt ranks or retained dead slots: dense=%d slots=%d names=%d", len(r.dense), len(r.ids), count)
		}
		checkRankArray(t, r, seal)
		// A second planner temporarily shares the metrics in reverse order. Its
		// installation overwrites both hints, just as an interrupted move can.
		other := newCoreRollups()
		other.identities = r.identities
		for i := count - 1; i >= 0; i-- {
			id := handles[i].id
			m := r.installed(id)
			md := r.meta[r.slotOf(id, m)]
			rollupTestInstall(other, id, m, md)
		}
		for i := 0; i < count; i++ {
			h := handles[i]
			if h.name.Load() != nil {
				t.Fatal("published name retained")
			}
			h.catalog = nil // a name read now fails immediately
		}
	}
}

func TestRollupNumericSlotSharedHints(t *testing.T) {
	a, b := newCoreRollups(), newCoreRollups()
	const n = 300
	for i := 0; i < n; i++ {
		m := rankTestMetric(i)
		rollupTestInstall(a, m.id, m, metricMetadata{Revision: uint64(i + 7)})
	}
	for i := n - 1; i >= 0; i-- {
		id := uint64(i + 1)
		rollupTestInstall(b, id, a.installed(id), metricMetadata{Revision: id + 1000})
	}
	for _, id := range rollupTestIDs(a) {
		m := a.installed(id)
		m.name.Store(nil)
		m.catalog = nil
	}
	for round := 0; round < 3; round++ {
		for _, id := range rollupTestIDs(a) {
			m := a.installed(id)
			m.ownerSlot.Store(0)
			if got := a.meta[a.slotOf(id, m)].Revision; got != id+6 {
				t.Fatalf("donor ID %d revision %d", id, got)
			}
			m.ownerSlot.Store(0)
			if got := b.meta[b.slotOf(id, m)].Revision; got != id+1000 {
				t.Fatalf("adopter ID %d revision %d", id, got)
			}
		}
	}
}
