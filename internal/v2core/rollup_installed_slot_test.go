package v2core

import (
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// Shared hints are deliberately invalid: the installed fingerprint value must
// work for sparse IDs, survive renumbering and reject a full fingerprint alias.
func TestRollupInstalledFingerprintID(t *testing.T) {
	r := newCoreRollups()
	ids := []uint64{7, 1<<42 | 19, 1<<31 | 3}
	ms := make([]*PolicyIdentity, len(ids))
	for i, id := range ids {
		name := rankTestName(i)
		_, raw := rollupTestPolicy(t, name, "10s:3d,1m:14d", sealrollup.Last, .1)
		m, err := r.identities.intern(id, name, raw)
		if err != nil {
			t.Fatal(err)
		}
		ms[i] = m
	}
	// A 64-bit hash collision with a different route remains independently indexed.
	ms[1].nameHash = ms[0].nameHash
	ms[1].route = ms[0].route + 1
	for _, m := range ms[:2] {
		r.install(m.id, m, metricMetadata{Revision: m.id})
	}
	for round := 0; round < 2; round++ {
		if round == 1 {
			r.renumber()
		}
		adopter := newCoreRollups()
		adopter.identities = r.identities
		for i := 1; i >= 0; i-- {
			m := ms[i]
			adopter.install(m.id, m, metricMetadata{Revision: 42})
		}
		for _, m := range ms[:2] {
			m.ownerSlot.Store(0)
			if got := r.installed(m.id); got != m {
				t.Fatal("lost installed sparse identity")
			}
			if got := r.meta[r.slotOf(m.id, m)].Revision; got != m.id {
				t.Fatal("wrong owner metadata")
			}
			m.ownerSlot.Store(0)
			if got := adopter.meta[adopter.slotOf(m.id, m)].Revision; got != 42 {
				t.Fatal("shared hint aliased owner metadata")
			}
		}
	}
	// The catalog resolves a different numeric ID to the exact same fingerprint.
	ms[2].nameHash, ms[2].route = ms[0].nameHash, ms[0].route
	if r.installed(ms[2].id) != nil {
		t.Fatal("fingerprint alias accepted as installed numeric ID")
	}
	lookup := func(id uint64) *ApplicationPolicy { return r.identities.lookup(id) }
	var h pendingHead
	h.add([]coreBatch{{ID: ms[2].id, Registered: ms[2], Order: 1, Clock: 100, Points: []row{{ID: ms[2].id, Time: 99, Seq: 1}}}}, lookup)
	if !r.pendingMetadataDue(&h, lookup) {
		t.Fatal("pending intake skipped uninstalled colliding ID")
	}
	r.noteNewNames(&h, lookup)
	if len(h.fresh) != 1 || h.fresh[0].id != ms[2].id {
		t.Fatal("pending intake lost new colliding ID")
	}
	if plan, err := r.plan(coreSealInput{Batches: []coreBatch{{ID: ms[2].id, Registered: ms[2], Order: 1, Clock: 100, Points: []row{{ID: ms[2].id, Time: 99, Seq: 1}}}}}); err == nil {
		plan.Abort()
		t.Fatal("seal accepted duplicate installed fingerprint")
	}
	var installed pendingHead
	installed.add([]coreBatch{{ID: ms[0].id, Registered: ms[0], Order: 2, Clock: 100, Points: []row{{ID: ms[0].id, Time: 99, Seq: 2}}}}, lookup)
	if r.pendingMetadataDue(&installed, lookup) {
		t.Fatal("installed coarse name made metadata due")
	}
	r.noteNewNames(&installed, lookup)
	if len(installed.fresh) != 0 {
		t.Fatal("installed name entered pending new names")
	}
	// A corrupt/outdated slot must be rejected before indexing owner arrays.
	r.names.Put(ms[0].fingerprint(), uint32(len(r.ids)+100))
	if r.installed(ms[0].id) != nil {
		t.Fatal("out-of-bounds fingerprint slot accepted")
	}
}
