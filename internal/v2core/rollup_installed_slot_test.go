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
	ms[0].ownerSlot.Store(0)
	r.names.Put(ms[0].fingerprint(), uint32(len(r.ids)+100))
	if r.installed(ms[0].id) != nil {
		t.Fatal("out-of-bounds fingerprint slot accepted")
	}
}

// Another owner can overwrite a shared hint with an in-bounds slot occupied by
// a different ID here. Installed lookup must then fall back to this owner's table.
func TestRollupInstalledOverwrittenHint(t *testing.T) {
	r := newCoreRollups()
	adopter := newCoreRollups()
	adopter.identities = r.identities
	ms := make([]*PolicyIdentity, 2)
	for i := range ms {
		id := uint64(i + 7)
		name := rankTestName(i)
		_, raw := rollupTestPolicy(t, name, "10s:3d,1m:14d", sealrollup.Last, .1)
		m, err := r.identities.intern(id, name, raw)
		if err != nil {
			t.Fatal(err)
		}
		ms[i] = m
		r.install(id, m, metricMetadata{Revision: id})
	}
	// Reverse installation order publishes valid hints for the adopter which
	// point to the wrong numeric IDs in the donor.
	for i := len(ms) - 1; i >= 0; i-- {
		adopter.install(ms[i].id, ms[i], metricMetadata{Revision: 42})
	}
	for i, m := range ms {
		if m.ownerSlot.Load() != uint32(2-i) {
			t.Fatal("test did not overwrite donor hint")
		}
		if got := r.installed(m.id); got != m {
			t.Fatal("overwritten hint did not fall back to installed fingerprint slot")
		}
		// A missing and an out-of-bounds hint require the same fallback.
		for _, hint := range []uint32{0, uint32(len(r.ids) + 10)} {
			m.ownerSlot.Store(hint)
			if got := r.installed(m.id); got != ms[i] {
				t.Fatal("missing or out-of-bounds hint did not fall back")
			}
		}
	}
}

// Publication can classify a row as fresh while the previous head's background
// seal is still running. Its commit must clear metadata demand without changing
// the immutable new-name list readers already hold.
func TestPendingClassificationAcrossInstall(t *testing.T) {
	r := newCoreRollups()
	lookup := func(id uint64) *ApplicationPolicy { return r.identities.lookup(id) }
	var h pendingHead
	for _, id := range []uint64{7, 1<<42 | 19} {
		name := rankTestName(int(id & 31))
		_, raw := rollupTestPolicy(t, name, "10s:3d,1m:14d", sealrollup.Last, .1)
		m, err := r.identities.intern(id, name, raw)
		if err != nil {
			t.Fatal(err)
		}
		h.add([]coreBatch{{ID: id, Registered: m, Order: id, Clock: 100, Points: []row{{ID: id, Time: 99, Seq: id}}}}, lookup)
	}
	r.noteNewNames(&h, lookup)
	published := h.view(lookup)
	if len(published.fresh) != 2 || !r.pendingMetadataDue(&h, lookup) {
		t.Fatal("new rows were not classified before metadata scan")
	}
	for _, f := range h.fresh {
		r.install(f.id, lookup(f.id), metricMetadata{})
	}
	r.renumber()
	if r.pendingMetadataDue(&h, lookup) {
		t.Fatal("installed fresh name still requested metadata seal")
	}
	if len(published.fresh) != 2 || h.scanned != h.n {
		t.Fatal("classification changed published names or lost progress")
	}
	// An unpublished cold application is independent of the row new-name list.
	h.add([]coreBatch{{ID: 99, Policy: []byte("cold"), Order: 100, Points: []row{{ID: 99}}}}, lookup)
	if !r.pendingMetadataDue(&h, lookup) {
		t.Fatal("cold application lost metadata demand")
	}
}

// Once this head classified an installed row ID, later publications and run
// compactions must recognize it from pending progress without the catalog.
func TestPendingClassificationOncePerHead(t *testing.T) {
	r := newCoreRollups()
	_, raw := rollupTestPolicy(t, rankTestName(7), "10s:3d,1m:14d", sealrollup.Last, .1)
	m, err := r.identities.intern(7, rankTestName(7), raw)
	if err != nil {
		t.Fatal(err)
	}
	r.install(7, m, metricMetadata{})
	var h pendingHead
	add := func(order uint64) {
		var rows pendingRows
		rows.startApplication(order, 100, 99, 2)
		rows.addRow(7, 99, 1, order)
		rows.addRow(7, 99, 2, order)
		rows.closePacket()
		h.addRows(&rows)
	}
	add(1)
	r.noteNewNames(&h, r.identities.lookup)
	pages := r.identities.pages.Swap(nil)
	defer r.identities.pages.Store(pages)
	unavailable := func(uint64) *ApplicationPolicy { t.Fatal("classified row revisited catalog"); return nil }
	for order := uint64(2); order < 5; order++ {
		add(order)
		r.noteNewNames(&h, unavailable)
		if r.pendingMetadataDue(&h, unavailable) || len(h.fresh) != 0 {
			t.Fatal("installed row was classified fresh")
		}
		var c pendingCompaction
		c.start(&h)
		c.wait(&h)
	}
}
