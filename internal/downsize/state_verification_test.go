package downsize

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestTypedVerificationProjection(t *testing.T) {
	s := newTypedState(t, 0)
	r := syncRow{Kind: "verified", Identity: "observation", Round: 7, First: "a", Last: "z", Names: 5, Time: "2026-11-16T19:00:00Z", MutationObserved: true, LegacyAdopted: 2, LegacyReverified: 3, LegacyObservation: true, LegacyObservationTime: "2026-11-15T19:00:00Z", LegacyRevalidated: 4, Reused: 1, ObservationProof: "batch-fence", InvalidationReason: "target_delete", Absent: []string{}, Unindexed: []string{"b"}, Short: []string{"c"}, LegacyTargetPresent: []string{"a", "d", "z"}, LegacyInvalidations: []string{"delete-1", "restart-2"}, Selected: []string{"a", "z"}, Skipped: []syncSkip{{"z", "source offline"}}, Resolved: []syncSkip{{"d", "expected_absent_target_expiration"}}, Holes: []syncHole{{Name: "a", Source: syncD1, Window: "gap", Step: 60, Points: 2, First: 600, Last: 660}, {Name: "a", Source: syncD2, Window: "history", Step: 3600, Points: 1, First: 3600, Last: 3600}}, LegacyExpiry: map[string]int64{"d": 100}, LegacyDonorProofs: map[string]string{"a": "donor-fence"}, HoleProofs: map[string]string{"a": "hole-fence"}, Work: []syncRedoWork{{Keys: []uint64{0, 1 << 63, math.MaxUint64}, IDs: []syncRedoRange{{0, 4096}, {8192, 8193}}, Reason: "destructive_mutation"}, {Keys: []uint64{}, IDs: nil}, {Keys: nil, IDs: []syncRedoRange{}}}}
	if err := s.update(func(tx *stateTx) error { return tx.appendVerification("root", r) }); err != nil {
		t.Fatal(err)
	}
	stateReadTx(t, s, func(tx *sql.Tx) {
		got, err := readStateVerification(tx, "root", 0)
		if err != nil || !reflect.DeepEqual(got, r) {
			t.Fatalf("lossy verification projection: err=%v\ngot=%+v\nwant=%+v", err, got, r)
		}
	})
	p := sqliteSyncVerification{s, "root"}
	for _, kind := range []string{"receipt-reverified", "ambiguous-receipt"} {
		r.Kind = kind
		if err := p.append(r); err != nil {
			t.Fatal(err)
		}
	}
	latest, found, err := p.latest(7, "a")
	if err != nil || !found || latest.Kind != "verified" || latest.Time != "2026-11-16T19:00:00Z" {
		t.Fatal("recovery plan replaced actual verification", latest, found, err)
	}
	if _, found, err := p.latest(8, "a"); err != nil || found {
		t.Fatal("receipt escaped its round", found, err)
	}
	before := typedDigests(t, s)
	if err := s.update(func(tx *stateTx) error {
		if err := tx.appendVerification("root", r); err != nil {
			return err
		}
		if err := tx.reduceProgress("root", r); err != nil {
			return err
		}
		return context.Canceled
	}); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("failed verification transaction leaked state", err)
	}
	r.Discovery = &syncDiscoveryCheckpoint{}
	if err := p.append(r); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("unmapped verification semantics accepted", err)
	}
	if _, err := s.db.Exec("UPDATE verification_hole SET first=first+1 WHERE scope='root' AND receipt=0 AND ordinal=0"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.latest(7, "a"); err == nil {
		t.Fatal("corrupted hole evidence reused")
	}
}

type verificationCutPersistence struct {
	syncVerificationPersistence
	cancel context.CancelFunc
	once   sync.Once
}

func (p *verificationCutPersistence) append(r syncRow) error {
	if err := p.syncVerificationPersistence.append(r); err != nil {
		return err
	}
	p.once.Do(p.cancel)
	return nil
}

func TestSyncVerificationPersistenceDifferential(t *testing.T) {
	type outcome struct {
		Holes     []syncHole
		Unindexed []string
		Skipped   []syncSkip
		Receipts  []syncRow
		Progress  progressSnapshot
		Calls     map[redoCall]int
	}
	outcomes := map[string]outcome{}
	for _, backend := range []string{"legacy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			b, o := newRedoHarness(t)
			// This test covers the verification controller, independent of the
			// frontier file adapter which remains a separate migration gate.
			b.put(syncT, "metric.05", []syncArchive{{3600, 240}})
			o.batch, o.streams = 2, 1
			o.state = t.TempDir()
			identity, _ := syncIdentity(o)
			open := func() (*syncJournal, *syncer) {
				j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
				if err != nil {
					t.Fatal(err)
				}
				s := &syncer{o: o, b: struct{ syncBackend }{b}, j: j, forbid: map[string]bool{}, copied: map[string]bool{}, sum: syncSummary{phases: map[string]*syncPhase{}}}
				s.redo = newSyncRedoGuard(j)
				return j, s
			}
			j, s := open()
			if err := s.discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			var store *stateStore
			var persistence syncVerificationPersistence
			if backend == "sqlite" {
				store = newTypedState(t, 0)
				persistence = sqliteSyncVerification{store, "root"}
			} else {
				persistence = &legacySyncVerification{j: j}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.verificationState = &verificationCutPersistence{syncVerificationPersistence: persistence, cancel: cancel}
			if _, _, _, err := s.verifyRound(ctx, 1, s.unionSource()); !errors.Is(err, context.Canceled) {
				t.Fatal("missing mid-round interruption", err)
			}
			before := b.snapshot()
			// A successful first batch must exist before the interruption.
			var partial []syncRow
			if err := persistence.scan(1, func(r syncRow) error { partial = append(partial, r); return nil }); err != nil {
				t.Fatal(err)
			}
			if len(partial) != 1 || partial[0].Names != 2 {
				t.Fatal("fixture did not commit exactly one batch", partial)
			}
			j.close()
			if store != nil {
				if err := store.close(); err != nil {
					t.Fatal(err)
				}
				var err error
				store, err = openStateStore(store.path, store.unit, false)
				if err != nil {
					t.Fatal(err)
				}
				defer store.close()
			}
			j, s = open()
			defer j.close()
			if store != nil {
				s.verificationState = sqliteSyncVerification{store, "root"}
			}
			if err := s.discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			holes, unindexed, skipped, err := s.verifyRound(context.Background(), 1, s.unionSource())
			if err != nil {
				t.Fatal(err)
			}
			assertNoRedoCalls(t, before, b.snapshot())
			rows, err := s.readVerificationRound(1)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 3 {
				t.Fatal("verification receipt count", len(rows))
			}
			out := outcome{Holes: holes, Unindexed: unindexed, Skipped: skipped, Receipts: rows, Calls: b.snapshot()}
			for i := range out.Receipts {
				out.Receipts[i].Identity = ""
				out.Receipts[i].Time = ""
			}
			if store == nil {
				for _, r := range rows {
					out.Progress.apply(r)
				}
			} else {
				stateReadTx(t, store, func(tx *sql.Tx) {
					var err error
					out.Progress, err = readStateProgress(tx, "root")
					if err != nil {
						t.Fatal(err)
					}
				})
				if rows := j.find(func(r syncRow) bool {
					return r.Kind == "verified" || r.Kind == "receipt-reverified" || r.Kind == "ambiguous-receipt"
				}); len(rows) != 0 {
					t.Fatal("SQLite verification dual-wrote legacy receipts")
				}
			}
			if out.Progress.Verified != 6 {
				t.Fatal("verification counter lost or duplicated", out.Progress)
			}
			for _, n := range out.Progress.RedoNames {
				if n != 0 {
					t.Fatal("verification redo", out.Progress)
				}
			}
			// Terminal replay uses the same owner and makes no data calls.
			s.resume = syncPhasePosition{Phase: "fill", Round: 1}
			terminalBefore := b.snapshot()
			if _, _, _, err := s.verifyRound(context.Background(), 1, s.unionSource()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(terminalBefore, b.snapshot()) {
				t.Fatal("completed verification repeated data work")
			}
			outcomes[backend] = out
		})
	}
	if !reflect.DeepEqual(outcomes["legacy"], outcomes["sqlite"]) {
		t.Fatalf("verification differential mismatch:\nlegacy=%+v\nsqlite=%+v", outcomes["legacy"], outcomes["sqlite"])
	}
}

func TestTypedVerificationEmptyCollections(t *testing.T) {
	s := newTypedState(t, 0)
	for i, empty := range []bool{false, true} {
		r := syncRow{Kind: "verified", Round: 1, First: fmt.Sprint(i), Names: 1}
		if empty {
			r.Absent = []string{}
			r.Unindexed = []string{}
			r.Short = []string{}
			r.LegacyTargetPresent = []string{}
			r.LegacyInvalidations = []string{}
			r.Selected = []string{}
			r.Skipped = []syncSkip{}
			r.Resolved = []syncSkip{}
			r.Holes = []syncHole{}
			r.LegacyExpiry = map[string]int64{}
			r.LegacyDonorProofs = map[string]string{}
			r.HoleProofs = map[string]string{}
			r.Work = []syncRedoWork{}
		}
		if err := s.update(func(tx *stateTx) error { return tx.appendVerification("root", r) }); err != nil {
			t.Fatal(err)
		}
		stateReadTx(t, s, func(tx *sql.Tx) {
			got, err := readStateVerification(tx, "root", int64(i))
			if err != nil || !reflect.DeepEqual(got, r) {
				t.Fatal("nil/empty collections changed", empty, err, got)
			}
		})
	}
}

func TestSyncVerificationObservationLookupDifferential(t *testing.T) {
	j, s := checkpointFixture(t)
	defer j.close()
	store := newTypedState(t, 0)
	rows := []syncRow{
		{Kind: "verified", Round: 1, Identity: "older", Time: "old", Holes: []syncHole{{Name: "a", Source: syncD1}, {Name: "b", Source: syncD1}}},
		{Kind: "receipt-reverified", Round: 1, Identity: "newer", Time: "new", LegacyObservationTime: "original", Holes: []syncHole{{Name: "a", Source: syncD1}}},
		{Kind: "verified", Round: 1, Identity: "peer", Time: "peer", Holes: []syncHole{{Name: "a", Source: syncD2}}},
		{Kind: "verified", Round: 2, Identity: "next-round", Time: "next", Holes: []syncHole{{Name: "b", Source: syncD1}}},
		{Kind: "verified", Round: 1, Identity: "clean", Time: "clean"},
		{Kind: "ambiguous-receipt", Round: 1, Identity: "plan", Time: "plan", Holes: []syncHole{{Name: "a", Source: syncD1}}},
	}
	// Use fixed durable times in both independent fixtures; journal.append would
	// assign a fresh timestamp, which is the runtime adapter's responsibility.
	j.rows = append(j.rows, rows...)
	if err := store.update(func(tx *stateTx) error {
		for _, r := range rows {
			if err := tx.appendVerification("root", r); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wanted := map[string]string{"a": "newer/original", "b": "older/old", "missing": ""}
	for _, p := range []syncVerificationPersistence{&legacySyncVerification{j: s.j}, sqliteSyncVerification{store, "root"}} {
		got, err := p.observations(1, syncD1, []string{"a", "b", "missing", "a"})
		if err != nil || !reflect.DeepEqual(got, wanted) {
			t.Fatal(got, err)
		}
	}
	if _, err := store.db.Exec("UPDATE verification_receipt SET identity='corrupt' WHERE ordinal=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := (sqliteSyncVerification{store, "root"}).observations(1, syncD1, []string{"a"}); err == nil {
		t.Fatal("corrupt selected observation reused")
	}
}
