package downsize

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type fillPersistenceCut struct {
	*fakeSync
	cancel context.CancelFunc
}

func (b *fillPersistenceCut) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	if err := ctx.Err(); err != nil {
		return syncCopyResult{}, err
	}
	r, err := b.fakeSync.Copy(ctx, source, target, names, journal)
	if err == nil && b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	return r, err
}

func TestSyncFillPersistenceDifferential(t *testing.T) {
	// Freeze the actual discovery and first verification results once. Both
	// executions see precisely the same observation generations and identities.
	fixture := func() *fillPersistenceCut {
		b := &fillPersistenceCut{fakeSync: newFakeSync()}
		for i, source := range []string{syncD1, syncD2} {
			name := []string{"a", "b"}[i]
			b.put(source, name, []syncArchive{{3600, 240}})
			b.put(syncT, name, []syncArchive{{3600, 240}}, syncTestNow-5*3600)
		}
		return b
	}
	o := convergeOptions(t)
	o.from, o.streams = []string{syncD1, syncD2}, 1
	identity, err := syncIdentity(o)
	if err != nil {
		t.Fatal(err)
	}
	open := func(o syncOptions, b syncBackend) (*syncJournal, *syncer) {
		t.Helper()
		j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
		if err != nil {
			t.Fatal(err)
		}
		s := &syncer{o: o, b: b, j: j, copied: map[string]bool{}, forbid: map[string]bool{}, sum: syncSummary{phases: map[string]*syncPhase{}}}
		s.redo = newSyncRedoGuard(j)
		return j, s
	}
	j, s := open(o, fixture())
	if err := s.discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.verifyRound(context.Background(), 1, s.unionSource()); err != nil {
		t.Fatal(err)
	}
	j.close()
	frozen := map[string][]byte{}
	for _, name := range []string{"sync.jsonl", "union.tsv", "missing.tsv"} {
		frozen[name], err = os.ReadFile(filepath.Join(o.state, name))
		if err != nil {
			t.Fatal(err)
		}
	}
	type outcome struct {
		Receipts []syncRow
		Progress progressSnapshot
		Copies   []string
		Filled   int
		Residual []syncResidual
	}
	outcomes := map[string]outcome{}
	for _, backend := range []string{"legacy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			o.state = t.TempDir()
			for name, data := range frozen {
				if err := os.WriteFile(filepath.Join(o.state, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			b := fixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.cancel = cancel
			j, s := open(o, b)
			var store *stateStore
			if backend == "sqlite" {
				store = newTypedState(t, 0)
				s.fillState = sqliteSyncFill{store, "root"}
			}
			if err := s.discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyAndFill(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("missing mid-fill interruption", err)
			}
			if len(b.copies) != 1 {
				t.Fatal("fixture did not stop after one committed donor", b.copies)
			}
			j.close()
			if store != nil {
				if err := store.close(); err != nil {
					t.Fatal(err)
				}
				store, err = openStateStore(store.path, store.unit, false)
				if err != nil {
					t.Fatal(err)
				}
				defer store.close()
			}
			// Reconstruct the controller too: an in-memory receipt cache cannot
			// accidentally make this restart test pass.
			j, s = open(o, b)
			defer j.close()
			if store != nil {
				s.fillState = sqliteSyncFill{store, "root"}
			}
			if err := s.discover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyAndFill(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(b.copies) != 2 || b.copies[0] != syncD1+" a" || b.copies[1] != syncD2+" b" {
				t.Fatal("completed fill was repeated or pending fill was lost", b.copies)
			}
			var out outcome
			out.Copies, out.Filled, out.Residual = b.copies, s.sum.Filled, s.sum.residual
			if store == nil {
				out.Receipts = j.find(func(r syncRow) bool { return r.Kind == "copied" && r.Phase == "fill" })
				for _, r := range out.Receipts {
					out.Progress.apply(r)
				}
			} else {
				stateReadTx(t, store, func(tx *sql.Tx) {
					for i := range 2 {
						r, err := readStateFillReceipt(tx, "root", int64(i))
						if err != nil {
							t.Fatal(err)
						}
						out.Receipts = append(out.Receipts, r)
					}
					out.Progress, err = readStateProgress(tx, "root")
					if err != nil {
						t.Fatal(err)
					}
				})
				if rows := j.find(func(r syncRow) bool { return r.Kind == "copied" && r.Phase == "fill" }); len(rows) != 0 {
					t.Fatal("SQLite fill dual-wrote legacy receipts")
				}
			}
			for i := range out.Receipts {
				out.Receipts[i].Time = "" // timestamps are observation metadata
			}
			if out.Progress.Filled != 2 {
				t.Fatal("durable progress lost or double counted", out.Progress)
			}
			for _, n := range out.Progress.RedoNames {
				if n != 0 {
					t.Fatal("redo counter advanced", out.Progress)
				}
			}
			outcomes[backend] = out
		})
	}
	if !reflect.DeepEqual(outcomes["legacy"], outcomes["sqlite"]) {
		t.Fatalf("fill persistence differs:\nlegacy=%+v\nsqlite=%+v", outcomes["legacy"], outcomes["sqlite"])
	}
}

func TestTypedFillReceiptProjectionAndLookup(t *testing.T) {
	store := newTypedState(t, 0)
	p := sqliteSyncFill{store, "root"}
	r := syncRow{Kind: "copied", Phase: "fill", Identity: "observation", Round: 4, Source: "source", First: "a", Last: "z", Names: 4, Copied: 1, Forbidden: true, SelectionComplete: true, InvalidationReason: "verified_holes", Failed: []string{"z", "z"}, Short: []string{}, Selected: []string{"a", "b", "c", "z"}, Resolved: []syncSkip{{"b", "expected_absent_target_expiration"}, {"c", "quarantined_or_policy_skipped"}}}
	if err := p.append(r); err != nil {
		t.Fatal(err)
	}
	if _, found, err := p.lookup(4, "source", "observation", false); err != nil || found {
		t.Fatal("forbidden receipt suppressed a live retry", found, err)
	}
	got, found, err := p.lookup(4, "source", "observation", true)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	r.Time = got.Time
	if !reflect.DeepEqual(got, r) {
		t.Fatalf("lossy projection: got=%+v want=%+v", got, r)
	}
	r.Forbidden = false
	if err := p.append(r); err != nil {
		t.Fatal(err)
	}
	got, found, err = p.lookup(4, "source", "observation", false)
	if err != nil || !found || got.Forbidden {
		t.Fatal("successful retry was not selected", got, found, err)
	}
	got, found, err = p.lookup(4, "source", "observation", true)
	if err != nil || !found || !got.Forbidden {
		t.Fatal("completed phase did not preserve first eligible receipt", got, found, err)
	}
	for _, test := range []struct {
		round            int
		source, identity string
	}{{5, "source", "observation"}, {4, "other", "observation"}, {4, "source", "changed-observation"}} {
		if _, found, err := p.lookup(test.round, test.source, test.identity, true); err != nil || found {
			t.Fatal("receipt escaped its identity", test, found, err)
		}
	}
	before := typedDigests(t, store)
	if err := store.update(func(tx *stateTx) error {
		if err := tx.appendFillReceipt("root", r); err != nil {
			return err
		}
		if err := tx.reduceProgress("root", r); err != nil {
			return err
		}
		return context.Canceled
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, store)) {
		t.Fatal("failed receipt transaction left counters or memberships")
	}
	r.Holes = []syncHole{{Name: "unmapped"}}
	if err := p.append(r); err == nil || !reflect.DeepEqual(before, typedDigests(t, store)) {
		t.Fatal("unmapped journal semantics accepted", err)
	}
	if _, err := store.db.Exec("UPDATE fill_receipt_name SET name='corrupt' WHERE scope='root' AND receipt=0 AND kind=0 AND ordinal=0"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.lookup(4, "source", "observation", true); err == nil {
		t.Fatal("corrupted receipt reused")
	}
}

func TestSyncFillMissingCompletedReceiptRefusesReplay(t *testing.T) {
	for _, backend := range []string{"legacy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			j, s := checkpointFixture(t)
			defer j.close()
			s.resume = syncPhasePosition{Phase: "fill", Round: 1, Complete: true}
			if backend == "sqlite" {
				s.fillState = sqliteSyncFill{newTypedState(t, 0), "root"}
			}
			if _, _, err := s.fillFrom(context.Background(), 1, 0, []string{"a"}); err == nil {
				t.Fatal("completed phase was allowed to repeat backend work")
			}
		})
	}
}
