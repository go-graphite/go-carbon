package downsize

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func newTypedState(t *testing.T, count int64) *stateStore {
	t.Helper()
	s, err := createStateStore(filepath.Join(t.TempDir(), "state.sqlite.adopting"), stateUnit{"identity", "options", "catalog-v1", count})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.close(); err != nil {
			t.Error(err)
		}
	})
	err = s.update(func(tx *stateTx) error {
		if err := tx.beginScope("root", "", "original execution scope"); err != nil {
			return err
		}
		return tx.putPhase(statePhase{Scope: "root", Phase: "window retry", Total: count})
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func typedPhase(t *testing.T, s *stateStore) statePhase {
	t.Helper()
	p, err := readStatePhase(s.db, "root")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func typedDigests(t *testing.T, s *stateStore) map[string]stateRelationDigest {
	t.Helper()
	d, err := s.relationDigests()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestTypedStateAtomicCompletions(t *testing.T) {
	s := newTypedState(t, 8193)
	k := stateMemberKey{Scope: "root", Phase: "window retry", Holder: "source", Window: "gap", Kind: "done"}
	op := stateOperation{ID: "native-identity", Scope: k.Scope, Window: k.Window, Holder: k.Holder, Plan: "plan", Stream: "stream", End: 2, Status: "intent", Selection: []byte("original selection")}
	err := s.update(func(tx *stateTx) error {
		pending := k
		pending.Kind = "pending"
		if _, err := tx.setMembers(pending, []int64{1, 2, 3, 4097, 8193}, true); err != nil {
			return err
		}
		return tx.putOperation(op)
	})
	if err != nil {
		t.Fatal(err)
	}
	op.Status, op.Receipt = "complete", []byte("authenticated receipt bytes")
	added, err := s.completeMembers(k, "batch-a", []int64{2, 1, 2}, &op)
	if err != nil || added != 2 || typedPhase(t, s).Done != 2 {
		t.Fatalf("first completion: added=%d err=%v", added, err)
	}
	before := typedDigests(t, s)
	added, err = s.completeMembers(k, "batch-a", []int64{1, 2}, &op)
	if err != nil || added != 0 || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatalf("replay changed durable outcome: %d %v", added, err)
	}
	if _, err := s.completeMembers(k, "batch-a", []int64{1, 3}, &op); err == nil {
		t.Fatal("changed token membership accepted")
	}
	added, err = s.completeMembers(k, "batch-b", []int64{2, 3, 4097, 8193}, nil)
	if err != nil || added != 3 || typedPhase(t, s).Done != 5 {
		t.Fatalf("overlap: %d %v", added, err)
	}
	var pending int
	if err := s.db.QueryRow("SELECT count(*) FROM membership_page WHERE kind='pending'").Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("pending frontier not retired: %d %v", pending, err)
	}
	before = typedDigests(t, s)
	bad := op
	bad.Receipt = []byte("changed receipt")
	if _, err = s.completeMembers(k, "batch-c", []int64{4}, &bad); err == nil {
		t.Fatal("changed native acknowledgement accepted")
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("failed receipt left partial membership/counter/token")
	}
	// The same names and token in the next phase are different work. A stale
	// previous-phase token replay is still a no-op rather than a counter write.
	if err := s.update(func(tx *stateTx) error {
		return tx.putPhase(statePhase{Scope: "root", Phase: "verify", Round: 1, Total: 8193})
	}); err != nil {
		t.Fatal(err)
	}
	k.Phase, k.Round = "verify", 1
	if n, e := s.completeMembers(k, "batch-a", []int64{1, 2}, nil); e != nil || n != 2 || typedPhase(t, s).Done != 2 {
		t.Fatalf("phase membership collided: %d %v", n, e)
	}
}

func TestTypedStateRegressionRollbackAndScopes(t *testing.T) {
	s := newTypedState(t, 10)
	k := stateMemberKey{Scope: "root", Phase: "window retry", Kind: "done"}
	if _, err := s.completeMembers(k, "batch", []int64{1, 2}, nil); err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	for _, next := range []statePhase{
		{Scope: "root", Phase: "copy", Done: 2},
		{Scope: "root", Phase: "window retry", Done: 1},
		{Scope: "root", Phase: "window retry", Done: -1, Total: 10},
		{Scope: "root", Phase: "unknown"},
	} {
		err := s.update(func(tx *stateTx) error { return tx.putPhase(next) })
		if err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
			t.Fatalf("accepted or partly committed regression %+v: %v", next, err)
		}
	}
	if err := s.update(func(tx *stateTx) error {
		if err := tx.beginScope("restart", "root", "known process restart window"); err != nil {
			return err
		}
		return tx.putPhase(statePhase{Scope: "restart", Phase: "copy", Total: 1})
	}); err != nil {
		t.Fatal(err)
	}
	if typedPhase(t, s).Done != 2 {
		t.Fatal("new scope changed historical floor")
	}
}

func TestTypedStateTerminalPositionsAreImmutable(t *testing.T) {
	s := newTypedState(t, 2)
	ready := stateReady{Scope: "root", ProofScope: "proof", Report: readyReport{ScopeKey: "scope", Verdict: "READY"}}
	complete := statePhase{Scope: "root", Phase: "window retry", Done: 2, Total: 2, Complete: true}
	if err := s.update(func(tx *stateTx) error {
		if err := tx.putPhase(complete); err != nil {
			return err
		}
		return tx.putReady(ready)
	}); err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	if err := s.update(func(tx *stateTx) error { return tx.putReady(ready) }); err != nil {
		t.Fatalf("exact READY replay: %v", err)
	}
	for _, changed := range []statePhase{
		{Scope: "root", Phase: "window retry", Done: 2, Total: 3, Complete: true},
		{Scope: "root", Phase: "window retry", Done: 2, Total: 2},
	} {
		if err := s.update(func(tx *stateTx) error { return tx.putPhase(changed) }); err == nil {
			t.Fatal("completed phase was rewritten")
		}
	}
	ready.Report.Verdict = "NOT_READY"
	if err := s.update(func(tx *stateTx) error { return tx.putReady(ready) }); err == nil {
		t.Fatal("terminal READY was replaced without a recovery scope")
	}
	if _, err := s.completeMembers(stateMemberKey{Scope: "root", Phase: "window retry", Kind: "done"}, "late", []int64{1}, nil); err == nil {
		t.Fatal("new completion changed a completed phase")
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("rejected terminal transition changed durable state")
	}
}

func TestTypedStateAllRelationsReopenAndLock(t *testing.T) {
	s := newTypedState(t, 2)
	err := s.update(func(tx *stateTx) error {
		if err := tx.putCatalog(1, []syncName{{Name: "a", Sources: 1 << 62, OnTarget: true}, {Name: "b", Sources: 3}}); err != nil {
			return err
		}
		if err := tx.putWindow(stateWindow{"window", "root", "source", 100, 200, "selected", "issued", "pending"}); err != nil {
			return err
		}
		if err := tx.putIssue(stateIssue{Scope: "root", Kind: "quarantine", NameID: 2, Reason: "source absent", Phase: "verify", FirstSeen: 1, LastSeen: 2}); err != nil {
			return err
		}
		proof := make([]byte, 64)
		copy(proof, "exact fence and integrity seal")
		if err := tx.putProof(stateProofPage{Scope: "root", Generation: "mutation-scope", FirstID: 1, Records: proof}); err != nil {
			return err
		}
		return tx.putReady(stateReady{Scope: "root", ProofScope: "original proof", Report: readyReport{ScopeKey: "scope", Verdict: "READY_EXCEPT", Quarantined: 1}})
	})
	if err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	if _, err := openStateStore(s.path, s.unit, false); err == nil {
		t.Fatal("second unit writer acquired the legacy lock")
	}
	if _, err := openSyncJournal(filepath.Join(filepath.Dir(s.path), "sync.jsonl"), "identity"); err == nil {
		t.Fatal("legacy journal writer bypassed indexed state unit lock")
	}
	ro, err := openStateStore(s.path, s.unit, true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, ro)) {
		t.Fatal("reader saw different relations")
	}
	if err := ro.update(func(tx *stateTx) error { return tx.beginScope("unexpected", "root", "read-only") }); err == nil {
		t.Fatal("read-only inspection wrote state")
	}
	if err := ro.close(); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	if !reflect.DeepEqual(before, typedDigests(t, reopened)) {
		t.Fatal("typed relation count/hash changed on reopen")
	}
	name, err := reopened.catalogName(1)
	if err != nil || name.Name != "a" || name.Sources != 1<<62 || !name.OnTarget {
		t.Fatalf("catalog: %+v %v", name, err)
	}
	if err := reopened.update(func(tx *stateTx) error {
		return tx.putWindow(stateWindow{"window", "root", "source", 100, 201, "selected", "issued", "pending"})
	}); err == nil {
		t.Fatal("changed window range accepted")
	}
}

func TestTypedStateCatalogCodecAndCorruption(t *testing.T) {
	names := make([]syncName, statePageNames)
	for i := range names {
		names[i] = syncName{Name: fmt.Sprintf("some.long.shared.prefix.%06d", i), Sources: uint64(i) | 1<<62, OnTarget: i%2 == 0}
	}
	b, err := encodeStateCatalog(names)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeStateCatalog(b)
	if err != nil || !reflect.DeepEqual(names, got) {
		t.Fatalf("round trip: %v", err)
	}
	for _, data := range [][]byte{nil, {2}, b[:len(b)-1], append(bytes.Clone(b), 0), {1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1}} {
		if _, err := decodeStateCatalog(data); err == nil {
			t.Fatal("torn/unknown/trailing catalog accepted")
		}
	}
	s := newTypedState(t, statePageNames)
	if err := s.update(func(tx *stateTx) error { return tx.putCatalog(1, names) }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE catalog_page SET data=?", []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.catalogName(1); err == nil {
		t.Fatal("persisted corrupt dictionary accepted")
	}
}

func TestTypedStateUnknownFormatUntouched(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.sqlite")
	db, err := connectStateDB(path, false)
	if err == nil {
		db.Close()
		t.Fatal("connect created unreserved database")
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	db, err = connectStateDB(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA user_version=99; CREATE TABLE future(value TEXT); INSERT INTO future VALUES('preserve')"); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if _, err := openStateStore(path, stateUnit{}, true); err == nil {
		t.Fatal("accepted future format")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("modified future database")
	}
}

func TestTypedStateCrashBoundary(t *testing.T) {
	if mode := os.Getenv("TYPED_STATE_CRASH"); mode != "" {
		s, err := openStateStore(os.Getenv("TYPED_STATE_PATH"), stateUnit{"identity", "options", "catalog-v1", 10}, false)
		if err != nil {
			t.Fatal(err)
		}
		err = s.update(func(tx *stateTx) error {
			k := stateMemberKey{Scope: "root", Phase: "window retry", Kind: "done"}
			if _, err := tx.setMembers(k, []int64{1, 2, 3}, true); err != nil {
				return err
			}
			if err := tx.putPhase(statePhase{Scope: "root", Phase: "window retry", Done: 3, Total: 10}); err != nil {
				return err
			}
			if mode == "before" {
				os.Exit(76)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(77) // acknowledged commit, no graceful checkpoint or close
	}
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			s := newTypedState(t, 10)
			before := typedDigests(t, s)
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestTypedStateCrashBoundary$")
			cmd.Env = append(os.Environ(), "TYPED_STATE_CRASH="+mode, "TYPED_STATE_PATH="+s.path)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != map[string]int{"before": 76, "after": 77}[mode] {
				t.Fatalf("child did not reach cut: %v %s", err, out)
			}
			reopened, err := openStateStore(s.path, s.unit, false)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.close()
			var check string
			if err := reopened.db.QueryRow("PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
				t.Fatalf("integrity: %s %v", check, err)
			}
			if mode == "before" && !reflect.DeepEqual(before, typedDigests(t, reopened)) {
				t.Fatal("uncommitted phase/membership survived")
			}
			if mode == "after" && typedPhase(t, reopened).Done != 3 {
				t.Fatal("acknowledged phase lost")
			}
		})
	}
}

// The diagnostic is ordinary typed SQL, rather than reconstructing an audit
// journal or accepting a progress export as a transfer receipt.
func TestTypedStateOperatorSQL(t *testing.T) {
	s := newTypedState(t, 10)
	var count int
	err := s.db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE type='table'").Scan(&count)
	if err != nil || count != 14+len(stateSemanticDigestQueries)+len(stateTransferDigestQueries)+len(stateIssuesDigestQueries)+len(stateSelectionDigestQueries)+len(stateDiscoveryDigestQueries) {
		t.Fatalf("schema inventory: %d %v", count, err)
	}
	var journal string
	if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || !strings.EqualFold(journal, "wal") {
		t.Fatalf("journal mode: %s %v", journal, err)
	}
	var synchronous int
	if err := s.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 2 {
		t.Fatalf("synchronous: %d %v", synchronous, err)
	}
	var payload []byte
	if err := s.db.QueryRow("SELECT report FROM ready").Scan(&payload); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if _, err := json.Marshal(typedDigests(t, s)); err != nil {
		t.Fatal(err)
	}
}
