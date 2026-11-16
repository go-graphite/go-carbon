package downsize

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func stateReadTx(t *testing.T, s *stateStore, read func(*sql.Tx)) {
	t.Helper()
	tx, err := s.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	read(tx)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedStateLegacyPhaseSummary(t *testing.T) {
	s := newTypedState(t, 3)
	p := syncPhasePosition{Phase: "verify", Round: 3, Done: 91, Total: 90, Options: "exact-options", Summary: syncCheckpointSummary{Discovered: 90, OnTarget: 83, Missing: 7, NotOwned: 12, Short: 2, Both: 81, Copied: 6, CopyFailed: 1, Residual: []syncResidual{
		{syncHole: syncHole{Name: "a", Source: "s1", Window: "one", Step: 60, Points: 3, First: 100, Last: 220}, Reason: "missing"},
		{syncHole: syncHole{Name: "a", Source: "s2", Window: "two", Step: 300, Points: 2, First: 400, Last: 700}, Reason: "quarantined"},
	}}}
	if err := s.update(func(tx *stateTx) error { return tx.putPhasePosition("root", p) }); err != nil {
		t.Fatal(err)
	}
	assert := func() {
		stateReadTx(t, s, func(tx *sql.Tx) {
			got, err := readStatePhasePosition(tx, "root")
			if err != nil || !reflect.DeepEqual(got, p) {
				t.Fatalf("position/summary changed: got %+v want %+v err %v", got, p, err)
			}
		})
	}
	assert()
	// All scheduling/summary changes roll back together, including a new residual.
	before := typedDigests(t, s)
	errAbort := errors.New("injected failure after phase/summary")
	if err := s.update(func(tx *stateTx) error {
		next := p
		next.Done++
		next.Summary.Copied++
		if err := tx.putPhasePosition("root", next); err != nil {
			return err
		}
		return errAbort
	}); !errors.Is(err, errAbort) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("partial phase/summary transaction")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	defer s.close()
	assert()
	for _, empty := range [][]syncResidual{nil, {}} {
		p.Done++
		p.Summary.Residual = empty
		if err := s.update(func(tx *stateTx) error { return tx.putPhasePosition("root", p) }); err != nil {
			t.Fatal(err)
		}
		assert()
	}
}

func TestTypedStateLegacyProgressReducerRestart(t *testing.T) {
	s := newTypedState(t, 10)
	var oracle progressSnapshot
	rows := []syncRow{
		{Kind: "discover_checkpoint", Discovery: &syncDiscoveryCheckpoint{Names: 3, Cursors: map[string]string{"s1": "c", "s2": "b"}}},
		{Kind: "discover_checkpoint", Discovery: &syncDiscoveryCheckpoint{Names: 5, Cursors: map[string]string{"s1": "e", "s2": "d"}}},
		{Kind: "discovered", Names: 10},
		{Kind: "copied", Phase: "missing", Copied: 3},
		{Kind: "verified", Round: 1, Names: 7, LegacyAdopted: 2, LegacyReverified: 3, LegacyRevalidated: 1, Holes: []syncHole{{Points: 8}, {Points: 9}}},
		{Kind: "copied", Phase: "fill", Round: 1, Copied: 2},
		{Kind: "verified", Round: 2, Names: 3, Holes: []syncHole{{Points: 2}}},
		{Kind: "ambiguous-receipt", Names: 2}, {Kind: "receipt-contradiction"}, {Kind: "phase-regression"},
		{Kind: "redo", Phase: "verify", Names: 0},
		{Kind: "phase-position", Position: &syncPhasePosition{Round: 1}}, // exactly legacy assignment, not max
		{Kind: "window-selected", Round: 8},                              // not a verified/fill observation
		{Kind: "discover_checkpoint", Discovery: &syncDiscoveryCheckpoint{Names: 10, Cursors: map[string]string{}}},
		{Kind: "discover_checkpoint", Discovery: &syncDiscoveryCheckpoint{Names: 10}},
	}
	for n, r := range rows {
		oracle.apply(r)
		if err := s.update(func(tx *stateTx) error { return tx.reduceProgress("root", r) }); err != nil {
			t.Fatalf("row %d: %v", n, err)
		}
		stateReadTx(t, s, func(tx *sql.Tx) {
			got, err := readStateProgress(tx, "root")
			if err != nil || !reflect.DeepEqual(got, oracle) {
				t.Fatalf("row %d: got %+v want %+v err %v", n, got, oracle, err)
			}
		})
		if n == 5 { // a mid-fill restart, before subsequent verification observations
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			next, err := openStateStore(s.path, s.unit, false)
			if err != nil {
				t.Fatal(err)
			}
			s = next
			defer s.close()
		}
	}
	if oracle.RedoNames["verify"] != 0 {
		t.Fatal("unexpected redo")
	}
	before := typedDigests(t, s)
	fail := errors.New("before commit")
	if err := s.update(func(tx *stateTx) error {
		if err := tx.reduceProgress("root", syncRow{Kind: "copied", Phase: "fill", Copied: 3}); err != nil {
			return err
		}
		return fail
	}); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("progress survived failed transaction")
	}
}

func TestTypedStateIssueEvidenceAndFirstReason(t *testing.T) {
	s := newTypedState(t, 2)
	i := stateIssue{Scope: "root", NameID: 1, Kind: "quarantine", Reason: "first reason", Phase: "copy", FirstSeen: 100, LastSeen: 100}
	ev := []stateIssueEvidence{{60, 3, 100, 220, "sealed-one"}, {300, 2, 400, 700, "sealed-two"}}
	if err := s.update(func(tx *stateTx) error { return tx.putIssueEvidence(i, ev) }); err != nil {
		t.Fatal(err)
	}
	next := i
	next.Reason = "later reason"
	next.Phase = "verify"
	next.FirstSeen = 200
	next.LastSeen = 300
	if err := s.update(func(tx *stateTx) error { return tx.putIssueEvidence(next, ev) }); err != nil {
		t.Fatal(err)
	}
	var reason, phase string
	var first, last int64
	if err := s.db.QueryRow("SELECT reason,phase,first_seen,last_seen FROM issue_detail").Scan(&reason, &phase, &first, &last); err != nil {
		t.Fatal(err)
	}
	if reason != i.Reason || phase != i.Phase || first != 100 || last != 300 {
		t.Fatalf("first issue changed: %s %s %d %d", reason, phase, first, last)
	}
	stateReadTx(t, s, func(tx *sql.Tx) {
		rows, err := tx.Query("SELECT step,points,first,last,proof FROM issue_evidence ORDER BY ordinal")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []stateIssueEvidence
		for rows.Next() {
			var v stateIssueEvidence
			if err = rows.Scan(&v.Step, &v.Points, &v.First, &v.Last, &v.Proof); err != nil {
				t.Fatal(err)
			}
			got = append(got, v)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, ev) {
			t.Fatalf("range evidence: %+v", got)
		}
	})
}

func TestTypedStateLegacyFrontierBytesAndSeal(t *testing.T) {
	s := newTypedState(t, 4097)
	g := stateFrontierGeneration{Scope: "root", Generation: "g1", Identity: "identity", Settle: 2 * time.Minute, Now: 1234567}
	legacy := &syncFrontier{}
	legacy.scope.Identity, legacy.scope.Settle, legacy.scope.Now = g.Identity, g.Settle, g.Now
	b, _ := json.Marshal(legacy.scope)
	legacy.key = string(b)
	if g.key() != legacy.key {
		t.Fatalf("scope seal key changed %q %q", g.key(), legacy.key)
	}
	data := make([]byte, 32*(4097+1))
	var stamp syncProof
	copy(stamp[:], []byte("mutation-token"))
	for _, id := range []int{1, 4096, 4097} {
		r := legacy.record(fmt.Sprintf("name-%d", id), stamp)
		copy(data[id*32:], r[:])
	}
	// ID zero deliberately has nonzero garbage and must never become ID one.
	copy(data, bytes.Repeat([]byte{0xff}, 32))
	data = data[:len(data)-11] // preserve exactly the legacy treatment of a torn last record
	reader := bytes.NewReader(data)
	if err := s.update(func(tx *stateTx) error {
		for _, first := range []int64{1, 4097} {
			if err := tx.importFrontierPage(g, reader, first); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stateReadTx(t, s, func(tx *sql.Tx) {
		for _, first := range []int64{1, 4097} {
			got, err := readStateProofPage(tx, g, first)
			if err != nil {
				t.Fatal(err)
			}
			want := make([]byte, len(got))
			_, err = reader.ReadAt(want, first*32)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("frontier offset changed at %d", first)
			}
		}
	})
	before := typedDigests(t, s)
	changed := g
	changed.Now++
	if err := s.update(func(tx *stateTx) error { return tx.importFrontierPage(changed, reader, 1) }); err == nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("changed generation accepted")
	}
	// A record from another scope/name remains different; copying is no proof synthesis.
	if bytes.Equal(data[32:64], legacy.recordSliceForTest("different", stamp)) {
		t.Fatal("name binding lost")
	}
}

func (f *syncFrontier) recordSliceForTest(name string, p syncProof) []byte {
	r := f.record(name, p)
	return r[:]
}

func TestTypedStateRedoRunsUnsignedKeys(t *testing.T) {
	s := newTypedState(t, 10)
	key := uint64(math.MaxUint64)
	work := []syncRedoWork{{Keys: []uint64{key, 1}, IDs: []syncRedoRange{{1, 3}, {8, 9}}}, {Keys: []uint64{key}, IDs: []syncRedoRange{{3, 8}, {0, 0}}}}
	oracle := &syncRedoGuard{seen: map[uint64][]syncRedoRange{}}
	for _, w := range work {
		oracle.remember(w)
		if err := s.update(func(tx *stateTx) error { return tx.rememberRedo("root", w) }); err != nil {
			t.Fatal(err)
		}
	}
	got := map[uint64][]syncRedoRange{}
	stateReadTx(t, s, func(tx *sql.Tx) {
		rows, err := tx.Query("SELECT operation,first,last FROM redo_work ORDER BY operation,first")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var k []byte
			var r syncRedoRange
			if err = rows.Scan(&k, &r.First, &r.Last); err != nil {
				t.Fatal(err)
			}
			v := binary.BigEndian.Uint64(k)
			got[v] = append(got[v], r)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
	})
	if !reflect.DeepEqual(got, oracle.seen) {
		t.Fatalf("redo receipt changed: %+v want %+v", got, oracle.seen)
	}
	before := typedDigests(t, s)
	if err := s.update(func(tx *stateTx) error { return tx.rememberRedo("root", work[1]) }); err != nil || !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("redo receipt replay changed state", err)
	}
}

func TestTypedStateRejectsPreviousCandidateSchema(t *testing.T) {
	for version := 1; version < stateSchema; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			s := newTypedState(t, 1)
			if err := s.close(); err != nil {
				t.Fatal(err)
			}
			db, err := connectStateDB(s.path, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(fmt.Sprintf("PRAGMA user_version=%d", version)); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(s.path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = openStateStore(s.path, stateUnit{}, true); err == nil {
				t.Fatal("accepted old candidate database")
			}
			after, err := os.ReadFile(s.path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("modified old candidate database")
			}
			if _, err = os.Stat(filepath.Join(filepath.Dir(s.path), "config.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("published configuration")
			}
		})
	}
}
