package downsize

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func stateAssertLedger(t *testing.T, s *stateStore, scope, kind string, want map[string]syncIssue) {
	t.Helper()
	var got []syncIssue
	stateReadTx(t, s, func(tx *sql.Tx) {
		after := ""
		for {
			page, err := readStateLedgerPage(tx, scope, kind, after, 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			got = append(got, page...)
			after = page[len(page)-1].Name
		}
	})
	expected := make([]syncIssue, 0, len(want))
	for _, v := range want {
		expected = append(expected, v)
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].Name < expected[j].Name })
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(expected)
	if len(got) != len(expected) || len(got) > 0 && string(a) != string(b) {
		t.Fatalf("%s ledger differs\ngot %s\nwant %s", kind, a, b)
	}
}

func TestTypedStateIssueLedgerLegacyReducerRestart(t *testing.T) {
	s := newTypedState(t, 0) // None of these names has a catalog ID.
	state := t.TempDir()
	now := time.Date(2026, 11, 16, 19, 0, 0, 123, time.FixedZone("plus-two", 7200))
	a := syncIssue{Name: "a", Reason: "snapshot first", Phase: "verify", FirstSeen: now.Add(-time.Hour), LastSeen: now}
	b := a
	b.Name = strings.Repeat("x", 4097)
	b.Reason = "policy oversized"
	c := a
	c.Name = "λ.metric"
	c.Reason = "unicode"
	overwritten := a
	overwritten.Reason = "snapshot last (older instant)"
	overwritten.LastSeen = now.Add(-time.Minute)
	snapshot := []syncIssue{a, b, c, overwritten}
	newer := a
	newer.Reason = "delta newer"
	newer.LastSeen = now.UTC()
	stale := a
	stale.Reason = "delta stale"
	stale.LastSeen = now.Add(-time.Second)
	tie := newer
	tie.Reason = "delta equal instant wins"
	tie.LastSeen = now.In(time.FixedZone("minus-five", -5*3600))
	deltas := [][]syncIssue{{newer, stale}, {tie}}
	data, _ := json.Marshal(snapshot)
	if err := os.WriteFile(filepath.Join(state, "quarantine.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	var journal []byte
	for _, batch := range deltas {
		row, _ := json.Marshal(batch)
		journal = append(journal, append(row, '\n')...)
	}
	journal = append(journal, []byte(`[{"name":"torn-final"`)...)
	if err := os.WriteFile(filepath.Join(state, "quarantine.json.jsonl"), journal, 0600); err != nil {
		t.Fatal(err)
	}
	oracle, err := openSyncIssues(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.update(func(tx *stateTx) error {
		for _, v := range snapshot {
			if err := tx.putLedgerIssue("root", "quarantine", v, false); err != nil {
				return err
			}
		}
		for _, batch := range deltas {
			for _, v := range batch {
				if err := tx.putLedgerIssue("root", "quarantine", v, true); err != nil {
					return err
				}
			}
		}
		return tx.putLedgerIssue("root", "policy", b, false)
	}); err != nil {
		t.Fatal(err)
	}
	stateAssertLedger(t, s, "root", "quarantine", oracle.quarantined)
	if err = s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	stateAssertLedger(t, s, "root", "quarantine", oracle.quarantined)
	// The actual record reducer supplies the wall clock; the typed reducer takes
	// that same input, never the resulting reason/first diagnosis.
	rows := []syncResidual{{syncHole: syncHole{Name: "a"}, Reason: "ignored new reason"}, {syncHole: syncHole{Name: "fresh"}, Reason: "first"}, {syncHole: syncHole{Name: "fresh"}, Reason: "second"}}
	if err = oracle.record(rows, "fill", false); err != nil {
		t.Fatal(err)
	}
	at := oracle.quarantined["fresh"].LastSeen
	if err = s.update(func(tx *stateTx) error { return tx.recordLedgerIssues("root", rows, "fill", false, at) }); err != nil {
		t.Fatal(err)
	}
	stateAssertLedger(t, s, "root", "quarantine", oracle.quarantined)
	before := typedDigests(t, s)
	abort := errors.New("commit failure")
	if err = s.update(func(tx *stateTx) error {
		if err := tx.recordLedgerIssues("root", []syncResidual{{syncHole: syncHole{Name: "not-committed"}, Reason: "new reason"}}, "verify", false, at); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("partial issue transaction")
	}
	// Existing ledgers accept reversed first/last timestamps; do not silently
	// normalize them or reject an otherwise readable old state during adoption.
	reversed := a
	reversed.LastSeen = reversed.FirstSeen.Add(-time.Hour)
	if err = s.update(func(tx *stateTx) error { return tx.putLedgerIssue("root", "quarantine", reversed, false) }); err != nil {
		t.Fatal(err)
	}
}

func TestTypedStateIssueLedgerRealRepairAtomicRestart(t *testing.T) {
	base, o, _ := newReadyFixture(t, 0)
	o.from = []string{syncD1}
	backend := &quarantineFixture{base, map[string]int{}}
	for _, n := range []string{"already", "repair", "broken", "held"} {
		for _, h := range []string{syncD1, syncT} {
			backend.put(h, n, syncTestSchema)
		}
	}
	for _, n := range []string{"repair", "broken"} {
		delete(backend.store(syncT).archives, n)
		delete(backend.store(syncT).points, n)
	}
	q, err := openSyncIssues(o.state)
	if err != nil {
		t.Fatal(err)
	}
	o.issues = q
	var candidates []syncResidual
	for _, n := range []string{"already", "repair", "broken"} {
		candidates = append(candidates, syncResidual{syncHole: syncHole{Name: n}, Reason: "copy_failed"})
	}
	if err = q.record(candidates, "window retry", false); err != nil {
		t.Fatal(err)
	}
	if err = q.record([]syncResidual{{syncHole: syncHole{Name: "held"}, Reason: "conflict"}}, "verify", false); err != nil {
		t.Fatal(err)
	}
	s := newTypedState(t, 0)
	original := map[string]syncIssue{}
	for n, v := range q.quarantined {
		original[n] = v
	}
	if err = s.update(func(tx *stateTx) error {
		if err := tx.beginScope("repair", "root", "fresh named proof"); err != nil {
			return err
		}
		for _, v := range original {
			if err := tx.putLedgerIssue("root", "quarantine", v, false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = repairLegacyWindowQuarantine(context.Background(), o, backend); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(filepath.Join(o.state, "quarantine-repair-*"))
	if len(matches) != 1 {
		t.Fatal(matches)
	}
	child, err := openSyncIssues(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = s.update(func(tx *stateTx) error {
		for _, v := range child.quarantined {
			if err := tx.putLedgerIssue("repair", "quarantine", v, false); err != nil {
				return err
			}
		}
		for _, v := range child.policy {
			if err := tx.putLedgerIssue("repair", "policy", v, false); err != nil {
				return err
			}
		}
		return tx.finishLedgerRepair("repair")
	}); err != nil {
		t.Fatal(err)
	}
	before := typedDigests(t, s)
	if err := s.update(func(tx *stateTx) error {
		return tx.recordLedgerIssues("repair", []syncResidual{{syncHole: syncHole{Name: "late"}, Reason: "late"}}, "verify", false, time.Now())
	}); err == nil {
		t.Fatal("terminal child exclusions changed")
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("failed terminal update changed child")
	}
	abort := errors.New("failed repair commit")
	names := []string{"already", "repair", "broken", "held"}
	at := q.quarantined["broken"].LastSeen
	if err = s.update(func(tx *stateTx) error {
		if _, err := tx.repairLedgerIssues("root", "repair", names, at); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("repair/archive partially committed")
	}
	if err = s.update(func(tx *stateTx) error {
		n, err := tx.repairLedgerIssues("root", "repair", names, at)
		if err == nil && n != 2 {
			t.Fatalf("resolved %d", n)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stateAssertLedger(t, s, "root", "quarantine", q.quarantined)
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM ledger_repair").Scan(&count); err != nil || count != 3 {
		t.Fatal(count, err)
	}
	before = typedDigests(t, s)
	if err = s.update(func(tx *stateTx) error {
		n, err := tx.repairLedgerIssues("root", "repair", names, at.Add(time.Hour))
		if n != 0 {
			t.Fatalf("repeated repair resolved %d", n)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("repair replay changed durable state")
	}
	if err = s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	reopened, err := openSyncIssues(o.state)
	if err != nil {
		t.Fatal(err)
	}
	stateAssertLedger(t, s, "root", "quarantine", reopened.quarantined)
}

func TestTypedStateInspectOutsideCatalogExclusion(t *testing.T) {
	s := newTypedState(t, 0)
	now := time.Date(2026, 11, 16, 19, 0, 0, 0, time.UTC)
	name := strings.Repeat("x", 4097)
	if err := s.update(func(tx *stateTx) error {
		return tx.recordLedgerIssues("root", []syncResidual{{syncHole: syncHole{Name: name}, Reason: "name exceeds store policy limit of 4096 bytes"}}, "discover", true, now)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := stateMain(context.Background(), []string{"inspect", "--database", s.path, "--name", name}, &output); err != nil {
		t.Fatal(err)
	}
	var got stateInspection
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != nil || got.NameID != 0 || len(got.Exclusions) != 1 || got.Exclusions[0].Name != name || got.Exclusions[0].Kind != "policy" {
		t.Fatal("unindexed exclusion missing")
	}
	after, err := os.ReadFile(s.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("inspection changed database", err)
	}
	if err := stateMain(context.Background(), []string{"inspect", "--database", s.path, "--name", "unknown"}, &output); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("unknown name accepted", err)
	}
}
