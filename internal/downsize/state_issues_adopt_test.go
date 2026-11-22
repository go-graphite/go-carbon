package downsize

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestTypedStateIssueAdoptionRestartAndLegacyParity(t *testing.T) {
	s := newTypedState(t, 0)
	root := filepath.Dir(s.path)
	var snapshot bytes.Buffer
	snapshot.WriteByte('[')
	now := time.Date(2026, 11, 16, 19, 0, 0, 1, time.UTC)
	var entries []syncIssue
	var cut int64
	for i := 0; i < statePageNames+9; i++ {
		if i > 0 {
			snapshot.WriteByte(',')
		}
		v := syncIssue{Name: fmt.Sprintf("name.%05d", i%100), Reason: fmt.Sprint("observation-", i), Phase: "verify", FirstSeen: now.Add(-time.Hour), LastSeen: now.Add(time.Duration(-i) * time.Second)}
		entries = append(entries, v)
		data, _ := json.Marshal(v)
		snapshot.Write(data)
		if i == statePageNames-1 {
			cut = int64(snapshot.Len())
		}
	}
	snapshot.WriteString("]\n")
	path := filepath.Join(root, "quarantine.json")
	if err := os.WriteFile(path, snapshot.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	delta := []syncIssue{entries[0]}
	delta[0].Reason = "latest delta"
	delta[0].LastSeen = now.Add(time.Hour)
	line, _ := json.Marshal(delta)
	journal := append(append(line, '\n'), []byte(`[{"name":"partial"`)...)
	if err := os.WriteFile(path+".jsonl", journal, 0600); err != nil {
		t.Fatal(err)
	}
	// Seed an actual committed page and import cursor, then close/reopen. The
	// continuation must skip its consumed prefix rather than replay old snapshot
	// rows over a later committed diagnosis.
	f, err := s.adoptSource("quarantine.json")
	if err != nil {
		t.Fatal(err)
	}
	next := f.cursor
	next.Offset = cut
	next.Records = statePageNames
	if _, err = io.CopyN(f.prefix, f.file, cut); err != nil {
		t.Fatal(err)
	}
	copy(next.PrefixSHA256[:], f.prefix.Sum(nil))
	if err = f.commit(next, func(tx *stateTx) error {
		if err := tx.bindIssueImport("quarantine.json", "root", "quarantine", false); err != nil {
			return err
		}
		for _, v := range entries[:statePageNames] {
			if err := tx.putLedgerIssue("root", "quarantine", v, false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = f.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStateStore(s.path, s.unit, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	stats, err := s.adoptIssueLedger("quarantine.json", "root", "quarantine", false)
	if err != nil || stats.ResumedNames != statePageNames || stats.AdoptedNames != 9 {
		t.Fatal(stats, err)
	}
	stats, err = s.adoptIssueLedger("quarantine.json.jsonl", "root", "quarantine", true)
	if err != nil || stats.AdoptedNames != 1 {
		t.Fatal(stats, err)
	}
	// The actual legacy reader truncates torn tails, so run it on a distinct
	// byte-identical copy and prove the authenticated inputs remain unchanged.
	legacy := t.TempDir()
	if err = os.WriteFile(filepath.Join(legacy, "quarantine.json"), snapshot.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(legacy, "quarantine.json.jsonl"), journal, 0600); err != nil {
		t.Fatal(err)
	}
	oracle, err := openSyncIssues(legacy)
	if err != nil {
		t.Fatal(err)
	}
	stateAssertLedger(t, s, "root", "quarantine", oracle.quarantined)
	// A compacted legacy snapshot has no obsolete diagnoses. Canonical semantic
	// hashes must ignore unreferenced interning rows and operational cursors.
	projection := newTypedState(t, 0)
	if err = projection.update(func(tx *stateTx) error {
		for _, v := range oracle.quarantined {
			if err := tx.putLedgerIssue("root", "quarantine", v, false); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(typedDigests(t, projection), typedDigests(t, s)) {
		t.Fatal("semantic digest depends on obsolete reasons or import history")
	}
	before := typedDigests(t, s)
	for _, input := range []struct {
		path  string
		delta bool
	}{{"quarantine.json", false}, {"quarantine.json.jsonl", true}} {
		stats, err = s.adoptIssueLedger(input.path, "root", "quarantine", input.delta)
		if err != nil || stats.AdoptedNames != 0 {
			t.Fatal(stats, err)
		}
	}
	if !reflect.DeepEqual(before, typedDigests(t, s)) {
		t.Fatal("completed import changed semantic state")
	}
	if _, err = s.adoptIssueLedger("quarantine.json", "root", "policy", false); err == nil {
		t.Fatal("import rebound to another ledger")
	}
	for _, input := range []struct {
		path string
		want []byte
	}{{path, snapshot.Bytes()}, {path + ".jsonl", journal}} {
		got, err := os.ReadFile(input.path)
		if err != nil || !bytes.Equal(got, input.want) {
			t.Fatal("legacy bytes changed", err)
		}
	}
}

func TestTypedStateIssueAdoptionCorruptionAndTornTail(t *testing.T) {
	for _, tc := range []struct {
		name      string
		data      string
		delta, ok bool
	}{
		{"null", "null", false, true}, {"empty", "[]\n", false, true}, {"snapshot-torn", "[", false, false},
		{"snapshot-trailing", "[] {}", false, false}, {"delta-torn", `[{"name":"broken"`, true, true},
		{"delta-empty", "\n", true, false}, {"delta-corrupt-complete", "{\n", true, false},
		{"invalid-snapshot-row", `[{"name":"bad"}]`, false, false},
		{"invalid-delta-row", "[{\"name\":\"bad\"}]\n", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTypedState(t, 0)
			path := filepath.Join(filepath.Dir(s.path), "ledger")
			original := []byte(tc.data)
			sum := sha256.Sum256(original)
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := s.adoptIssueLedger("ledger", "root", "quarantine", tc.delta)
			if (err == nil) != tc.ok {
				t.Fatalf("success=%v err=%v", tc.ok, err)
			}
			got, err := os.ReadFile(path)
			if err != nil || sha256.Sum256(got) != sum {
				t.Fatal("source mutated", err)
			}
			var count int
			if err = s.db.QueryRow("SELECT count(*) FROM ledger_issue").Scan(&count); err != nil || count != 0 {
				t.Fatal("bad issue committed", count, err)
			}
		})
	}
}
