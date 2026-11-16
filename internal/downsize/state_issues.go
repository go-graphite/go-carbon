package downsize

// Unit-wide exclusion ledgers are keyed by the original name, not catalog ID:
// policy rejects can precede catalog admission. They are distinct from sparse
// per-window evidence in issue. Repair archives and removal share one commit.

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const stateIssuesDDL = `
CREATE TABLE ledger_import(path TEXT PRIMARY KEY, scope TEXT NOT NULL REFERENCES scope(id), kind TEXT NOT NULL CHECK(kind IN ('policy','quarantine')), delta INTEGER NOT NULL CHECK(delta IN (0,1))) STRICT;
CREATE TABLE ledger_issue(scope TEXT NOT NULL REFERENCES scope(id), kind TEXT NOT NULL CHECK(kind IN ('policy','quarantine')), name TEXT NOT NULL CHECK(name<>''), version INTEGER NOT NULL CHECK(version=1), reason INTEGER NOT NULL REFERENCES reason(id), phase TEXT NOT NULL CHECK(phase<>''), first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, PRIMARY KEY(scope,kind,name)) STRICT, WITHOUT ROWID;
CREATE INDEX ledger_name ON ledger_issue(name,scope,kind);
CREATE TABLE ledger_repair_run(child_scope TEXT PRIMARY KEY REFERENCES scope(id), version INTEGER NOT NULL CHECK(version=1), quarantined INTEGER NOT NULL CHECK(quarantined>=0), policy INTEGER NOT NULL CHECK(policy>=0)) STRICT;
CREATE TABLE ledger_repair(scope TEXT NOT NULL REFERENCES scope(id), repair_scope TEXT NOT NULL REFERENCES scope(id), name TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), reason INTEGER NOT NULL REFERENCES reason(id), phase TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, PRIMARY KEY(scope,repair_scope,name)) STRICT, WITHOUT ROWID;
`

var stateIssuesDigestQueries = map[string]string{
	"ledger_repair_run": "SELECT * FROM ledger_repair_run ORDER BY child_scope",
	"ledger_issue":      "SELECT i.scope,i.kind,i.name,i.version,r.text,i.phase,i.first_seen,i.last_seen FROM ledger_issue i JOIN reason r ON r.id=i.reason ORDER BY i.scope,i.kind,i.name",
	"ledger_repair":     "SELECT i.scope,i.repair_scope,i.name,i.version,r.text,i.phase,i.first_seen,i.last_seen FROM ledger_repair i JOIN reason r ON r.id=i.reason ORDER BY i.scope,i.repair_scope,i.name",
}

func stateIssueKind(policy bool) string {
	if policy {
		return "policy"
	}
	return "quarantine"
}

func scanStateLedgerIssue(row interface{ Scan(...any) error }) (v syncIssue, err error) {
	var first, last string
	if err = row.Scan(&v.Name, &v.Reason, &v.Phase, &first, &last); err != nil {
		return v, err
	}
	if err = v.FirstSeen.UnmarshalText([]byte(first)); err != nil {
		return v, err
	}
	err = v.LastSeen.UnmarshalText([]byte(last))
	return v, err
}

func readStateLedgerIssue(tx *sql.Tx, scope, kind, name string) (syncIssue, error) {
	return scanStateLedgerIssue(tx.QueryRow(`SELECT i.name,r.text,i.phase,i.first_seen,i.last_seen FROM ledger_issue i JOIN reason r ON r.id=i.reason WHERE i.scope=? AND i.kind=? AND i.name=?`, scope, kind, name))
}

// Snapshot order is last-entry-wins. Delta order is last-entry-wins only when
// LastSeen is at least the previous value. Compare instants, not RFC3339 text:
// legacy files can contain different offsets and nanosecond precision.
func (t *stateTx) putLedgerIssue(scope, kind string, v syncIssue, delta bool) error {
	if kind != "policy" && kind != "quarantine" {
		return errors.New("invalid issue ledger kind")
	}
	if v.Name == "" || v.Reason == "" || v.Phase == "" || v.FirstSeen.IsZero() || v.LastSeen.IsZero() {
		return errors.New("invalid legacy issue")
	}
	if delta {
		old, err := readStateLedgerIssue(t.tx, scope, kind, v.Name)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && v.LastSeen.Before(old.LastSeen) {
			return nil
		}
	}
	var terminal bool
	if err := t.tx.QueryRow("SELECT EXISTS(SELECT 1 FROM ledger_repair_run WHERE child_scope=?)", scope).Scan(&terminal); err != nil {
		return err
	}
	if terminal {
		return errors.New("completed repair child exclusions are immutable")
	}
	first, err := v.FirstSeen.MarshalText()
	if err != nil {
		return err
	}
	last, err := v.LastSeen.MarshalText()
	if err != nil {
		return err
	}
	if _, err = t.tx.Exec("INSERT INTO reason(version,text) VALUES(1,?) ON CONFLICT(text) DO NOTHING", v.Reason); err != nil {
		return err
	}
	_, err = t.tx.Exec(`INSERT INTO ledger_issue SELECT ?,?,?,1,id,?,?,? FROM reason WHERE text=? ON CONFLICT(scope,kind,name) DO UPDATE SET reason=excluded.reason,phase=excluded.phase,first_seen=excluded.first_seen,last_seen=excluded.last_seen`, scope, kind, v.Name, v.Phase, string(first), string(last), v.Reason)
	return err
}

// This is the durable part of syncIssues.record. Callers publish exclusion
// counts/cursors only after this transaction commits. The first diagnosis is
// retained even when a name appears several times in the same batch.
func (t *stateTx) recordLedgerIssues(scope string, rows []syncResidual, phase string, policy bool, now time.Time) error {
	kind := stateIssueKind(policy)
	for _, r := range rows {
		v, err := readStateLedgerIssue(t.tx, scope, kind, r.Name)
		if errors.Is(err, sql.ErrNoRows) {
			v = syncIssue{Name: r.Name, Reason: r.Reason, Phase: phase, FirstSeen: now}
		} else if err != nil {
			return err
		}
		v.LastSeen = now
		if err := t.putLedgerIssue(scope, kind, v, false); err != nil {
			return err
		}
	}
	return nil
}

// Read in bounded name order. Call again with the last returned name. A caller
// requiring one coherent multi-page inventory keeps this read transaction open.
func readStateLedgerPage(tx *sql.Tx, scope, kind, after string, limit int) ([]syncIssue, error) {
	if limit < 1 || limit > statePageNames {
		return nil, errors.New("invalid issue ledger page size")
	}
	if kind != "policy" && kind != "quarantine" {
		return nil, errors.New("invalid issue ledger kind")
	}
	rows, err := tx.Query(`SELECT i.name,r.text,i.phase,i.first_seen,i.last_seen FROM ledger_issue i JOIN reason r ON r.id=i.reason WHERE i.scope=? AND i.kind=? AND i.name>? ORDER BY i.name LIMIT ?`, scope, kind, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []syncIssue{}
	for rows.Next() {
		v, err := scanStateLedgerIssue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Only the legacy provisional class is repairable here. Child quarantine wins
// over child policy just as repairLegacyWindowQuarantine does. Original entries
// are archived before deletion/update in the same transaction. Repeating a
// committed repair cannot resurrect resolved names or prefix a diagnosis twice.
func (t *stateTx) repairLedgerIssues(scope, child string, names []string, now time.Time) (int, error) {
	if scope == child {
		return 0, errors.New("repair must have an independent child scope")
	}
	if len(names) == 0 {
		return 0, nil
	}
	var quarantined, policy, actualQuarantined, actualPolicy int64
	if err := t.tx.QueryRow("SELECT quarantined,policy FROM ledger_repair_run WHERE child_scope=?", child).Scan(&quarantined, &policy); err != nil {
		return 0, fmt.Errorf("quarantine repair child has no terminal result: %w", err)
	}
	if err := t.tx.QueryRow("SELECT coalesce(sum(kind='quarantine'),0),coalesce(sum(kind='policy'),0) FROM ledger_issue WHERE scope=?", child).Scan(&actualQuarantined, &actualPolicy); err != nil {
		return 0, err
	}
	if quarantined != actualQuarantined || policy != actualPolicy {
		return 0, errors.New("repair child exclusions contradict terminal result")
	}
	var resolved int
	for _, name := range names {
		old, err := readStateLedgerIssue(t.tx, scope, "quarantine", name)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if old.Phase != "window retry" || old.Reason != "copy_failed" {
			continue
		}
		// Reject an unknown child even when it has no exclusion rows.
		var parent sql.NullString
		if err := t.tx.QueryRow("SELECT parent FROM scope WHERE id=?", child).Scan(&parent); err != nil {
			return 0, err
		}
		if !parent.Valid || parent.String != scope {
			return 0, fmt.Errorf("repair scope %q is not a child of %q", child, scope)
		}
		if _, err = t.tx.Exec(`INSERT INTO ledger_repair SELECT scope,?,name,1,reason,phase,first_seen,last_seen FROM ledger_issue WHERE scope=? AND kind='quarantine' AND name=? ON CONFLICT(scope,repair_scope,name) DO NOTHING`, child, scope, name); err != nil {
			return 0, err
		}
		failure, err := readStateLedgerIssue(t.tx, child, "quarantine", name)
		if errors.Is(err, sql.ErrNoRows) {
			failure, err = readStateLedgerIssue(t.tx, child, "policy", name)
		}
		if errors.Is(err, sql.ErrNoRows) {
			if _, err = t.tx.Exec("DELETE FROM ledger_issue WHERE scope=? AND kind='quarantine' AND name=?", scope, name); err != nil {
				return 0, err
			}
			resolved++
			continue
		}
		if err != nil {
			return 0, err
		}
		old.Reason = "fresh quarantine repair: " + failure.Reason
		old.LastSeen = now
		if err := t.putLedgerIssue(scope, "quarantine", old, false); err != nil {
			return 0, err
		}
	}
	return resolved, nil
}

// A rejected name need not have a catalog ID. The inspection path queries this
// index directly rather than scanning every issue or inventing a catalog entry.
type stateNamedLedgerIssue struct {
	Scope string `json:"scope"`
	Kind  string `json:"kind"`
	syncIssue
}

func readStateNamedLedgerIssues(tx *sql.Tx, name string) ([]stateNamedLedgerIssue, error) {
	rows, err := tx.Query(`SELECT i.scope,i.kind,i.name,r.text,i.phase,i.first_seen,i.last_seen FROM ledger_issue i JOIN reason r ON r.id=i.reason WHERE i.name=? ORDER BY i.scope,i.kind LIMIT 4097`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stateNamedLedgerIssue{}
	for rows.Next() {
		var v stateNamedLedgerIssue
		var first, last string
		if err := rows.Scan(&v.Scope, &v.Kind, &v.Name, &v.Reason, &v.Phase, &first, &last); err != nil {
			return nil, err
		}
		if err := v.FirstSeen.UnmarshalText([]byte(first)); err != nil {
			return nil, err
		}
		if err := v.LastSeen.UnmarshalText([]byte(last)); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if len(out) > statePageNames {
		return nil, errors.New("named issue lookup exceeds scope limit")
	}
	return out, rows.Err()
}

// Call only after runSync returns nil or ErrSyncQuarantined. The latter is an
// accepted finished repair with exceptions even when its saved phase is verify,
// not complete. Persisting this outcome prevents phase inference from dropping
// a still-running child's exclusions or rejecting a legitimate legacy repair.
func (t *stateTx) finishLedgerRepair(child string) error {
	var parent sql.NullString
	if err := t.tx.QueryRow("SELECT parent FROM scope WHERE id=?", child).Scan(&parent); err != nil {
		return err
	}
	if !parent.Valid {
		return errors.New("repair requires an independent child scope")
	}
	var quarantined, policy int64
	if err := t.tx.QueryRow("SELECT coalesce(sum(kind='quarantine'),0),coalesce(sum(kind='policy'),0) FROM ledger_issue WHERE scope=?", child).Scan(&quarantined, &policy); err != nil {
		return err
	}
	result, err := t.tx.Exec(`INSERT INTO ledger_repair_run VALUES(?,1,?,?) ON CONFLICT(child_scope) DO UPDATE SET child_scope=excluded.child_scope WHERE ledger_repair_run.quarantined=excluded.quarantined AND ledger_repair_run.policy=excluded.policy`, child, quarantined, policy)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("quarantine repair terminal result changed")
	}
	return err
}
