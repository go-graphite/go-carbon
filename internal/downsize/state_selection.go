package downsize

// Immutable ordered selection intents and indexed retry state. A visited group
// advances the legacy scheduling counter; only an explicit resolution retires
// uncertainty. The two concepts must never share a completion bit.

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
)

const stateSelectionDDL = `
CREATE TABLE window_selection(scope TEXT NOT NULL REFERENCES scope(id), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), group_id INTEGER NOT NULL, holder TEXT NOT NULL, first_name TEXT NOT NULL, last_name TEXT NOT NULL, names INTEGER NOT NULL, complete INTEGER NOT NULL CHECK(complete IN (0,1)), saved_at TEXT NOT NULL, selected_present INTEGER NOT NULL CHECK(selected_present IN (0,1)), skipped_present INTEGER NOT NULL CHECK(skipped_present IN (0,1)), sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE INDEX selection_group ON window_selection(scope,group_id,holder,ordinal DESC);
CREATE TABLE window_selection_name(scope TEXT NOT NULL, selection INTEGER NOT NULL, kind INTEGER NOT NULL CHECK(kind IN (0,1)), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(scope,selection,kind,ordinal), FOREIGN KEY(scope,selection) REFERENCES window_selection(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE INDEX selection_name_identity ON window_selection_name(scope,selection,kind,name,ordinal);
CREATE TABLE window_retry(scope TEXT PRIMARY KEY REFERENCES scope(id), version INTEGER NOT NULL CHECK(version=1), floor INTEGER NOT NULL CHECK(floor>=0)) STRICT;
CREATE TABLE window_retry_visited(scope TEXT NOT NULL REFERENCES scope(id), group_id INTEGER NOT NULL, holder TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), PRIMARY KEY(scope,group_id,holder)) STRICT, WITHOUT ROWID;
CREATE TABLE window_resolution_batch(scope TEXT NOT NULL REFERENCES scope(id), token TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), group_id INTEGER NOT NULL, holder TEXT NOT NULL, copied INTEGER NOT NULL, names_present INTEGER NOT NULL CHECK(names_present IN (0,1)), proof_present INTEGER NOT NULL CHECK(proof_present IN (0,1)), sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,token)) STRICT, WITHOUT ROWID;
CREATE TABLE window_resolution_name(scope TEXT NOT NULL, token TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, FOREIGN KEY(scope,token) REFERENCES window_resolution_batch(scope,token), PRIMARY KEY(scope,token,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE window_resolved(scope TEXT NOT NULL REFERENCES scope(id), group_id INTEGER NOT NULL, holder TEXT NOT NULL, name TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), PRIMARY KEY(scope,group_id,holder,name)) STRICT, WITHOUT ROWID;
CREATE TABLE window_resolution_proof(scope TEXT NOT NULL, token TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, reason TEXT NOT NULL, FOREIGN KEY(scope,token) REFERENCES window_resolution_batch(scope,token), PRIMARY KEY(scope,token,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE window_runtime_receipt(scope TEXT NOT NULL REFERENCES scope(id), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), kind TEXT NOT NULL CHECK(kind IN ('window-selected','window-resolved','window-retry-visited','window-retry-floor')), selection INTEGER, token TEXT, group_id INTEGER NOT NULL, holder TEXT NOT NULL, names INTEGER NOT NULL, saved_at TEXT NOT NULL, sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,ordinal), FOREIGN KEY(scope,selection) REFERENCES window_selection(scope,ordinal), FOREIGN KEY(scope,token) REFERENCES window_resolution_batch(scope,token), CHECK((kind='window-selected' AND selection IS NOT NULL AND token IS NULL AND group_id=0 AND holder='' AND names=0) OR (kind='window-resolved' AND selection IS NULL AND token IS NOT NULL AND group_id=0 AND holder='' AND names=0) OR (kind='window-retry-visited' AND selection IS NULL AND token IS NULL) OR (kind='window-retry-floor' AND selection IS NULL AND token IS NULL AND group_id=0 AND holder=''))) STRICT, WITHOUT ROWID;

`

var stateSelectionDigestQueries = map[string]string{
	"window_runtime_receipt":  "SELECT * FROM window_runtime_receipt ORDER BY scope,ordinal",
	"window_selection":        "SELECT * FROM window_selection ORDER BY scope,ordinal",
	"window_selection_name":   "SELECT * FROM window_selection_name ORDER BY scope,selection,kind,ordinal",
	"window_retry":            "SELECT * FROM window_retry ORDER BY scope",
	"window_retry_visited":    "SELECT * FROM window_retry_visited ORDER BY scope,group_id,holder",
	"window_resolution_batch": "SELECT * FROM window_resolution_batch ORDER BY scope,token",
	"window_resolution_name":  "SELECT * FROM window_resolution_name ORDER BY scope,token,ordinal",
	"window_resolved":         "SELECT * FROM window_resolved ORDER BY scope,group_id,holder,name",
	"window_resolution_proof": "SELECT * FROM window_resolution_proof ORDER BY scope,token,ordinal",
}

type stateSelection struct {
	Scope               string
	Ordinal             int64
	Group               int
	Holder, First, Last string
	Names               int
	Complete            bool
	SavedAt             string
	Selected            []string
	Skipped             []syncSkip
}

func selectionDigest(v stateSelection) ([32]byte, error) {
	b, err := json.Marshal(v)
	return sha256.Sum256(b), err
}

func (t *stateTx) putWindowSelection(v stateSelection) error {
	if v.Ordinal < 0 {
		return errors.New("negative selection ordinal")
	}
	sum, err := selectionDigest(v)
	if err != nil {
		return err
	}
	var old []byte
	err = t.tx.QueryRow("SELECT sha256 FROM window_selection WHERE scope=? AND ordinal=?", v.Scope, v.Ordinal).Scan(&old)
	if err == nil {
		// Read the projection too: a matching stored checksum cannot conceal a
		// changed child name, nil/empty flag or selected order.
		_, err := readStateWindowSelection(t.tx, v.Scope, v.Ordinal)
		if err != nil {
			return err
		}
		if !bytes.Equal(old, sum[:]) {
			return errors.New("immutable window selection changed")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err = t.tx.Exec("INSERT INTO window_selection VALUES(?,?,1,?,?,?,?,?,?,?,?,?,?)", v.Scope, v.Ordinal, v.Group, v.Holder, v.First, v.Last, v.Names, v.Complete, v.SavedAt, v.Selected != nil, v.Skipped != nil, sum[:]); err != nil {
		return err
	}
	for i, name := range v.Selected {
		if _, err = t.tx.Exec("INSERT INTO window_selection_name VALUES(?,?,0,?,1,?,'')", v.Scope, v.Ordinal, i, name); err != nil {
			return err
		}
	}
	for i, skip := range v.Skipped {
		if _, err = t.tx.Exec("INSERT INTO window_selection_name VALUES(?,?,1,?,1,?,?)", v.Scope, v.Ordinal, i, skip.Name, skip.Reason); err != nil {
			return err
		}
	}
	return nil
}

func readStateWindowSelection(tx *sql.Tx, scope string, ordinal int64) (v stateSelection, err error) {
	v.Scope, v.Ordinal = scope, ordinal
	var selected, skipped bool
	var want []byte
	err = tx.QueryRow("SELECT group_id,holder,first_name,last_name,names,complete,saved_at,selected_present,skipped_present,sha256 FROM window_selection WHERE scope=? AND ordinal=?", scope, ordinal).Scan(&v.Group, &v.Holder, &v.First, &v.Last, &v.Names, &v.Complete, &v.SavedAt, &selected, &skipped, &want)
	if err != nil {
		return v, err
	}
	if selected {
		v.Selected = []string{}
	}
	if skipped {
		v.Skipped = []syncSkip{}
	}
	rows, err := tx.Query("SELECT kind,ordinal,name,reason FROM window_selection_name WHERE scope=? AND selection=? ORDER BY kind,ordinal", scope, ordinal)
	if err != nil {
		return v, err
	}
	for rows.Next() {
		var kind, order int
		var name, reason string
		if err = rows.Scan(&kind, &order, &name, &reason); err != nil {
			break
		}
		if kind == 0 {
			if !selected || order != len(v.Selected) || reason != "" {
				err = errors.New("invalid selected name projection")
				break
			}
			v.Selected = append(v.Selected, name)
		} else {
			if !skipped || order != len(v.Skipped) {
				err = errors.New("invalid skipped name projection")
				break
			}
			v.Skipped = append(v.Skipped, syncSkip{Name: name, Reason: reason})
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return v, err
	}
	sum, err := selectionDigest(v)
	if err == nil && !bytes.Equal(want, sum[:]) {
		err = errors.New("window selection checksum mismatch")
	}
	return v, err
}

func readStateLatestWindowSelection(tx *sql.Tx, scope string, group int, holder string) (stateSelection, error) {
	var ordinal int64
	if err := tx.QueryRow("SELECT ordinal FROM window_selection WHERE scope=? AND group_id=? AND holder=? ORDER BY ordinal DESC LIMIT 1", scope, group, holder).Scan(&ordinal); err != nil {
		return stateSelection{}, err
	}
	return readStateWindowSelection(tx, scope, ordinal)
}

func (t *stateTx) putWindowRetryFloor(scope string, floor int64) error {
	_, err := t.tx.Exec("INSERT INTO window_retry VALUES(?,1,?) ON CONFLICT(scope) DO UPDATE SET floor=max(floor,excluded.floor)", scope, max(int64(0), floor))
	return err
}
func (t *stateTx) visitWindowSelection(scope string, group int, holder string) error {
	_, err := t.tx.Exec("INSERT INTO window_retry_visited VALUES(?,?,?,1) ON CONFLICT DO NOTHING", scope, group, holder)
	return err
}

// A stable token binds one resolution's copied count, exact names and source
// absence evidence. It can share a transaction with a native receipt/phase write.
// Distinct historical tokens preserve the old sum; replaying one token adds 0.
func (t *stateTx) resolveWindowSelection(scope, token string, group int, holder string, names []string, proof []syncSkip, copied int) error {
	if token == "" {
		return errors.New("window resolution requires a stable token")
	}
	b, err := json.Marshal([]any{group, holder, names, proof, copied})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	var old []byte
	err = t.tx.QueryRow("SELECT sha256 FROM window_resolution_batch WHERE scope=? AND token=?", scope, token).Scan(&old)
	if err == nil {
		if !bytes.Equal(old, sum[:]) {
			return errors.New("window resolution token changed")
		}
		return checkStateWindowResolution(t.tx, scope, token)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err = t.tx.Exec("INSERT INTO window_resolution_batch VALUES(?,?,1,?,?,?,?,?,?)", scope, token, group, holder, copied, names != nil, proof != nil, sum[:]); err != nil {
		return err
	}
	for ordinal, name := range names {
		if _, err = t.tx.Exec("INSERT INTO window_resolution_name VALUES(?,?,?,1,?)", scope, token, ordinal, name); err != nil {
			return err
		}
		if _, err = t.tx.Exec("INSERT INTO window_resolved VALUES(?,?,?,?,1) ON CONFLICT DO NOTHING", scope, group, holder, name); err != nil {
			return err
		}
	}
	for i, p := range proof {
		if _, err = t.tx.Exec("INSERT INTO window_resolution_proof VALUES(?,?,?,1,?,?)", scope, token, i, p.Name, p.Reason); err != nil {
			return err
		}
	}
	return nil
}

type stateRetryCounts struct{ Total, Durable, Floor, Copied int64 }

func readStateWindowRetryCounts(tx *sql.Tx, scope string) (out stateRetryCounts, err error) {
	err = tx.QueryRow(`SELECT count(*),coalesce(sum(CASE WHEN v.scope IS NOT NULL OR r.scope IS NOT NULL THEN 1 ELSE 0 END),0) FROM window_selection_name n JOIN window_selection s ON s.scope=n.scope AND s.ordinal=n.selection LEFT JOIN window_retry_visited v ON v.scope=s.scope AND v.group_id=s.group_id AND v.holder=s.holder LEFT JOIN window_resolved r ON r.scope=s.scope AND r.group_id=s.group_id AND r.holder=s.holder AND r.name=n.name WHERE n.scope=? AND n.kind=1`, scope).Scan(&out.Total, &out.Durable)
	if err != nil {
		return out, err
	}
	err = tx.QueryRow("SELECT coalesce((SELECT floor FROM window_retry WHERE scope=?),0),coalesce((SELECT sum(copied) FROM window_resolution_batch WHERE scope=?),0)", scope, scope).Scan(&out.Floor, &out.Copied)
	out.Floor = min(out.Total, max(out.Floor, out.Durable))
	return out, err
}

type statePendingSelection struct {
	Ordinal int
	syncSkip
}

// A visited-but-unresolved name remains pending. Scheduler queries may exclude
// quarantines; coverage queries exclude only policy entries. Neither query
// confuses attempted progress with confirmed source coverage.
func readStatePendingSelection(tx *sql.Tx, scope string, selection int64, after, limit int, scheduler bool) ([]statePendingSelection, error) {
	if limit < 1 || limit > statePageNames {
		return nil, errors.New("invalid pending selection page size")
	}
	rows, err := tx.Query(`SELECT n.ordinal,n.name,n.reason FROM window_selection_name n JOIN window_selection s ON s.scope=n.scope AND s.ordinal=n.selection LEFT JOIN window_resolved r ON r.scope=s.scope AND r.group_id=s.group_id AND r.holder=s.holder AND r.name=n.name WHERE n.scope=? AND n.selection=? AND n.kind=1 AND n.ordinal>? AND r.scope IS NULL AND NOT EXISTS(SELECT 1 FROM ledger_issue i WHERE i.scope=n.scope AND i.name=n.name AND (i.kind='policy' OR ?)) AND (NOT ? OR n.ordinal=(SELECT min(d.ordinal) FROM window_selection_name d WHERE d.scope=n.scope AND d.selection=n.selection AND d.kind=1 AND d.name=n.name)) ORDER BY n.ordinal LIMIT ?`, scope, selection, after, scheduler, scheduler, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []statePendingSelection{}
	for rows.Next() {
		var p statePendingSelection
		if err := rows.Scan(&p.Ordinal, &p.Name, &p.Reason); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Check the immutable receipt projection, not just its saved input checksum.
func checkStateWindowResolution(tx *sql.Tx, scope, token string) error {
	var group, copied int
	var holder string
	var namesPresent, proofPresent bool
	var want []byte
	if err := tx.QueryRow("SELECT group_id,holder,copied,names_present,proof_present,sha256 FROM window_resolution_batch WHERE scope=? AND token=?", scope, token).Scan(&group, &holder, &copied, &namesPresent, &proofPresent, &want); err != nil {
		return err
	}
	var names []string
	var proof []syncSkip
	if namesPresent {
		names = []string{}
	}
	if proofPresent {
		proof = []syncSkip{}
	}
	if err := stateTaskRows(tx, "SELECT ordinal,name FROM window_resolution_name WHERE scope=? AND token=? ORDER BY ordinal", []any{scope, token}, func(rows *sql.Rows) error {
		var ordinal int
		var name string
		if err := rows.Scan(&ordinal, &name); err != nil {
			return err
		}
		if !namesPresent || ordinal != len(names) {
			return errors.New("invalid resolution name projection")
		}
		names = append(names, name)
		return nil
	}); err != nil {
		return err
	}
	if err := stateTaskRows(tx, "SELECT ordinal,name,reason FROM window_resolution_proof WHERE scope=? AND token=? ORDER BY ordinal", []any{scope, token}, func(rows *sql.Rows) error {
		var ordinal int
		var p syncSkip
		if err := rows.Scan(&ordinal, &p.Name, &p.Reason); err != nil {
			return err
		}
		if !proofPresent || ordinal != len(proof) {
			return errors.New("invalid resolution proof projection")
		}
		proof = append(proof, p)
		return nil
	}); err != nil {
		return err
	}
	var missing int
	if err := tx.QueryRow(`SELECT count(*) FROM window_resolution_name n JOIN window_resolution_batch b ON b.scope=n.scope AND b.token=n.token WHERE n.scope=? AND n.token=? AND NOT EXISTS(SELECT 1 FROM window_resolved r WHERE r.scope=n.scope AND r.group_id=b.group_id AND r.holder=b.holder AND r.name=n.name)`, scope, token).Scan(&missing); err != nil {
		return err
	}
	if missing != 0 {
		return errors.New("resolution membership contradicts immutable receipt")
	}
	b, err := json.Marshal([]any{group, holder, names, proof, copied})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	if !bytes.Equal(want, sum[:]) {
		return errors.New("window resolution projection checksum mismatch")
	}
	return nil
}
