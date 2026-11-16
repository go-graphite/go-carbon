package downsize

// Verification observations retain exact hole ranges, generation/fence evidence
// and recovery membership. These are typed relations, never JSON event blobs.

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
)

const stateVerificationDDL = `
CREATE TABLE verification_receipt(scope TEXT NOT NULL REFERENCES scope(id), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), kind TEXT NOT NULL CHECK(kind IN ('verified','receipt-reverified','ambiguous-receipt')), round INTEGER NOT NULL CHECK(round>=0), identity TEXT NOT NULL, first_name TEXT NOT NULL, last_name TEXT NOT NULL, names INTEGER NOT NULL CHECK(names>=0), saved_at TEXT NOT NULL, mutation INTEGER NOT NULL CHECK(mutation IN (0,1)), legacy_adopted INTEGER NOT NULL, legacy_reverified INTEGER NOT NULL, legacy_observation INTEGER NOT NULL CHECK(legacy_observation IN (0,1)), legacy_time TEXT NOT NULL, legacy_revalidated INTEGER NOT NULL, reused INTEGER NOT NULL, proof TEXT NOT NULL, reason TEXT NOT NULL, lists INTEGER NOT NULL CHECK(lists BETWEEN 0 AND 8191), sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE INDEX verification_round ON verification_receipt(scope,round,ordinal);
CREATE INDEX verification_first ON verification_receipt(scope,round,kind,first_name,ordinal DESC);
CREATE TABLE verification_name(scope TEXT NOT NULL, receipt INTEGER NOT NULL, kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 7), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(scope,receipt,kind,ordinal), FOREIGN KEY(scope,receipt) REFERENCES verification_receipt(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE verification_hole(scope TEXT NOT NULL, receipt INTEGER NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, holder TEXT NOT NULL, window TEXT NOT NULL, step INTEGER NOT NULL, points INTEGER NOT NULL, first INTEGER NOT NULL, last INTEGER NOT NULL, PRIMARY KEY(scope,receipt,ordinal), FOREIGN KEY(scope,receipt) REFERENCES verification_receipt(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE INDEX verification_hole_identity ON verification_hole(scope,holder,name,receipt DESC);
CREATE TABLE verification_evidence(scope TEXT NOT NULL, receipt INTEGER NOT NULL, kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 2), name TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), expiration INTEGER NOT NULL, proof TEXT NOT NULL, PRIMARY KEY(scope,receipt,kind,name), FOREIGN KEY(scope,receipt) REFERENCES verification_receipt(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE verification_work(scope TEXT NOT NULL, receipt INTEGER NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), reason TEXT NOT NULL, keys_present INTEGER NOT NULL CHECK(keys_present IN (0,1)), ids_present INTEGER NOT NULL CHECK(ids_present IN (0,1)), PRIMARY KEY(scope,receipt,ordinal), FOREIGN KEY(scope,receipt) REFERENCES verification_receipt(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE verification_work_key(scope TEXT NOT NULL, receipt INTEGER NOT NULL, work INTEGER NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), value BLOB NOT NULL CHECK(length(value)=8), PRIMARY KEY(scope,receipt,work,ordinal), FOREIGN KEY(scope,receipt,work) REFERENCES verification_work(scope,receipt,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE verification_work_range(scope TEXT NOT NULL, receipt INTEGER NOT NULL, work INTEGER NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), first INTEGER NOT NULL CHECK(first>=0), last INTEGER NOT NULL CHECK(last>=first), PRIMARY KEY(scope,receipt,work,ordinal), FOREIGN KEY(scope,receipt,work) REFERENCES verification_work(scope,receipt,ordinal)) STRICT, WITHOUT ROWID;
`

var stateVerificationDigestQueries = map[string]string{
	"verification_receipt":    "SELECT * FROM verification_receipt ORDER BY scope,ordinal",
	"verification_name":       "SELECT * FROM verification_name ORDER BY scope,receipt,kind,ordinal",
	"verification_hole":       "SELECT * FROM verification_hole ORDER BY scope,receipt,ordinal",
	"verification_evidence":   "SELECT * FROM verification_evidence ORDER BY scope,receipt,kind,name",
	"verification_work":       "SELECT * FROM verification_work ORDER BY scope,receipt,ordinal",
	"verification_work_key":   "SELECT * FROM verification_work_key ORDER BY scope,receipt,work,ordinal",
	"verification_work_range": "SELECT * FROM verification_work_range ORDER BY scope,receipt,work,ordinal",
}

func stateVerificationFlags(r syncRow) int {
	flags := 0
	for i, present := range []bool{r.Absent != nil, r.Unindexed != nil, r.Short != nil, r.LegacyTargetPresent != nil, r.LegacyInvalidations != nil, r.Selected != nil, r.Skipped != nil, r.Resolved != nil, r.Holes != nil, r.LegacyExpiry != nil, r.LegacyDonorProofs != nil, r.HoleProofs != nil, r.Work != nil} {
		if present {
			flags |= 1 << i
		}
	}
	return flags
}

func stateVerificationDigest(r syncRow) ([32]byte, error) {
	b, err := json.Marshal(struct {
		Row   syncRow
		Flags int
	}{r, stateVerificationFlags(r)})
	return sha256.Sum256(b), err
}

func (t *stateTx) appendVerification(scope string, r syncRow) error {
	if r.Kind != "verified" && r.Kind != "receipt-reverified" && r.Kind != "ambiguous-receipt" {
		return errors.New("invalid verification receipt kind")
	}
	projected := syncRow{Kind: r.Kind, Round: r.Round, Identity: r.Identity, First: r.First, Last: r.Last, Names: r.Names, Time: r.Time, MutationObserved: r.MutationObserved, LegacyAdopted: r.LegacyAdopted, LegacyReverified: r.LegacyReverified, LegacyObservation: r.LegacyObservation, LegacyObservationTime: r.LegacyObservationTime, LegacyRevalidated: r.LegacyRevalidated, Reused: r.Reused, ObservationProof: r.ObservationProof, InvalidationReason: r.InvalidationReason, Absent: r.Absent, Unindexed: r.Unindexed, Short: r.Short, LegacyTargetPresent: r.LegacyTargetPresent, LegacyInvalidations: r.LegacyInvalidations, Selected: r.Selected, Skipped: r.Skipped, Resolved: r.Resolved, Holes: r.Holes, LegacyExpiry: r.LegacyExpiry, LegacyDonorProofs: r.LegacyDonorProofs, HoleProofs: r.HoleProofs, Work: r.Work}
	sum, err := stateVerificationDigest(r)
	if err != nil {
		return err
	}
	check, err := stateVerificationDigest(projected)
	if err != nil || sum != check || !reflect.DeepEqual(r, projected) {
		return errors.New("unmapped verification receipt fields")
	}
	var ordinal int64
	if err := t.tx.QueryRow("SELECT coalesce(max(ordinal)+1,0) FROM verification_receipt WHERE scope=?", scope).Scan(&ordinal); err != nil {
		return err
	}
	if _, err := t.tx.Exec("INSERT INTO verification_receipt VALUES(?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", scope, ordinal, r.Kind, r.Round, r.Identity, r.First, r.Last, r.Names, r.Time, r.MutationObserved, r.LegacyAdopted, r.LegacyReverified, r.LegacyObservation, r.LegacyObservationTime, r.LegacyRevalidated, r.Reused, r.ObservationProof, r.InvalidationReason, stateVerificationFlags(r), sum[:]); err != nil {
		return err
	}
	for kind, names := range [][]string{r.Absent, r.Unindexed, r.Short, r.LegacyTargetPresent, r.LegacyInvalidations, r.Selected} {
		for order, name := range names {
			if _, err := t.tx.Exec("INSERT INTO verification_name VALUES(?,?,?,?,1,?,'')", scope, ordinal, kind, order, name); err != nil {
				return err
			}
		}
	}
	for kind, skips := range [][]syncSkip{r.Skipped, r.Resolved} {
		for order, skip := range skips {
			if _, err := t.tx.Exec("INSERT INTO verification_name VALUES(?,?,?,?,1,?,?)", scope, ordinal, kind+6, order, skip.Name, skip.Reason); err != nil {
				return err
			}
		}
	}
	for order, h := range r.Holes {
		if _, err := t.tx.Exec("INSERT INTO verification_hole VALUES(?,?,?,1,?,?,?,?,?,?,?)", scope, ordinal, order, h.Name, h.Source, h.Window, h.Step, h.Points, h.First, h.Last); err != nil {
			return err
		}
	}
	for name, expiry := range r.LegacyExpiry {
		if _, err := t.tx.Exec("INSERT INTO verification_evidence VALUES(?,?,0,?,1,?,'')", scope, ordinal, name, expiry); err != nil {
			return err
		}
	}
	for kind, proofs := range []map[string]string{r.LegacyDonorProofs, r.HoleProofs} {
		for name, proof := range proofs {
			if _, err := t.tx.Exec("INSERT INTO verification_evidence VALUES(?,?,?,?,1,0,?)", scope, ordinal, kind+1, name, proof); err != nil {
				return err
			}
		}
	}
	for order, w := range r.Work {
		if _, err := t.tx.Exec("INSERT INTO verification_work VALUES(?,?,?,1,?,?,?)", scope, ordinal, order, w.Reason, w.Keys != nil, w.IDs != nil); err != nil {
			return err
		}
		for i, key := range w.Keys {
			value := binary.BigEndian.AppendUint64(nil, key)
			if _, err := t.tx.Exec("INSERT INTO verification_work_key VALUES(?,?,?,?,1,?)", scope, ordinal, order, i, value); err != nil {
				return err
			}
		}
		for i, ids := range w.IDs {
			if _, err := t.tx.Exec("INSERT INTO verification_work_range VALUES(?,?,?,?,1,?,?)", scope, ordinal, order, i, ids.First, ids.Last); err != nil {
				return err
			}
		}
	}
	return nil
}

func readStateVerification(tx *sql.Tx, scope string, ordinal int64) (r syncRow, err error) {
	var flags int
	var want []byte
	err = tx.QueryRow("SELECT kind,round,identity,first_name,last_name,names,saved_at,mutation,legacy_adopted,legacy_reverified,legacy_observation,legacy_time,legacy_revalidated,reused,proof,reason,lists,sha256 FROM verification_receipt WHERE scope=? AND ordinal=?", scope, ordinal).Scan(&r.Kind, &r.Round, &r.Identity, &r.First, &r.Last, &r.Names, &r.Time, &r.MutationObserved, &r.LegacyAdopted, &r.LegacyReverified, &r.LegacyObservation, &r.LegacyObservationTime, &r.LegacyRevalidated, &r.Reused, &r.ObservationProof, &r.InvalidationReason, &flags, &want)
	if err != nil {
		return r, err
	}
	lists := []*[]string{&r.Absent, &r.Unindexed, &r.Short, &r.LegacyTargetPresent, &r.LegacyInvalidations, &r.Selected}
	for i, list := range lists {
		if flags&(1<<i) != 0 {
			*list = []string{}
		}
	}
	if flags&(1<<6) != 0 {
		r.Skipped = []syncSkip{}
	}
	if flags&(1<<7) != 0 {
		r.Resolved = []syncSkip{}
	}
	if flags&(1<<8) != 0 {
		r.Holes = []syncHole{}
	}
	if flags&(1<<9) != 0 {
		r.LegacyExpiry = map[string]int64{}
	}
	if flags&(1<<10) != 0 {
		r.LegacyDonorProofs = map[string]string{}
	}
	if flags&(1<<11) != 0 {
		r.HoleProofs = map[string]string{}
	}
	if flags&(1<<12) != 0 {
		r.Work = []syncRedoWork{}
	}
	args := []any{scope, ordinal}
	if err = stateTaskRows(tx, "SELECT kind,ordinal,name,reason FROM verification_name WHERE scope=? AND receipt=? ORDER BY kind,ordinal", args, func(rows *sql.Rows) error {
		var kind, order int
		var name, reason string
		if err := rows.Scan(&kind, &order, &name, &reason); err != nil {
			return err
		}
		if kind < 0 || kind > 7 || flags&(1<<kind) == 0 {
			return errors.New("invalid verification membership kind")
		}
		if kind < 6 {
			if order != len(*lists[kind]) || reason != "" {
				return errors.New("invalid verification name order")
			}
			*lists[kind] = append(*lists[kind], name)
		} else {
			list := &r.Skipped
			if kind == 7 {
				list = &r.Resolved
			}
			if order != len(*list) {
				return errors.New("invalid verification skip order")
			}
			*list = append(*list, syncSkip{Name: name, Reason: reason})
		}
		return nil
	}); err != nil {
		return r, err
	}
	if err = stateTaskRows(tx, "SELECT ordinal,name,holder,window,step,points,first,last FROM verification_hole WHERE scope=? AND receipt=? ORDER BY ordinal", args, func(rows *sql.Rows) error {
		var order int
		var h syncHole
		if err := rows.Scan(&order, &h.Name, &h.Source, &h.Window, &h.Step, &h.Points, &h.First, &h.Last); err != nil {
			return err
		}
		if flags&(1<<8) == 0 || order != len(r.Holes) {
			return errors.New("invalid verification hole order")
		}
		r.Holes = append(r.Holes, h)
		return nil
	}); err != nil {
		return r, err
	}
	if err = stateTaskRows(tx, "SELECT kind,name,expiration,proof FROM verification_evidence WHERE scope=? AND receipt=? ORDER BY kind,name", args, func(rows *sql.Rows) error {
		var kind int
		var name, proof string
		var expiry int64
		if err := rows.Scan(&kind, &name, &expiry, &proof); err != nil {
			return err
		}
		if kind < 0 || kind > 2 || flags&(1<<(kind+9)) == 0 || kind == 0 && proof != "" || kind != 0 && expiry != 0 {
			return errors.New("invalid verification evidence")
		}
		switch kind {
		case 0:
			r.LegacyExpiry[name] = expiry
		case 1:
			r.LegacyDonorProofs[name] = proof
		case 2:
			r.HoleProofs[name] = proof
		}
		return nil
	}); err != nil {
		return r, err
	}
	if err = stateTaskRows(tx, "SELECT ordinal,reason,keys_present,ids_present FROM verification_work WHERE scope=? AND receipt=? ORDER BY ordinal", args, func(rows *sql.Rows) error {
		var order int
		var w syncRedoWork
		var keys, ids bool
		if err := rows.Scan(&order, &w.Reason, &keys, &ids); err != nil {
			return err
		}
		if flags&(1<<12) == 0 || order != len(r.Work) {
			return errors.New("invalid verification work order")
		}
		if keys {
			w.Keys = []uint64{}
		}
		if ids {
			w.IDs = []syncRedoRange{}
		}
		r.Work = append(r.Work, w)
		return nil
	}); err != nil {
		return r, err
	}
	if err = stateTaskRows(tx, "SELECT work,ordinal,value FROM verification_work_key WHERE scope=? AND receipt=? ORDER BY work,ordinal", args, func(rows *sql.Rows) error {
		var work, order int
		var b []byte
		if err := rows.Scan(&work, &order, &b); err != nil {
			return err
		}
		if work < 0 || work >= len(r.Work) || r.Work[work].Keys == nil || order != len(r.Work[work].Keys) || len(b) != 8 {
			return errors.New("invalid verification work key")
		}
		r.Work[work].Keys = append(r.Work[work].Keys, binary.BigEndian.Uint64(b))
		return nil
	}); err != nil {
		return r, err
	}
	if err = stateTaskRows(tx, "SELECT work,ordinal,first,last FROM verification_work_range WHERE scope=? AND receipt=? ORDER BY work,ordinal", args, func(rows *sql.Rows) error {
		var work, order int
		var ids syncRedoRange
		if err := rows.Scan(&work, &order, &ids.First, &ids.Last); err != nil {
			return err
		}
		if work < 0 || work >= len(r.Work) || r.Work[work].IDs == nil || order != len(r.Work[work].IDs) {
			return errors.New("invalid verification work range")
		}
		r.Work[work].IDs = append(r.Work[work].IDs, ids)
		return nil
	}); err != nil {
		return r, err
	}
	sum, err := stateVerificationDigest(r)
	if err == nil && !bytes.Equal(want, sum[:]) {
		err = errors.New("verification receipt projection checksum mismatch")
	}
	return r, err
}
