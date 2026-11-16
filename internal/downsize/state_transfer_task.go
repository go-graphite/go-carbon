package downsize

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A child transfer keeps its original operation and plan across controller
// restarts. Task, fallback intent, exact selection and receipts commit together.
// These are typed projections, not an alternative JSON journal inside SQLite.
const stateTransferTaskDDL = `
CREATE TABLE child_transfer(scope TEXT NOT NULL REFERENCES scope(id), id TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), operation TEXT NOT NULL, plan TEXT NOT NULL, PRIMARY KEY(scope,id)) STRICT, WITHOUT ROWID;
CREATE TABLE transfer_task(scope TEXT NOT NULL, transfer TEXT NOT NULL, key TEXT NOT NULL, node INTEGER NOT NULL CHECK(node IN (0,1)), version INTEGER NOT NULL CHECK(version=1), native_parent TEXT NOT NULL, kind TEXT NOT NULL, checkpoint TEXT NOT NULL, retries INTEGER NOT NULL, stream TEXT NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL, phase TEXT NOT NULL, bytes INTEGER NOT NULL, slices INTEGER NOT NULL CHECK(slices BETWEEN 0 AND 7), sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,transfer,key,node), FOREIGN KEY(scope,transfer) REFERENCES child_transfer(scope,id)) STRICT, WITHOUT ROWID;
CREATE TABLE transfer_task_name(scope TEXT NOT NULL, transfer TEXT NOT NULL, key TEXT NOT NULL, node INTEGER NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, PRIMARY KEY(scope,transfer,key,node,ordinal), FOREIGN KEY(scope,transfer,key,node) REFERENCES transfer_task(scope,transfer,key,node) ON DELETE CASCADE) STRICT, WITHOUT ROWID;
CREATE TABLE transfer_task_ordinal(scope TEXT NOT NULL, transfer TEXT NOT NULL, key TEXT NOT NULL, node INTEGER NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), value INTEGER NOT NULL, PRIMARY KEY(scope,transfer,key,node,ordinal), FOREIGN KEY(scope,transfer,key,node) REFERENCES transfer_task(scope,transfer,key,node) ON DELETE CASCADE) STRICT, WITHOUT ROWID;
CREATE TABLE transfer_task_proof(scope TEXT NOT NULL, transfer TEXT NOT NULL, key TEXT NOT NULL, node INTEGER NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), key_count INTEGER NOT NULL, sampled INTEGER NOT NULL, source_name TEXT NOT NULL, source_id BLOB NOT NULL CHECK(length(source_id)=8), source_generation BLOB NOT NULL CHECK(length(source_generation)=8), source_revision BLOB NOT NULL CHECK(length(source_revision)=8), target_name TEXT NOT NULL, target_id BLOB NOT NULL CHECK(length(target_id)=8), target_generation BLOB NOT NULL CHECK(length(target_generation)=8), target_revision BLOB NOT NULL CHECK(length(target_revision)=8), digest TEXT NOT NULL, merged INTEGER NOT NULL, counts_present INTEGER NOT NULL, PRIMARY KEY(scope,transfer,key,node,ordinal), FOREIGN KEY(scope,transfer,key,node) REFERENCES transfer_task(scope,transfer,key,node) ON DELETE CASCADE) STRICT, WITHOUT ROWID;
CREATE TABLE transfer_task_count(scope TEXT NOT NULL, transfer TEXT NOT NULL, key TEXT NOT NULL, node INTEGER NOT NULL, proof INTEGER NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), value INTEGER NOT NULL, PRIMARY KEY(scope,transfer,key,node,proof,ordinal), FOREIGN KEY(scope,transfer,key,node,proof) REFERENCES transfer_task_proof(scope,transfer,key,node,ordinal) ON DELETE CASCADE) STRICT, WITHOUT ROWID;
CREATE TABLE transfer_native(scope TEXT NOT NULL, transfer TEXT NOT NULL, key TEXT NOT NULL, node INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), intent INTEGER NOT NULL CHECK(intent IN (0,1)), combine INTEGER NOT NULL CHECK(combine IN (0,1)), receipt TEXT, request_identity TEXT NOT NULL, request_names_sha256 TEXT NOT NULL, request_count INTEGER NOT NULL, done BLOB NOT NULL, done_present INTEGER NOT NULL CHECK(done_present IN (0,1)), pending INTEGER NOT NULL CHECK(pending IN (0,1)), combines_present INTEGER NOT NULL CHECK(combines_present IN (0,1)), PRIMARY KEY(scope,transfer,key,node), FOREIGN KEY(scope,transfer,key,node) REFERENCES transfer_task(scope,transfer,key,node) ON DELETE CASCADE, FOREIGN KEY(scope,receipt) REFERENCES native_receipt(scope,identity)) STRICT, WITHOUT ROWID;
CREATE TABLE transfer_attempt(scope TEXT NOT NULL, transfer TEXT NOT NULL, key TEXT NOT NULL, node INTEGER NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), selection BLOB NOT NULL, selection_present INTEGER NOT NULL CHECK(selection_present IN (0,1)), receipt TEXT, PRIMARY KEY(scope,transfer,key,node,ordinal), FOREIGN KEY(scope,transfer,key,node) REFERENCES transfer_native(scope,transfer,key,node) ON DELETE CASCADE, FOREIGN KEY(scope,receipt) REFERENCES native_receipt(scope,identity)) STRICT, WITHOUT ROWID;
`

var stateTransferDigestQueries = map[string]string{
	"child_transfer":        "SELECT * FROM child_transfer ORDER BY scope,id",
	"transfer_task":         "SELECT * FROM transfer_task ORDER BY scope,transfer,key,node",
	"transfer_task_name":    "SELECT * FROM transfer_task_name ORDER BY scope,transfer,key,node,ordinal",
	"transfer_task_ordinal": "SELECT * FROM transfer_task_ordinal ORDER BY scope,transfer,key,node,ordinal",
	"transfer_task_proof":   "SELECT * FROM transfer_task_proof ORDER BY scope,transfer,key,node,ordinal",
	"transfer_task_count":   "SELECT * FROM transfer_task_count ORDER BY scope,transfer,key,node,proof,ordinal",
	"transfer_native":       "SELECT * FROM transfer_native ORDER BY scope,transfer,key,node",
	"transfer_attempt":      "SELECT * FROM transfer_attempt ORDER BY scope,transfer,key,node,ordinal",
}

func (t *stateTx) putTransferIdentity(scope, id, operation, plan string) error {
	if id == "" || len(operation) != 64 || plan == "" {
		return errors.New("incomplete child transfer identity")
	}
	result, err := t.tx.Exec(`INSERT INTO child_transfer VALUES(?,?,1,?,?) ON CONFLICT(scope,id) DO UPDATE SET operation=excluded.operation WHERE child_transfer.operation=excluded.operation AND child_transfer.plan=excluded.plan`, scope, id, operation, plan)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("child transfer identity changed")
	}
	return err
}

func stateTaskDigest(v task) ([32]byte, error) {
	// idleAttempts is deliberately not journaled by the legacy engine either.
	b, err := json.Marshal(v)
	return sha256.Sum256(b), err
}

func (t *stateTx) putTransferTask(scope, transfer string, v task) error {
	if v.End <= v.Start || v.Start < 0 || v.Stream == "" || v.NativeParent != "" {
		return errors.New("invalid root transfer task")
	}
	key := taskKey(v)
	old, err := readStateTransferTask(t.tx, scope, transfer, key)
	if err == nil {
		if old.Source != v.Source || old.Target != v.Target {
			return errors.New("transfer task endpoints changed")
		}
		oldDigest, oldErr := stateTaskDigest(old)
		newDigest, newErr := stateTaskDigest(v)
		if oldErr != nil || newErr != nil {
			return errors.Join(oldErr, newErr)
		}
		if old.Phase == "done" && oldDigest != newDigest {
			return errors.New("completed transfer task changed")
		}
		if err := checkStateNativeTransition(old.Native, v.Native, v.Phase); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err = t.tx.Exec("DELETE FROM transfer_task WHERE scope=? AND transfer=? AND key=?", scope, transfer, key); err != nil {
		return err
	}
	return t.putTransferTaskNode(scope, transfer, key, 0, v)
}

func (t *stateTx) putTransferTaskNode(scope, transfer, key string, node int, v task) error {
	digest, err := stateTaskDigest(v)
	if err != nil {
		return err
	}
	flags := 0
	if v.Names != nil {
		flags |= 1
	}
	if v.NativeOrdinals != nil {
		flags |= 2
	}
	if v.Proofs != nil {
		flags |= 4
	}
	if _, err = t.tx.Exec("INSERT INTO transfer_task VALUES(?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?)", scope, transfer, key, node, v.NativeParent, v.Kind, v.Checkpoint, v.Retries, v.Stream, v.Start, v.End, v.Source, v.Target, v.Phase, v.Bytes, flags, digest[:]); err != nil {
		return err
	}
	for ordinal, name := range v.Names {
		if _, err = t.tx.Exec("INSERT INTO transfer_task_name VALUES(?,?,?,?,?,1,?)", scope, transfer, key, node, ordinal, name); err != nil {
			return err
		}
	}
	for ordinal, value := range v.NativeOrdinals {
		if _, err = t.tx.Exec("INSERT INTO transfer_task_ordinal VALUES(?,?,?,?,?,1,?)", scope, transfer, key, node, ordinal, value); err != nil {
			return err
		}
	}
	for ordinal, p := range v.Proofs {
		a, b := p.Source, p.Target
		if _, err = t.tx.Exec("INSERT INTO transfer_task_proof VALUES(?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?)", scope, transfer, key, node, ordinal, p.KeyCount, p.Sampled, a.Name, stateUint64(a.ID), stateUint64(a.Generation), stateUint64(a.Revision), b.Name, stateUint64(b.ID), stateUint64(b.Generation), stateUint64(b.Revision), p.Digest, p.Merged, p.Counts != nil); err != nil {
			return err
		}
		for n, count := range p.Counts {
			if _, err = t.tx.Exec("INSERT INTO transfer_task_count VALUES(?,?,?,?,?,?,1,?)", scope, transfer, key, node, ordinal, n, count); err != nil {
				return err
			}
		}
	}
	if v.Native == nil {
		return nil
	}
	n := v.Native
	var receipt any
	if n.Intent {
		want := chunkstore.NativeReceipt{Identity: n.Receipt.Identity, NamesSHA256: n.Receipt.NamesSHA256, Count: n.Receipt.Count}
		if !reflect.DeepEqual(n.Receipt, want) {
			return errors.New("native intent contains unacknowledged receipt fields")
		}
	} else if !reflect.DeepEqual(n.Receipt, chunkstore.NativeReceipt{}) {
		if err = t.putNativeReceipt(scope, n.Receipt); err != nil {
			return err
		}
		receipt = n.Receipt.Identity
	}
	done := n.Done
	if done == nil {
		done = []byte{}
	}
	if _, err = t.tx.Exec("INSERT INTO transfer_native VALUES(?,?,?,?,1,?,?,?,?,?,?,?,?,?,?)", scope, transfer, key, node, n.Intent, n.Combine, receipt, n.Receipt.Identity, n.Receipt.NamesSHA256, n.Receipt.Count, done, n.Done != nil, n.Pending != nil, n.Combines != nil); err != nil {
		return err
	}
	for ordinal, a := range n.Combines {
		var attemptReceipt any
		if a.Receipt != nil {
			if err = t.putNativeReceipt(scope, *a.Receipt); err != nil {
				return err
			}
			attemptReceipt = a.Receipt.Identity
		}
		selection := a.Selection
		if selection == nil {
			selection = []byte{}
		}
		if _, err = t.tx.Exec("INSERT INTO transfer_attempt VALUES(?,?,?,?,?,1,?,?,?)", scope, transfer, key, node, ordinal, selection, a.Selection != nil, attemptReceipt); err != nil {
			return err
		}
	}
	if n.Pending != nil {
		// The existing fallback engine has one in-flight metric child; it cannot
		// dispatch another native fallback recursively.
		if node != 0 || n.Pending.Native != nil {
			return errors.New("nested native fallback is not a legacy task")
		}
		return t.putTransferTaskNode(scope, transfer, key, 1, *n.Pending)
	}
	return nil
}

// Read each normalized child in bounded queries. No replay of historical task
// versions is needed, and proof/counter integers retain the full uint64 domain.
func readStateTransferTask(tx *sql.Tx, scope, transfer, key string) (task, error) {
	return readStateTransferTaskNode(tx, scope, transfer, key, 0)
}

func readStateTransferTaskNode(tx *sql.Tx, scope, transfer, key string, node int) (v task, err error) {
	var flags int
	var want []byte
	err = tx.QueryRow("SELECT native_parent,kind,checkpoint,retries,stream,start,end,source,target,phase,bytes,slices,sha256 FROM transfer_task WHERE scope=? AND transfer=? AND key=? AND node=?", scope, transfer, key, node).Scan(&v.NativeParent, &v.Kind, &v.Checkpoint, &v.Retries, &v.Stream, &v.Start, &v.End, &v.Source, &v.Target, &v.Phase, &v.Bytes, &flags, &want)
	if err != nil {
		return v, err
	}
	if flags&1 != 0 {
		v.Names = []string{}
	}
	if flags&2 != 0 {
		v.NativeOrdinals = []int{}
	}
	if flags&4 != 0 {
		v.Proofs = []chunkstore.TransferProof{}
	}
	args := []any{scope, transfer, key, node}
	if err = stateTaskRows(tx, "SELECT name FROM transfer_task_name WHERE scope=? AND transfer=? AND key=? AND node=? ORDER BY ordinal", args, func(rows *sql.Rows) error {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		v.Names = append(v.Names, name)
		return nil
	}); err != nil {
		return v, err
	}
	if err = stateTaskRows(tx, "SELECT value FROM transfer_task_ordinal WHERE scope=? AND transfer=? AND key=? AND node=? ORDER BY ordinal", args, func(rows *sql.Rows) error {
		var value int
		if err := rows.Scan(&value); err != nil {
			return err
		}
		v.NativeOrdinals = append(v.NativeOrdinals, value)
		return nil
	}); err != nil {
		return v, err
	}
	var countsPresent []bool
	if err = stateTaskRows(tx, "SELECT key_count,sampled,source_name,source_id,source_generation,source_revision,target_name,target_id,target_generation,target_revision,digest,merged,counts_present FROM transfer_task_proof WHERE scope=? AND transfer=? AND key=? AND node=? ORDER BY ordinal", args, func(rows *sql.Rows) error {
		var p chunkstore.TransferProof
		var ids [6][]byte
		var present bool
		if err := rows.Scan(&p.KeyCount, &p.Sampled, &p.Source.Name, &ids[0], &ids[1], &ids[2], &p.Target.Name, &ids[3], &ids[4], &ids[5], &p.Digest, &p.Merged, &present); err != nil {
			return err
		}
		for i, dst := range []*uint64{&p.Source.ID, &p.Source.Generation, &p.Source.Revision, &p.Target.ID, &p.Target.Generation, &p.Target.Revision} {
			if len(ids[i]) != 8 {
				return errors.New("invalid transfer proof ID")
			}
			*dst = binary.BigEndian.Uint64(ids[i])
		}
		if present {
			p.Counts = []int{}
		}
		v.Proofs = append(v.Proofs, p)
		countsPresent = append(countsPresent, present)
		return nil
	}); err != nil {
		return v, err
	}
	if err = stateTaskRows(tx, "SELECT proof,value FROM transfer_task_count WHERE scope=? AND transfer=? AND key=? AND node=? ORDER BY proof,ordinal", args, func(rows *sql.Rows) error {
		var proof, value int
		if err := rows.Scan(&proof, &value); err != nil {
			return err
		}
		if proof < 0 || proof >= len(v.Proofs) || !countsPresent[proof] {
			return errors.New("invalid transfer proof count")
		}
		v.Proofs[proof].Counts = append(v.Proofs[proof].Counts, value)
		return nil
	}); err != nil {
		return v, err
	}
	var n nativeRangeState
	var receipt sql.NullString
	var done, pending, combines bool
	err = tx.QueryRow("SELECT intent,combine,receipt,request_identity,request_names_sha256,request_count,done,done_present,pending,combines_present FROM transfer_native WHERE scope=? AND transfer=? AND key=? AND node=?", args...).Scan(&n.Intent, &n.Combine, &receipt, &n.Receipt.Identity, &n.Receipt.NamesSHA256, &n.Receipt.Count, &n.Done, &done, &pending, &combines)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return v, err
	}
	if err == nil {
		if !done {
			n.Done = nil
		} else if n.Done == nil {
			n.Done = []byte{}
		}
		if receipt.Valid {
			n.Receipt, err = readStateNativeReceipt(tx, scope, receipt.String)
			if err != nil {
				return v, err
			}
		}
		if combines {
			n.Combines = []nativeDeferredAttempt{}
		}
		var receiptIDs []sql.NullString
		if err = stateTaskRows(tx, "SELECT selection,selection_present,receipt FROM transfer_attempt WHERE scope=? AND transfer=? AND key=? AND node=? ORDER BY ordinal", args, func(rows *sql.Rows) error {
			var a nativeDeferredAttempt
			var present bool
			var id sql.NullString
			if err := rows.Scan(&a.Selection, &present, &id); err != nil {
				return err
			}
			if !present {
				a.Selection = nil
			} else if a.Selection == nil {
				a.Selection = []byte{}
			}
			n.Combines = append(n.Combines, a)
			receiptIDs = append(receiptIDs, id)
			return nil
		}); err != nil {
			return v, err
		}
		for i, id := range receiptIDs {
			if id.Valid {
				r, e := readStateNativeReceipt(tx, scope, id.String)
				if e != nil {
					return v, e
				}
				n.Combines[i].Receipt = &r
			}
		}
		if pending {
			if node != 0 {
				return v, errors.New("nested native fallback")
			}
			p, e := readStateTransferTaskNode(tx, scope, transfer, key, 1)
			if e != nil {
				return v, e
			}
			n.Pending = &p
		}
		v.Native = &n
	}
	actual, err := stateTaskDigest(v)
	if err != nil {
		return v, err
	}
	if !bytes.Equal(actual[:], want) {
		return v, fmt.Errorf("transfer task projection checksum mismatch: %s", key)
	}
	return v, nil
}

func stateTaskRows(tx *sql.Tx, query string, args []any, read func(*sql.Rows) error) (err error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		if err = read(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// A later checkpoint may add acknowledged ordinals and deferred attempts. It
// cannot forget a receipt or replace an already issued selection on recovery.
func checkStateNativeTransition(old, next *nativeRangeState, phase string) error {
	if old == nil {
		return nil
	}
	if next == nil {
		if phase != "done" {
			return errors.New("native task lost its pending state")
		}
		return nil
	}
	if old.Receipt.Identity != next.Receipt.Identity || old.Receipt.NamesSHA256 != next.Receipt.NamesSHA256 || old.Receipt.Count != next.Receipt.Count {
		return errors.New("native request identity changed")
	}
	if old.Combine != next.Combine || !old.Intent && next.Intent {
		return errors.New("native acknowledgement regressed")
	}
	if !old.Intent && !reflect.DeepEqual(old.Receipt, next.Receipt) {
		return errors.New("native task receipt changed")
	}
	if len(next.Done) < len(old.Done) {
		return errors.New("native completed bitmap shrank")
	}
	for i, b := range old.Done {
		if b&^next.Done[i] != 0 {
			return errors.New("native completed ordinal became pending")
		}
	}
	if len(next.Combines) < len(old.Combines) {
		return errors.New("native deferred history shrank")
	}
	for i, a := range old.Combines {
		b := next.Combines[i]
		if !bytes.Equal(a.Selection, b.Selection) || a.Receipt != nil && !reflect.DeepEqual(a.Receipt, b.Receipt) {
			return errors.New("native deferred selection or acknowledgement changed")
		}
	}
	return nil
}
