package downsize

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

const stateNativeReceiptDDL = `
CREATE TABLE native_receipt(scope TEXT NOT NULL REFERENCES scope(id), identity TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), names_sha256 TEXT NOT NULL, count INTEGER NOT NULL, copied_count INTEGER NOT NULL, deferred_count INTEGER NOT NULL, copied BLOB NOT NULL, digest TEXT NOT NULL, bytes INTEGER NOT NULL, first_id BLOB NOT NULL CHECK(length(first_id)=8), reserved_ids INTEGER NOT NULL, slices INTEGER NOT NULL, sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,identity)) STRICT, WITHOUT ROWID;
CREATE TABLE native_receipt_table(scope TEXT NOT NULL, identity TEXT NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), namespace TEXT NOT NULL, lower_key BLOB, upper_key BLOB, bounds INTEGER NOT NULL CHECK(bounds BETWEEN 0 AND 3), bytes BLOB NOT NULL CHECK(length(bytes)=8), entries BLOB NOT NULL CHECK(length(entries)=8), PRIMARY KEY(scope,identity,ordinal), FOREIGN KEY(scope,identity) REFERENCES native_receipt(scope,identity)) STRICT, WITHOUT ROWID;
CREATE TABLE native_receipt_mapping(scope TEXT NOT NULL, identity TEXT NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), source_id BLOB NOT NULL CHECK(length(source_id)=8), source_generation BLOB NOT NULL CHECK(length(source_generation)=8), source_revision BLOB NOT NULL CHECK(length(source_revision)=8), target_id BLOB NOT NULL CHECK(length(target_id)=8), target_generation BLOB NOT NULL CHECK(length(target_generation)=8), target_revision BLOB NOT NULL CHECK(length(target_revision)=8), PRIMARY KEY(scope,identity,ordinal), FOREIGN KEY(scope,identity) REFERENCES native_receipt(scope,identity)) STRICT, WITHOUT ROWID;
CREATE TABLE native_receipt_skip(scope TEXT NOT NULL, identity TEXT NOT NULL, ordinal INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, bytes INTEGER NOT NULL, physical_points INTEGER NOT NULL, archive_slots INTEGER NOT NULL, memory_bytes INTEGER NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(scope,identity,ordinal), FOREIGN KEY(scope,identity) REFERENCES native_receipt(scope,identity)) STRICT, WITHOUT ROWID;
CREATE TABLE native_receipt_window_skip(scope TEXT NOT NULL, identity TEXT NOT NULL, entry INTEGER NOT NULL, version INTEGER NOT NULL CHECK(version=1), ordinal INTEGER NOT NULL, archive INTEGER NOT NULL, timestamp INTEGER NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(scope,identity,entry), FOREIGN KEY(scope,identity) REFERENCES native_receipt(scope,identity)) STRICT, WITHOUT ROWID;
`

// Typed immutable receipt projections include table bounds, sparse source/target
// ID mappings, size-policy skips and archive-boundary skips. JSON is hashed only
// as a canonical external receipt representation; it is never stored/replayed.
func (t *stateTx) putNativeReceipt(scope string, r chunkstore.NativeReceipt) error {
	if err := r.Validate(); err != nil {
		return err
	}
	old, err := readStateNativeReceipt(t.tx, scope, r.Identity)
	if err == nil {
		if !reflect.DeepEqual(old, r) {
			return errors.New("immutable native receipt changed")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	slices := 0
	if r.Copied != nil {
		slices |= 1
	}
	if r.Tables != nil {
		slices |= 2
	}
	if r.Mappings != nil {
		slices |= 4
	}
	if r.Skipped != nil {
		slices |= 8
	}
	if r.WindowSkipped != nil {
		slices |= 16
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	copied := r.Copied
	if copied == nil {
		copied = []byte{}
	}
	if _, err = t.tx.Exec("INSERT INTO native_receipt VALUES(?,?,1,?,?,?,?,?,?,?,?,?,?,?)", scope, r.Identity, r.NamesSHA256, r.Count, r.CopiedCount, r.DeferredCount, copied, r.Digest, r.Bytes, stateUint64(r.FirstID), r.ReservedIDs, slices, digest[:]); err != nil {
		return err
	}
	for n, v := range r.Tables {
		if _, err = t.tx.Exec("INSERT INTO native_receipt_table VALUES(?,?,?,1,?,?,?,?,?,?)", scope, r.Identity, n, v.Namespace, v.Lower, v.Upper, stateBounds(v.Lower, v.Upper), stateUint64(v.Bytes), stateUint64(v.Entries)); err != nil {
			return err
		}
	}
	for _, v := range r.Mappings {
		if _, err = t.tx.Exec("INSERT INTO native_receipt_mapping VALUES(?,?,?,1,?,?,?,?,?,?)", scope, r.Identity, v.Ordinal, stateUint64(v.SourceID), stateUint64(v.SourceGeneration), stateUint64(v.SourceRevision), stateUint64(v.TargetID), stateUint64(v.TargetGeneration), stateUint64(v.TargetRevision)); err != nil {
			return err
		}
	}
	for _, v := range r.Skipped {
		if _, err = t.tx.Exec("INSERT INTO native_receipt_skip VALUES(?,?,?,1,?,?,?,?,?,?)", scope, r.Identity, v.Ordinal, v.Name, v.Bytes, v.PhysicalPoints, v.ArchiveSlots, v.MemoryBytes, v.Reason); err != nil {
			return err
		}
	}
	for n, v := range r.WindowSkipped {
		if _, err = t.tx.Exec("INSERT INTO native_receipt_window_skip VALUES(?,?,?,1,?,?,?,?)", scope, r.Identity, n, v.Ordinal, v.Archive, v.Timestamp, v.Reason); err != nil {
			return err
		}
	}
	return nil
}

func stateBounds(lower, upper []byte) (flags int) {
	if lower != nil {
		flags |= 1
	}
	if upper != nil {
		flags |= 2
	}
	return flags
}

func stateUint64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func readStateNativeReceipt(tx *sql.Tx, scope, identity string) (r chunkstore.NativeReceipt, err error) {
	var first, digest []byte
	var slices int
	r.Identity = identity
	err = tx.QueryRow("SELECT names_sha256,count,copied_count,deferred_count,copied,digest,bytes,first_id,reserved_ids,slices,sha256 FROM native_receipt WHERE scope=? AND identity=?", scope, identity).Scan(&r.NamesSHA256, &r.Count, &r.CopiedCount, &r.DeferredCount, &r.Copied, &r.Digest, &r.Bytes, &first, &r.ReservedIDs, &slices, &digest)
	if err != nil {
		return r, err
	}
	if len(first) != 8 {
		return r, errors.New("invalid native first ID")
	}
	r.FirstID = binary.BigEndian.Uint64(first)
	if slices&1 == 0 {
		r.Copied = nil
	} else if r.Copied == nil {
		r.Copied = []byte{}
	}
	if slices&2 != 0 {
		r.Tables = []chunkstore.NativeSSTReceipt{}
	}
	if slices&4 != 0 {
		r.Mappings = []chunkstore.NativeMapping{}
	}
	if slices&8 != 0 {
		r.Skipped = []chunkstore.NativeSkip{}
	}
	if slices&16 != 0 {
		r.WindowSkipped = []chunkstore.NativeWindowSkip{}
	}
	rows, err := tx.Query("SELECT namespace,lower_key,upper_key,bounds,bytes,entries FROM native_receipt_table WHERE scope=? AND identity=? ORDER BY ordinal", scope, identity)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var v chunkstore.NativeSSTReceipt
		var size, entries []byte
		var bounds int
		if err = rows.Scan(&v.Namespace, &v.Lower, &v.Upper, &bounds, &size, &entries); err != nil {
			rows.Close()
			return r, err
		}
		if len(size) != 8 || len(entries) != 8 {
			rows.Close()
			return r, errors.New("invalid native table counters")
		}
		if bounds&1 != 0 && v.Lower == nil {
			v.Lower = []byte{}
		}
		if bounds&2 != 0 && v.Upper == nil {
			v.Upper = []byte{}
		}
		v.Bytes = binary.BigEndian.Uint64(size)
		v.Entries = binary.BigEndian.Uint64(entries)
		r.Tables = append(r.Tables, v)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return r, err
	}
	rows, err = tx.Query("SELECT ordinal,source_id,source_generation,source_revision,target_id,target_generation,target_revision FROM native_receipt_mapping WHERE scope=? AND identity=? ORDER BY ordinal", scope, identity)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var v chunkstore.NativeMapping
		var fields [6][]byte
		if err = rows.Scan(&v.Ordinal, &fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5]); err != nil {
			rows.Close()
			return r, err
		}
		targets := []*uint64{&v.SourceID, &v.SourceGeneration, &v.SourceRevision, &v.TargetID, &v.TargetGeneration, &v.TargetRevision}
		for i, p := range targets {
			if len(fields[i]) != 8 {
				rows.Close()
				return r, errors.New("invalid native mapping ID")
			}
			*p = binary.BigEndian.Uint64(fields[i])
		}
		r.Mappings = append(r.Mappings, v)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return r, err
	}
	rows, err = tx.Query("SELECT ordinal,name,bytes,physical_points,archive_slots,memory_bytes,reason FROM native_receipt_skip WHERE scope=? AND identity=? ORDER BY ordinal", scope, identity)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var v chunkstore.NativeSkip
		if err = rows.Scan(&v.Ordinal, &v.Name, &v.Bytes, &v.PhysicalPoints, &v.ArchiveSlots, &v.MemoryBytes, &v.Reason); err != nil {
			rows.Close()
			return r, err
		}
		r.Skipped = append(r.Skipped, v)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return r, err
	}
	rows, err = tx.Query("SELECT ordinal,archive,timestamp,reason FROM native_receipt_window_skip WHERE scope=? AND identity=? ORDER BY entry", scope, identity)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var v chunkstore.NativeWindowSkip
		if err = rows.Scan(&v.Ordinal, &v.Archive, &v.Timestamp, &v.Reason); err != nil {
			rows.Close()
			return r, err
		}
		r.WindowSkipped = append(r.WindowSkipped, v)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return r, err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	actual := sha256.Sum256(data)
	if !reflect.DeepEqual(actual[:], digest) {
		return r, errors.New("native receipt projection checksum mismatch")
	}
	return r, r.Validate()
}
