package downsize

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
)

// A receipt is an immutable outcome of one fill attempt. The indexed lookup
// preserves legacy's first eligible outcome, including its completed-phase
// treatment of forbidden attempts. Name lists retain their order and nilness.
const stateFillDDL = `
CREATE TABLE fill_receipt(scope TEXT NOT NULL REFERENCES scope(id), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), round INTEGER NOT NULL CHECK(round>=0), holder TEXT NOT NULL, identity TEXT NOT NULL CHECK(identity<>''), first_name TEXT NOT NULL, last_name TEXT NOT NULL, names INTEGER NOT NULL CHECK(names>=0), copied INTEGER NOT NULL CHECK(copied>=0), forbidden INTEGER NOT NULL CHECK(forbidden IN (0,1)), complete INTEGER NOT NULL CHECK(complete IN (0,1)), reason TEXT NOT NULL, saved_at TEXT NOT NULL, lists INTEGER NOT NULL CHECK(lists BETWEEN 0 AND 15), sha256 BLOB NOT NULL CHECK(length(sha256)=32), PRIMARY KEY(scope,ordinal)) STRICT, WITHOUT ROWID;
CREATE INDEX fill_receipt_identity ON fill_receipt(scope,round,holder,identity,ordinal);
CREATE TABLE fill_receipt_name(scope TEXT NOT NULL, receipt INTEGER NOT NULL, kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 3), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(scope,receipt,kind,ordinal), FOREIGN KEY(scope,receipt) REFERENCES fill_receipt(scope,ordinal)) STRICT, WITHOUT ROWID;
`

var stateFillDigestQueries = map[string]string{
	"fill_receipt":      "SELECT * FROM fill_receipt ORDER BY scope,ordinal",
	"fill_receipt_name": "SELECT * FROM fill_receipt_name ORDER BY scope,receipt,kind,ordinal",
}

func stateFillDigest(r syncRow) ([32]byte, error) {
	b, err := json.Marshal(struct {
		Row     syncRow
		Present [4]bool
	}{r, [4]bool{r.Failed != nil, r.Short != nil, r.Selected != nil, r.Resolved != nil}})
	return sha256.Sum256(b), err
}

func (t *stateTx) appendFillReceipt(scope string, r syncRow) error {
	if r.Kind != "copied" || r.Phase != "fill" || r.Identity == "" {
		return errors.New("invalid fill receipt identity")
	}
	// Refuse accidental use as a generic journal-row sink. Every accepted field
	// must have a lossless typed projection below.
	projected := syncRow{Kind: "copied", Phase: "fill", Identity: r.Identity, Round: r.Round, Source: r.Source, First: r.First, Last: r.Last, Names: r.Names, Copied: r.Copied, Forbidden: r.Forbidden, SelectionComplete: r.SelectionComplete, InvalidationReason: r.InvalidationReason, Time: r.Time, Failed: r.Failed, Short: r.Short, Selected: r.Selected, Resolved: r.Resolved}
	sum, err := stateFillDigest(r)
	if err != nil {
		return err
	}
	check, err := stateFillDigest(projected)
	if err != nil || check != sum || !reflect.DeepEqual(r, projected) {
		return errors.New("unmapped fill receipt fields")
	}
	flags := 0
	for i, present := range []bool{r.Failed != nil, r.Short != nil, r.Selected != nil, r.Resolved != nil} {
		if present {
			flags |= 1 << i
		}
	}
	var ordinal int64
	if err := t.tx.QueryRow("SELECT coalesce(max(ordinal)+1,0) FROM fill_receipt WHERE scope=?", scope).Scan(&ordinal); err != nil {
		return err
	}
	if _, err := t.tx.Exec("INSERT INTO fill_receipt VALUES(?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?)", scope, ordinal, r.Round, r.Source, r.Identity, r.First, r.Last, r.Names, r.Copied, r.Forbidden, r.SelectionComplete, r.InvalidationReason, r.Time, flags, sum[:]); err != nil {
		return err
	}
	for kind, names := range [][]string{r.Failed, r.Short, r.Selected} {
		for order, name := range names {
			if _, err := t.tx.Exec("INSERT INTO fill_receipt_name VALUES(?,?,?,?,1,?,'')", scope, ordinal, kind, order, name); err != nil {
				return err
			}
		}
	}
	for order, item := range r.Resolved {
		if _, err := t.tx.Exec("INSERT INTO fill_receipt_name VALUES(?,?,3,?,1,?,?)", scope, ordinal, order, item.Name, item.Reason); err != nil {
			return err
		}
	}
	return nil
}

func readStateFillReceipt(tx *sql.Tx, scope string, ordinal int64) (r syncRow, err error) {
	r.Kind, r.Phase = "copied", "fill"
	var flags int
	var want []byte
	err = tx.QueryRow("SELECT round,holder,identity,first_name,last_name,names,copied,forbidden,complete,reason,saved_at,lists,sha256 FROM fill_receipt WHERE scope=? AND ordinal=?", scope, ordinal).Scan(&r.Round, &r.Source, &r.Identity, &r.First, &r.Last, &r.Names, &r.Copied, &r.Forbidden, &r.SelectionComplete, &r.InvalidationReason, &r.Time, &flags, &want)
	if err != nil {
		return r, err
	}
	lists := []*[]string{&r.Failed, &r.Short, &r.Selected}
	for i, list := range lists {
		if flags&(1<<i) != 0 {
			*list = []string{}
		}
	}
	if flags&8 != 0 {
		r.Resolved = []syncSkip{}
	}
	rows, err := tx.Query("SELECT kind,ordinal,name,reason FROM fill_receipt_name WHERE scope=? AND receipt=? ORDER BY kind,ordinal", scope, ordinal)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var kind, order int
		var name, reason string
		if err = rows.Scan(&kind, &order, &name, &reason); err != nil {
			break
		}
		if kind < 0 || kind > 3 || flags&(1<<kind) == 0 || kind < 3 && (order != len(*lists[kind]) || reason != "") || kind == 3 && order != len(r.Resolved) {
			err = errors.New("invalid fill receipt membership")
			break
		}
		if kind == 3 {
			r.Resolved = append(r.Resolved, syncSkip{Name: name, Reason: reason})
		} else {
			*lists[kind] = append(*lists[kind], name)
		}
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err == nil {
		var sum [32]byte
		sum, err = stateFillDigest(r)
		if err == nil && !bytes.Equal(want, sum[:]) {
			err = errors.New("fill receipt projection checksum mismatch")
		}
	}
	return r, err
}
