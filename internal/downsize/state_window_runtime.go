package downsize

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
)

// Keep the original ordered window receipt headers as typed references. The
// mutable scheduling floor/visit indexes cannot reproduce historical rows or
// their timestamps by themselves. Payload membership lives only in its typed
// immutable selection or resolution relation.
func stateWindowRuntimeDigest(r syncRow) ([32]byte, error) {
	b, err := json.Marshal(struct {
		Row     syncRow
		Present [3]bool
	}{r, [3]bool{r.Selected != nil, r.Skipped != nil, r.Resolved != nil}})
	return sha256.Sum256(b), err
}

func (t *stateTx) appendWindowRuntimeReceipt(scope string, r syncRow, selection, token any) error {
	sum, err := stateWindowRuntimeDigest(r)
	if err != nil {
		return err
	}
	var ordinal int64
	if err := t.tx.QueryRow("SELECT coalesce(max(ordinal)+1,0) FROM window_runtime_receipt WHERE scope=?", scope).Scan(&ordinal); err != nil {
		return err
	}
	group, holder, names := 0, "", 0
	if r.Kind == "window-retry-visited" {
		group, holder, names = r.Round, r.Source, r.Names
	}
	if r.Kind == "window-retry-floor" {
		names = r.Names
	}
	_, err = t.tx.Exec("INSERT INTO window_runtime_receipt VALUES(?,?,1,?,?,?,?,?,?,?,?)", scope, ordinal, r.Kind, selection, token, group, holder, names, r.Time, sum[:])
	return err
}

func readStateWindowRuntimeReceipt(tx *sql.Tx, scope string, ordinal int64) (r syncRow, err error) {
	var selection sql.NullInt64
	var token sql.NullString
	var want []byte
	err = tx.QueryRow("SELECT kind,selection,token,group_id,holder,names,saved_at,sha256 FROM window_runtime_receipt WHERE scope=? AND ordinal=?", scope, ordinal).Scan(&r.Kind, &selection, &token, &r.Round, &r.Source, &r.Names, &r.Time, &want)
	if err != nil {
		return r, err
	}
	switch r.Kind {
	case "window-selected":
		v, err := readStateWindowSelection(tx, scope, selection.Int64)
		if err != nil {
			return r, err
		}
		if v.SavedAt != r.Time {
			return r, errors.New("window receipt timestamp contradicts selection")
		}
		r.Round, r.Source, r.First, r.Last, r.Names, r.SelectionComplete, r.Selected, r.Skipped = v.Group, v.Holder, v.First, v.Last, v.Names, v.Complete, v.Selected, v.Skipped
	case "window-resolved":
		if err := checkStateWindowResolution(tx, scope, token.String); err != nil {
			return r, err
		}
		var names, proof bool
		err = tx.QueryRow("SELECT group_id,holder,copied,names_present,proof_present FROM window_resolution_batch WHERE scope=? AND token=?", scope, token.String).Scan(&r.Round, &r.Source, &r.Copied, &names, &proof)
		if err != nil {
			return r, err
		}
		if names {
			r.Selected = []string{}
		}
		if proof {
			r.Resolved = []syncSkip{}
		}
		if err = stateTaskRows(tx, "SELECT name FROM window_resolution_name WHERE scope=? AND token=? ORDER BY ordinal", []any{scope, token.String}, func(rows *sql.Rows) error {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			r.Selected = append(r.Selected, name)
			return nil
		}); err != nil {
			return r, err
		}
		err = stateTaskRows(tx, "SELECT name,reason FROM window_resolution_proof WHERE scope=? AND token=? ORDER BY ordinal", []any{scope, token.String}, func(rows *sql.Rows) error {
			var p syncSkip
			if err := rows.Scan(&p.Name, &p.Reason); err != nil {
				return err
			}
			r.Resolved = append(r.Resolved, p)
			return nil
		})
	case "window-retry-visited":
		var n int
		err = tx.QueryRow("SELECT count(*) FROM window_retry_visited WHERE scope=? AND group_id=? AND holder=?", scope, r.Round, r.Source).Scan(&n)
		if err == nil && n != 1 {
			err = errors.New("window visit receipt lacks scheduling marker")
		}
	case "window-retry-floor":
		var floor int64
		err = tx.QueryRow("SELECT floor FROM window_retry WHERE scope=?", scope).Scan(&floor)
		if err == nil && floor < max(int64(0), int64(r.Names)) {
			err = errors.New("window floor contradicts its receipt")
		}
	default:
		err = errors.New("unknown window runtime receipt")
	}
	if err == nil {
		var sum [32]byte
		sum, err = stateWindowRuntimeDigest(r)
		if err == nil && !bytes.Equal(want, sum[:]) {
			err = errors.New("window runtime receipt checksum mismatch")
		}
	}
	return r, err
}
