package downsize

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
)

const stateRecoveryDDL = `
CREATE TABLE window_points(scope TEXT NOT NULL REFERENCES scope(id), operation TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), source TEXT NOT NULL, window TEXT NOT NULL, start INTEGER NOT NULL, until INTEGER NOT NULL, before_count INTEGER, after_count INTEGER, superseded INTEGER NOT NULL CHECK(superseded IN (0,1)), PRIMARY KEY(scope,operation)) STRICT, WITHOUT ROWID;
CREATE TABLE recovery(parent_scope TEXT NOT NULL REFERENCES scope(id), child_scope TEXT PRIMARY KEY REFERENCES scope(id), version INTEGER NOT NULL CHECK(version=1), identity TEXT NOT NULL, data_scope TEXT NOT NULL, state TEXT NOT NULL, restart_window INTEGER NOT NULL CHECK(restart_window IN (0,1)), full_history INTEGER NOT NULL CHECK(full_history IN (0,1)), windows_present INTEGER NOT NULL CHECK(windows_present IN (0,1)), restarts_present INTEGER NOT NULL CHECK(restarts_present IN (0,1)), processes_present INTEGER NOT NULL CHECK(processes_present IN (0,1)), UNIQUE(parent_scope,state)) STRICT;
CREATE TABLE recovery_window(child_scope TEXT NOT NULL REFERENCES recovery(child_scope), kind TEXT NOT NULL CHECK(kind IN ('window','restart')), ordinal INTEGER NOT NULL CHECK(ordinal>=0), version INTEGER NOT NULL CHECK(version=1), name TEXT NOT NULL, start INTEGER NOT NULL, until INTEGER NOT NULL, PRIMARY KEY(child_scope,kind,ordinal)) STRICT, WITHOUT ROWID;
CREATE TABLE recovery_process(child_scope TEXT NOT NULL REFERENCES recovery(child_scope), holder TEXT NOT NULL, version INTEGER NOT NULL CHECK(version=1), start BLOB NOT NULL CHECK(length(start)=8), observed INTEGER NOT NULL, PRIMARY KEY(child_scope,holder)) STRICT, WITHOUT ROWID;
`

// Preserve the first pre-import baseline and the uncertainty left by a crash.
// A different source/window is a different operation, never an overwrite.
func (t *stateTx) putWindowPoints(scope, operation string, c windowPointCount) error {
	if operation == "" || c.Source == "" || c.Window.From < 0 || c.Window.Until <= c.Window.From {
		return errors.New("invalid window point measurement")
	}
	r, err := t.tx.Exec(`INSERT INTO window_points VALUES(?,?,1,?,?,?,?,?,?,?) ON CONFLICT(scope,operation) DO UPDATE SET before_count=excluded.before_count,after_count=excluded.after_count,superseded=excluded.superseded
WHERE window_points.source=excluded.source AND window_points.window=excluded.window AND window_points.start=excluded.start AND window_points.until=excluded.until
AND (window_points.before_count IS excluded.before_count OR window_points.after_count IS NULL AND excluded.before_count IS NULL)
AND (window_points.after_count IS NULL OR window_points.after_count IS excluded.after_count)
AND (window_points.superseded=0 OR excluded.superseded=1)`, scope, operation, c.Source, c.Window.Name, c.Window.From, c.Window.Until, c.Before, c.After, c.Superseded)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n != 1 {
		return errors.New("window point identity/baseline changed or completion regressed")
	}
	return err
}

func readStateWindowPoints(tx *sql.Tx, scope, operation string) (c windowPointCount, err error) {
	err = tx.QueryRow("SELECT source,window,start,until,before_count,after_count,superseded FROM window_points WHERE scope=? AND operation=?", scope, operation).Scan(&c.Source, &c.Window.Name, &c.Window.From, &c.Window.Until, &c.Before, &c.After, &c.Superseded)
	return c, err
}

func (t *stateTx) resumeWindowPoints(scope, operation string) (c windowPointCount, err error) {
	c, err = readStateWindowPoints(t.tx, scope, operation)
	if err == nil && c.After == nil {
		c.Before = nil
		err = t.putWindowPoints(scope, operation, c)
	}
	return c, err
}

func readStateWindowRepair(tx *sql.Tx, scope string) (out windowRepairCount, err error) {
	out.Unit = "finite serving-archive points added in selected windows; includes missing-name copies; not raw input events"
	var added int64
	rows, err := tx.Query("SELECT before_count,after_count FROM window_points WHERE scope=? AND superseded=0 ORDER BY operation", scope)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var before, after *int64
		if err = rows.Scan(&before, &after); err != nil {
			return out, err
		}
		if before == nil || after == nil || *after < *before {
			out.Unknown++
		} else {
			out.Transfers++
			added += *after - *before
		}
	}
	if out.Unknown == 0 && out.Transfers > 0 {
		out.Added = &added
	}
	return out, rows.Err()
}

// Recovery intent fixes the process/window boundary before any child work.
// The caller creates an explicit child scope; no terminal parent is reopened.
func (t *stateTx) putRecovery(parent, child string, r readyRecovery) error {
	if r.Identity != t.unit.Identity || r.Scope == "" || filepath.Base(r.State) != r.State || !strings.HasPrefix(r.State, "ready-recovery-") || len(r.Windows) == 0 && !r.FullHistory {
		return errors.New("invalid READY recovery identity/scope")
	}
	var actualParent string
	if err := t.tx.QueryRow("SELECT parent FROM scope WHERE id=?", child).Scan(&actualParent); err != nil {
		return err
	}
	if actualParent != parent {
		return errors.New("READY recovery child belongs to another parent")
	}
	old, err := readStateRecovery(t.tx, parent, child)
	if err == nil {
		if !reflect.DeepEqual(old, r) {
			return errors.New("immutable READY recovery intent changed")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err = t.tx.Exec("INSERT INTO recovery VALUES(?,?,1,?,?,?,?,?,?,?,?)", parent, child, r.Identity, r.Scope, r.State, r.RestartWindow, r.FullHistory, r.Windows != nil, r.Restarts != nil, r.Processes != nil); err != nil {
		return err
	}
	for _, kind := range []string{"window", "restart"} {
		ranges := r.Windows
		if kind == "restart" {
			ranges = r.Restarts
		}
		for n, w := range ranges {
			if w.From < 0 || w.Until <= w.From {
				return errors.New("invalid READY recovery window")
			}
			if _, err = t.tx.Exec("INSERT INTO recovery_window VALUES(?,?,?,1,?,?,?)", child, kind, n, w.Name, w.From, w.Until); err != nil {
				return err
			}
		}
	}
	for holder, p := range r.Processes {
		if p.Start <= 0 || math.IsNaN(p.Start) || math.IsInf(p.Start, 0) || p.Observed <= 0 {
			return errors.New("invalid READY recovery process")
		}
		if _, err = t.tx.Exec("INSERT INTO recovery_process VALUES(?,?,1,?,?)", child, holder, binary.BigEndian.AppendUint64(nil, math.Float64bits(p.Start)), p.Observed); err != nil {
			return err
		}
	}
	return nil
}

func readStateRecovery(tx *sql.Tx, parent, child string) (r readyRecovery, err error) {
	var windows, restarts, processes bool
	err = tx.QueryRow("SELECT identity,data_scope,state,restart_window,full_history,windows_present,restarts_present,processes_present FROM recovery WHERE parent_scope=? AND child_scope=?", parent, child).Scan(&r.Identity, &r.Scope, &r.State, &r.RestartWindow, &r.FullHistory, &windows, &restarts, &processes)
	if err != nil {
		return r, err
	}
	if windows {
		r.Windows = syncRanges{}
	}
	if restarts {
		r.Restarts = syncRanges{}
	}
	if processes {
		r.Processes = map[string]readyProcess{}
	}
	rows, err := tx.Query("SELECT kind,name,start,until FROM recovery_window WHERE child_scope=? ORDER BY kind,ordinal", child)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var kind string
		var w syncRange
		if err = rows.Scan(&kind, &w.Name, &w.From, &w.Until); err != nil {
			rows.Close()
			return r, err
		}
		if kind == "window" {
			r.Windows = append(r.Windows, w)
		} else {
			r.Restarts = append(r.Restarts, w)
		}
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return r, err
	}
	rows, err = tx.Query("SELECT holder,start,observed FROM recovery_process WHERE child_scope=? ORDER BY holder", child)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		var holder string
		var start []byte
		var p readyProcess
		if err = rows.Scan(&holder, &start, &p.Observed); err != nil {
			return r, err
		}
		if len(start) != 8 {
			return r, errors.New("invalid READY process bits")
		}
		p.Start = math.Float64frombits(binary.BigEndian.Uint64(start))
		r.Processes[holder] = p
	}
	return r, rows.Err()
}
