package downsize

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"reflect"
	"time"
)

// Retry persistence owns immutable selection intents and mutable scheduling
// hints separately from confirmed source coverage. Phase/checkpoint ownership
// still has to move before this seam can be selected by the command.
type syncWindowPersistence interface {
	load() (syncWindowSnapshot, error)
	append(syncRow) error
}

type syncWindowSnapshot struct {
	selections []syncRow
	receipts   []syncRow
	resolved   map[syncWindowSelectionKey]map[string]bool
	visited    map[syncWindowSelectionKey]bool
	floor      int64
	copied     int
}

func newSyncWindowSnapshot() syncWindowSnapshot {
	return syncWindowSnapshot{resolved: map[syncWindowSelectionKey]map[string]bool{}, visited: map[syncWindowSelectionKey]bool{}}
}

func (v *syncWindowSnapshot) resolve(group int, holder, name string) {
	k := syncWindowSelectionKey{group, holder}
	if v.resolved[k] == nil {
		v.resolved[k] = map[string]bool{}
	}
	v.resolved[k][name] = true
}

type legacySyncWindow struct{ j *syncJournal }

func (p legacySyncWindow) append(r syncRow) error { return p.j.append(r) }
func (p legacySyncWindow) load() (syncWindowSnapshot, error) {
	out := newSyncWindowSnapshot()
	for _, r := range p.j.find(func(r syncRow) bool {
		return r.Kind == "window-selected" || r.Kind == "window-resolved" || r.Kind == "window-retry-visited" || r.Kind == "window-retry-floor"
	}) {
		out.receipts = append(out.receipts, r)
		switch r.Kind {
		case "window-selected":
			out.selections = append(out.selections, r)
		case "window-resolved":
			out.copied += r.Copied
			for _, n := range r.Selected {
				out.resolve(r.Round, r.Source, n)
			}
		case "window-retry-visited":
			out.visited[syncWindowSelectionKey{r.Round, r.Source}] = true
		case "window-retry-floor":
			out.floor = max(out.floor, int64(r.Names))
		}
	}
	return out, nil
}
func (s *syncer) windowPersistence() syncWindowPersistence {
	if s.windowState != nil {
		return s.windowState
	}
	return legacySyncWindow{s.j}
}

type sqliteSyncWindow struct {
	store *stateStore
	scope string
}

func (p sqliteSyncWindow) append(r syncRow) error {
	r.Time = time.Now().UTC().Format(time.RFC3339Nano)
	return p.store.update(func(tx *stateTx) error {
		switch r.Kind {
		case "window-selected":
			v := stateSelection{Scope: p.scope, Group: r.Round, Holder: r.Source, First: r.First, Last: r.Last, Names: r.Names, Complete: r.SelectionComplete, SavedAt: r.Time, Selected: r.Selected, Skipped: r.Skipped}
			projected := syncRow{Kind: r.Kind, Round: r.Round, Source: r.Source, First: r.First, Last: r.Last, Names: r.Names, SelectionComplete: r.SelectionComplete, Time: r.Time, Selected: r.Selected, Skipped: r.Skipped}
			if !reflect.DeepEqual(r, projected) {
				return errors.New("unmapped window selection fields")
			}
			if err := tx.tx.QueryRow("SELECT coalesce(max(ordinal)+1,0) FROM window_selection WHERE scope=?", p.scope).Scan(&v.Ordinal); err != nil {
				return err
			}
			if err := tx.putWindowSelection(v); err != nil {
				return err
			}
			return tx.appendWindowRuntimeReceipt(p.scope, r, v.Ordinal, nil)
		case "window-resolved":
			projected := syncRow{Kind: r.Kind, Round: r.Round, Source: r.Source, Selected: r.Selected, Resolved: r.Resolved, Copied: r.Copied, Time: r.Time}
			if !reflect.DeepEqual(r, projected) {
				return errors.New("unmapped window resolution fields")
			}
			// A committed resolution retires its exact membership. A restarted worker
			// never issues it again; distinct historical attempts retain their counts.
			token := rand.Text()
			if err := tx.resolveWindowSelection(p.scope, token, r.Round, r.Source, r.Selected, r.Resolved, r.Copied); err != nil {
				return err
			}
			return tx.appendWindowRuntimeReceipt(p.scope, r, nil, token)
		case "window-retry-visited":
			projected := syncRow{Kind: r.Kind, Round: r.Round, Source: r.Source, Names: r.Names, Time: r.Time}
			if !reflect.DeepEqual(r, projected) {
				return errors.New("unmapped window visit fields")
			}
			if err := tx.visitWindowSelection(p.scope, r.Round, r.Source); err != nil {
				return err
			}
			return tx.appendWindowRuntimeReceipt(p.scope, r, nil, nil)
		case "window-retry-floor":
			projected := syncRow{Kind: r.Kind, Names: r.Names, Time: r.Time}
			if !reflect.DeepEqual(r, projected) {
				return errors.New("unmapped window floor fields")
			}
			if err := tx.putWindowRetryFloor(p.scope, int64(r.Names)); err != nil {
				return err
			}
			return tx.appendWindowRuntimeReceipt(p.scope, r, nil, nil)
		default:
			return errors.New("invalid window persistence kind")
		}
	})
}

func (p sqliteSyncWindow) load() (syncWindowSnapshot, error) {
	out := newSyncWindowSnapshot()
	tx, err := p.store.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	// The controller retains the legacy collection shape for now; relational
	// reads validate each immutable intent and resolution before scheduling.
	var ordinals []int64
	err = stateTaskRows(tx, "SELECT ordinal FROM window_selection WHERE scope=? ORDER BY ordinal", []any{p.scope}, func(rows *sql.Rows) error {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return err
		}
		ordinals = append(ordinals, n)
		return nil
	})
	if err != nil {
		return out, err
	}
	for _, ordinal := range ordinals {
		v, err := readStateWindowSelection(tx, p.scope, ordinal)
		if err != nil {
			return out, err
		}
		out.selections = append(out.selections, syncRow{Kind: "window-selected", Round: v.Group, Source: v.Holder, First: v.First, Last: v.Last, Names: v.Names, SelectionComplete: v.Complete, Time: v.SavedAt, Selected: v.Selected, Skipped: v.Skipped})
	}
	var tokens []string
	err = stateTaskRows(tx, "SELECT token FROM window_resolution_batch WHERE scope=?", []any{p.scope}, func(rows *sql.Rows) error {
		var token string
		if err := rows.Scan(&token); err != nil {
			return err
		}
		tokens = append(tokens, token)
		return nil
	})
	if err != nil {
		return out, err
	}
	for _, token := range tokens {
		if err := checkStateWindowResolution(tx, p.scope, token); err != nil {
			return out, err
		}
	}
	var extra int
	if err := tx.QueryRow(`SELECT count(*) FROM window_resolved r WHERE r.scope=? AND NOT EXISTS(SELECT 1 FROM window_resolution_name n JOIN window_resolution_batch b ON b.scope=n.scope AND b.token=n.token WHERE n.scope=r.scope AND b.group_id=r.group_id AND b.holder=r.holder AND n.name=r.name)`, p.scope).Scan(&extra); err != nil {
		return out, err
	}
	if extra != 0 {
		return out, errors.New("window resolution membership lacks its immutable receipt")
	}
	err = stateTaskRows(tx, "SELECT group_id,holder,name FROM window_resolved WHERE scope=?", []any{p.scope}, func(rows *sql.Rows) error {
		var group int
		var holder, name string
		if err := rows.Scan(&group, &holder, &name); err != nil {
			return err
		}
		out.resolve(group, holder, name)
		return nil
	})
	if err != nil {
		return out, err
	}
	err = stateTaskRows(tx, "SELECT group_id,holder FROM window_retry_visited WHERE scope=?", []any{p.scope}, func(rows *sql.Rows) error {
		var k syncWindowSelectionKey
		if err := rows.Scan(&k.group, &k.source); err != nil {
			return err
		}
		out.visited[k] = true
		return nil
	})
	if err != nil {
		return out, err
	}
	err = tx.QueryRow("SELECT coalesce((SELECT floor FROM window_retry WHERE scope=?),0),coalesce((SELECT sum(copied) FROM window_resolution_batch WHERE scope=?),0)", p.scope, p.scope).Scan(&out.floor, &out.copied)
	if err != nil {
		return out, err
	}
	var receipts []int64
	if err := stateTaskRows(tx, "SELECT ordinal FROM window_runtime_receipt WHERE scope=? ORDER BY ordinal", []any{p.scope}, func(rows *sql.Rows) error {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		receipts = append(receipts, id)
		return nil
	}); err != nil {
		return out, err
	}
	for _, id := range receipts {
		row, err := readStateWindowRuntimeReceipt(tx, p.scope, id)
		if err != nil {
			return out, err
		}
		out.receipts = append(out.receipts, row)
	}
	return out, tx.Commit()
}
